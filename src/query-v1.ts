import type { ConstraintValidationOptions } from "./constraints";
import {
  createQuerySession as createLegacyQuerySession,
  query as legacyQuery,
  type QueryExecutionPlan as LegacyQueryExecutionPlan,
  type QueryExecutionPlanScope,
  type QueryExecutionPlanStep as LegacyQueryExecutionPlanStep,
  type QuerySession as LegacyQuerySession,
  type QuerySessionInput as LegacyQuerySessionInput,
  type QueryStepEvent as LegacyQueryStepEvent,
  type QueryStepKind as LegacyQueryStepKind,
  type QueryStepRoute as LegacyQueryStepRoute,
  type QueryStepState as LegacyQueryStepState,
} from "./query";
import {
  normalizeCapability,
  resolveTableProvider,
  validateProviderBindings,
  type ProviderAdapter,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import { collectRelTables, countRelNodes, type RelNode } from "./rel";
import { executeRelWithProviders, UnsupportedRelExecutionError } from "./executor";
import { lowerSqlToRel, planPhysicalQuery } from "./planning";
import type {
  QueryRow,
  SchemaDefinition,
  TableLookupRequest,
  TableMethodsMap,
  TableScanRequest,
} from "./schema";

export interface QueryGuardrails {
  maxPlannerNodes: number;
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
  timeoutMs: number;
}

export const DEFAULT_QUERY_GUARDRAILS: QueryGuardrails = {
  maxPlannerNodes: 50_000,
  maxExecutionRows: 1_000_000,
  maxLookupKeysPerBatch: 1000,
  maxLookupBatches: 100,
  timeoutMs: 30_000,
};

export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  sql: string;
  queryGuardrails?: Partial<QueryGuardrails>;
  constraintValidation?: ConstraintValidationOptions;
}

export type QueryStepKind = LegacyQueryStepKind | "remote_fragment" | "lookup_join";
export type QueryStepRoute = "provider_fragment" | "lookup_join" | "local";

export interface QueryExecutionPlanStep
  extends Omit<LegacyQueryExecutionPlanStep, "kind"> {
  kind: QueryStepKind;
}

export interface QueryExecutionPlan {
  steps: QueryExecutionPlanStep[];
  scopes?: QueryExecutionPlanScope[];
}

export interface QueryStepState extends Omit<LegacyQueryStepState, "kind" | "routeUsed"> {
  kind: QueryStepKind;
  routeUsed?: QueryStepRoute;
}

export interface QueryStepEvent extends Omit<LegacyQueryStepEvent, "kind" | "routeUsed"> {
  kind: QueryStepKind;
  routeUsed?: QueryStepRoute;
}

export interface QuerySessionOptions {
  maxConcurrency?: number;
  eventOrder?: "plan";
  captureRows?: "full";
  onEvent?: (event: QueryStepEvent) => void;
}

export interface QuerySessionInput<TContext> extends QueryInput<TContext> {
  options?: QuerySessionOptions;
}

export interface QuerySession {
  getPlan(): QueryExecutionPlan;
  next(): Promise<QueryStepEvent | { done: true; result: QueryRow[] }>;
  runToCompletion(): Promise<QueryRow[]>;
  getResult(): QueryRow[] | null;
  getStepState(stepId: string): QueryStepState | undefined;
}

interface GuardrailRuntimeState {
  lookupBatches: number;
}

function resolveGuardrails(overrides?: Partial<QueryGuardrails>): QueryGuardrails {
  return {
    maxPlannerNodes: overrides?.maxPlannerNodes ?? DEFAULT_QUERY_GUARDRAILS.maxPlannerNodes,
    maxExecutionRows: overrides?.maxExecutionRows ?? DEFAULT_QUERY_GUARDRAILS.maxExecutionRows,
    maxLookupKeysPerBatch:
      overrides?.maxLookupKeysPerBatch ?? DEFAULT_QUERY_GUARDRAILS.maxLookupKeysPerBatch,
    maxLookupBatches: overrides?.maxLookupBatches ?? DEFAULT_QUERY_GUARDRAILS.maxLookupBatches,
    timeoutMs: overrides?.timeoutMs ?? DEFAULT_QUERY_GUARDRAILS.timeoutMs,
  };
}

function enforceExecutionRowLimit(rows: QueryRow[], guardrails: QueryGuardrails): void {
  if (rows.length > guardrails.maxExecutionRows) {
    throw new Error(
      `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
    );
  }
}

function isPromiseLike<T>(value: unknown): value is Promise<T> {
  return !!value && typeof value === "object" && "then" in value;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  if (timeoutMs <= 0 || !Number.isFinite(timeoutMs)) {
    return promise;
  }

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Query timed out after ${timeoutMs}ms.`));
    }, timeoutMs);
  });

  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function buildSqlQueryFragment(provider: string, sql: string, rel: RelNode): ProviderFragment {
  return {
    kind: "sql_query",
    provider,
    sql,
    rel,
  };
}

async function maybeExecuteWholeQueryFragment<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<QueryRow[] | null> {
  const tables = new Set(collectRelTables(rel));

  if (tables.size === 0) {
    return null;
  }

  const providers = new Set<string>();
  for (const table of tables) {
    providers.add(resolveTableProvider(input.schema, table));
  }

  if (providers.size !== 1) {
    return null;
  }

  const providerName = [...providers][0];
  if (!providerName) {
    return null;
  }

  const provider = input.providers[providerName];
  if (!provider) {
    throw new Error(`Missing provider adapter: ${providerName}`);
  }

  const fragment = buildSqlQueryFragment(providerName, input.sql, rel);
  const capability = normalizeCapability(await provider.canExecute(fragment, input.context));
  if (!capability.supported) {
    return null;
  }

  const compiled = await provider.compile(fragment, input.context);
  return provider.execute(compiled, input.context);
}

function buildLegacyMethodsMap<TContext>(
  input: QueryInput<TContext>,
  guardrails: QueryGuardrails,
  runtimeState: GuardrailRuntimeState,
): TableMethodsMap<TContext> {
  const methods: TableMethodsMap<TContext> = {};

  for (const tableName of Object.keys(input.schema.tables)) {
    const providerName = resolveTableProvider(input.schema, tableName);
    const provider = input.providers[providerName];
    if (!provider) {
      throw new Error(`Missing provider adapter ${providerName} for table ${tableName}.`);
    }

    methods[tableName] = {
      scan: async (request, context) => {
        const fragment: ProviderFragment = {
          kind: "scan",
          provider: providerName,
          table: request.table,
          request,
        };

        const capability = normalizeCapability(await provider.canExecute(fragment, context));
        if (!capability.supported) {
          throw new Error(
            `Provider ${providerName} cannot execute scan for table ${request.table}${capability.reason ? `: ${capability.reason}` : ""}.`,
          );
        }

        const compiled = await provider.compile(fragment, context);
        return provider.execute(compiled, context);
      },
      ...(provider.lookupMany
        ? {
            lookup: async (request: TableLookupRequest, context: TContext): Promise<QueryRow[]> => {
              const dedupedKeys = [...new Set(request.values)];
              const allRows: QueryRow[] = [];

              for (
                let startIndex = 0;
                startIndex < dedupedKeys.length;
                startIndex += guardrails.maxLookupKeysPerBatch
              ) {
                runtimeState.lookupBatches += 1;
                if (runtimeState.lookupBatches > guardrails.maxLookupBatches) {
                  throw new Error(
                    `Query exceeded maxLookupBatches guardrail (${guardrails.maxLookupBatches}).`,
                  );
                }

                const batch = dedupedKeys.slice(
                  startIndex,
                  startIndex + guardrails.maxLookupKeysPerBatch,
                );

                const batchRows = await provider.lookupMany!(
                  {
                    table: request.table,
                    key: request.key,
                    keys: batch,
                    select: request.select,
                    ...(request.where ? { where: request.where } : {}),
                  },
                  context,
                );
                allRows.push(...batchRows);
              }

              return allRows;
            },
          }
        : {}),
    };
  }

  return methods;
}

function mapStepKind(step: LegacyQueryExecutionPlanStep): QueryStepKind {
  if (step.kind !== "scan") {
    return step.kind;
  }

  const routeCandidates = (step.pushdown as { routeCandidates?: string[] } | undefined)
    ?.routeCandidates;
  if (Array.isArray(routeCandidates) && routeCandidates.includes("lookup")) {
    return "lookup_join";
  }

  return "remote_fragment";
}

function mapRoute(route: LegacyQueryStepRoute | undefined): QueryStepRoute | undefined {
  switch (route) {
    case "lookup":
      return "lookup_join";
    case "scan":
    case "aggregate":
      return "provider_fragment";
    case "local":
      return "local";
    default:
      return undefined;
  }
}

function mapPlan(plan: LegacyQueryExecutionPlan): QueryExecutionPlan {
  return {
    steps: plan.steps.map((step) => ({
      ...step,
      kind: mapStepKind(step),
    })),
    ...(plan.scopes ? { scopes: plan.scopes } : {}),
  };
}

function mapStepState(state: LegacyQueryStepState | undefined): QueryStepState | undefined {
  if (!state) {
    return undefined;
  }

  const { kind, routeUsed, ...rest } = state;
  const mappedRoute = mapRoute(routeUsed);
  return {
    ...rest,
    kind: kind === "scan" ? "remote_fragment" : (kind as QueryStepKind),
    ...(mappedRoute ? { routeUsed: mappedRoute } : {}),
  };
}

function mapStepEvent(event: LegacyQueryStepEvent): QueryStepEvent {
  const { kind, routeUsed, ...rest } = event;
  const mappedRoute = mapRoute(routeUsed);
  return {
    ...rest,
    kind: kind === "scan" ? "remote_fragment" : (kind as QueryStepKind),
    ...(mappedRoute ? { routeUsed: mappedRoute } : {}),
  };
}

function createProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  provider: ProviderAdapter<TContext>,
  providerName: string,
  fragment: ProviderFragment,
): QuerySession {
  let executed = false;
  let result: QueryRow[] | null = null;
  let eventDispatched = false;

  const stepId = "remote_fragment_1";
  const plan: QueryExecutionPlan = {
    steps: [
      {
        id: stepId,
        kind: "remote_fragment",
        dependsOn: [],
        summary: `Execute provider fragment (${providerName})`,
        phase: "fetch",
        operation: {
          name: "provider_fragment",
          details: {
            provider: providerName,
          },
        },
        request: {
          fragment: fragment.kind,
        },
      },
    ],
    scopes: [
      {
        id: "scope_root",
        kind: "root",
        label: "Root query",
      },
    ],
  };

  let state: QueryStepState = {
    id: stepId,
    kind: "remote_fragment",
    status: "ready",
    summary: `Execute provider fragment (${providerName})`,
    dependsOn: [],
  };

  const run = async (): Promise<QueryRow[]> => {
    if (executed) {
      return result ?? [];
    }

    executed = true;
    const startedAt = Date.now();
    state = {
      ...state,
      status: "running",
      startedAt,
    };

    const compiled = await provider.compile(fragment, input.context);
    const rows = await withTimeout(provider.execute(compiled, input.context), guardrails.timeoutMs);
    enforceExecutionRowLimit(rows, guardrails);
    result = rows;

    const endedAt = Date.now();
    state = {
      ...state,
      status: "done",
      routeUsed: "provider_fragment",
      rowCount: rows.length,
      outputRowCount: rows.length,
      startedAt,
      endedAt,
      durationMs: endedAt - startedAt,
      ...(input.options?.captureRows === "full" ? { rows } : {}),
    };

    return rows;
  };

  return {
    getPlan: () => plan,
    next: async () => {
      await run();
      if (!eventDispatched) {
        eventDispatched = true;
        const event: QueryStepEvent = {
          id: stepId,
          kind: "remote_fragment",
          status: "done",
          summary: state.summary,
          dependsOn: [],
          executionIndex: 1,
          startedAt: state.startedAt ?? Date.now(),
          endedAt: state.endedAt ?? Date.now(),
          durationMs: state.durationMs ?? 0,
          routeUsed: "provider_fragment",
          ...(state.rowCount != null ? { rowCount: state.rowCount } : {}),
          ...(state.outputRowCount != null ? { outputRowCount: state.outputRowCount } : {}),
          ...(input.options?.captureRows === "full" ? { rows: result ?? [] } : {}),
        };

        input.options?.onEvent?.(event);
        return event;
      }

      return {
        done: true as const,
        result: result ?? [],
      };
    },
    runToCompletion: async () => {
      const rows = await run();
      return rows;
    },
    getResult: () => result,
    getStepState: (id: string) => (id === stepId ? state : undefined),
  };
}

function createLegacyBackedSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
): QuerySession {
  const runtimeState: GuardrailRuntimeState = {
    lookupBatches: 0,
  };
  const methods = buildLegacyMethodsMap(input, guardrails, runtimeState);

  const legacyOptions = input.options
    ? {
        ...(input.options.maxConcurrency != null
          ? { maxConcurrency: input.options.maxConcurrency }
          : {}),
        ...(input.options.eventOrder ? { eventOrder: input.options.eventOrder } : {}),
        ...(input.options.captureRows ? { captureRows: input.options.captureRows } : {}),
      }
    : undefined;

  const legacyInput: LegacyQuerySessionInput<TContext> = {
    schema: input.schema,
    methods,
    context: input.context,
    sql: input.sql,
    ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
    ...(legacyOptions ? { options: legacyOptions } : {}),
  };

  const legacySession: LegacyQuerySession = createLegacyQuerySession(legacyInput);

  return {
    getPlan: () => mapPlan(legacySession.getPlan()),
    next: async () => {
      const next = await withTimeout(legacySession.next(), guardrails.timeoutMs);
      if ("done" in next) {
        enforceExecutionRowLimit(next.result, guardrails);
        return next;
      }

      const mapped = mapStepEvent(next);
      input.options?.onEvent?.(mapped);
      return mapped;
    },
    runToCompletion: async () => {
      const rows = await withTimeout(legacySession.runToCompletion(), guardrails.timeoutMs);
      enforceExecutionRowLimit(rows, guardrails);
      return rows;
    },
    getResult: () => legacySession.getResult(),
    getStepState: (stepId: string) => mapStepState(legacySession.getStepState(stepId)),
  };
}

function tryCreateSyncProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
): QuerySession | null {
  const tables = new Set(collectRelTables(rel));
  if (tables.size === 0) {
    return null;
  }

  const providers = new Set<string>();
  for (const table of tables) {
    providers.add(resolveTableProvider(input.schema, table));
  }

  if (providers.size !== 1) {
    return null;
  }

  const providerName = [...providers][0];
  if (!providerName) {
    return null;
  }

  const provider = input.providers[providerName];
  if (!provider) {
    return null;
  }

  const fragment = buildSqlQueryFragment(providerName, input.sql, rel);
  const capability = provider.canExecute(fragment, input.context);
  if (isPromiseLike(capability)) {
    return null;
  }

  const normalized = normalizeCapability(capability);
  if (!normalized.supported) {
    return null;
  }

  return createProviderFragmentSession(input, guardrails, provider, providerName, fragment);
}

export function createQuerySession<TContext>(input: QuerySessionInput<TContext>): QuerySession {
  validateProviderBindings(input.schema, input.providers);
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(input.sql, input.schema);

  const plannerNodeCount = countRelNodes(lowered.rel);
  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    throw new Error(
      `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
    );
  }

  return (
    tryCreateSyncProviderFragmentSession(input, guardrails, lowered.rel) ??
    createLegacyBackedSession(input, guardrails)
  );
}

export async function query<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  validateProviderBindings(input.schema, input.providers);

  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(input.sql, input.schema);
  const plannerNodeCount = countRelNodes(lowered.rel);

  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    throw new Error(
      `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
    );
  }

  const remoteRows = await withTimeout(
    maybeExecuteWholeQueryFragment(input, lowered.rel),
    guardrails.timeoutMs,
  );

  if (remoteRows) {
    enforceExecutionRowLimit(remoteRows, guardrails);
    return remoteRows;
  }

  try {
    const physicalPlan = await planPhysicalQuery(
      lowered.rel,
      input.schema,
      input.providers,
      input.context,
      input.sql,
    );

    const singleStep = physicalPlan.steps.length === 1 ? physicalPlan.steps[0] : null;
    const rows =
      singleStep?.kind === "remote_fragment"
        ? await withTimeout(
            executeProviderFragmentStep(singleStep, input.providers, input.context),
            guardrails.timeoutMs,
          )
        : await withTimeout(
            executeRelWithProviders(
              lowered.rel,
              input.schema,
              input.providers,
              input.context,
              {
                maxExecutionRows: guardrails.maxExecutionRows,
                maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
                maxLookupBatches: guardrails.maxLookupBatches,
              },
            ),
            guardrails.timeoutMs,
          );

    enforceExecutionRowLimit(rows, guardrails);
    return rows;
  } catch (error) {
    if (!(error instanceof UnsupportedRelExecutionError)) {
      throw error;
    }
  }

  const runtimeState: GuardrailRuntimeState = {
    lookupBatches: 0,
  };

  const methods = buildLegacyMethodsMap(input, guardrails, runtimeState);
  const result = await withTimeout(
    legacyQuery({
      schema: input.schema,
      methods,
      context: input.context,
      sql: input.sql,
      ...(input.constraintValidation ? { constraintValidation: input.constraintValidation } : {}),
    }),
    guardrails.timeoutMs,
  );

  enforceExecutionRowLimit(result, guardrails);
  return result;
}

export interface ExplainResult {
  rel: RelNode;
  plannerNodeCount: number;
  guardrails: QueryGuardrails;
}

export function explain<TContext>(input: QueryInput<TContext>): ExplainResult {
  validateProviderBindings(input.schema, input.providers);
  const guardrails = resolveGuardrails(input.queryGuardrails);
  const lowered = lowerSqlToRel(input.sql, input.schema);

  return {
    rel: lowered.rel,
    plannerNodeCount: countRelNodes(lowered.rel),
    guardrails,
  };
}

export function asProviderCompiledPlan(
  provider: string,
  kind: string,
  payload: unknown,
): ProviderCompiledPlan {
  return {
    provider,
    kind,
    payload,
  };
}

async function executeProviderFragmentStep<TContext>(
  step: { kind: "remote_fragment"; provider: string; fragment: ProviderFragment },
  providers: ProvidersMap<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const provider = providers[step.provider];
  if (!provider) {
    throw new Error(`Missing provider adapter: ${step.provider}`);
  }
  const compiled = await provider.compile(step.fragment, context);
  return provider.execute(compiled, context);
}
