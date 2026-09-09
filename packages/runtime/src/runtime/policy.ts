import { Result } from "better-result";
import { TuplGuardrailError } from "@tupl/foundation";

import {
  DEFAULT_QUERY_FALLBACK_POLICY,
  DEFAULT_QUERY_GUARDRAILS,
  type QueryFallbackPolicy,
  type QueryGuardrails,
} from "./contracts";

/**
 * Runtime policy helpers own guardrail normalization and cheap policy decisions.
 */
export function resolveGuardrails(overrides?: Partial<QueryGuardrails>): QueryGuardrails {
  return {
    maxPlannerNodes: overrides?.maxPlannerNodes ?? DEFAULT_QUERY_GUARDRAILS.maxPlannerNodes,
    maxExecutionRows: overrides?.maxExecutionRows ?? DEFAULT_QUERY_GUARDRAILS.maxExecutionRows,
    maxLookupKeysPerBatch:
      overrides?.maxLookupKeysPerBatch ?? DEFAULT_QUERY_GUARDRAILS.maxLookupKeysPerBatch,
    maxLookupBatches: overrides?.maxLookupBatches ?? DEFAULT_QUERY_GUARDRAILS.maxLookupBatches,
    timeoutMs: overrides?.timeoutMs ?? DEFAULT_QUERY_GUARDRAILS.timeoutMs,
  };
}

export function resolveFallbackPolicy(
  queryPolicy?: QueryFallbackPolicy,
  providerPolicy?: QueryFallbackPolicy,
): Required<QueryFallbackPolicy> {
  return {
    ...DEFAULT_QUERY_FALLBACK_POLICY,
    ...providerPolicy,
    ...queryPolicy,
  };
}

type ExecutionRowGuardrails = Pick<QueryGuardrails, "maxExecutionRows">;

function validatePositiveSafeIntegerResult(guardrail: string, value: number) {
  if (Number.isSafeInteger(value) && value >= 1) {
    return Result.ok(value);
  }

  return Result.err(
    new TuplGuardrailError({
      guardrail,
      limit: Number.MAX_SAFE_INTEGER,
      actual: value,
      message: `Query guardrail ${guardrail} must be a positive safe integer. Received ${value}.`,
    }),
  );
}

function validatePositiveSafeIntegerGuardrailsResult(
  entries: ReadonlyArray<readonly [guardrail: string, value: number]>,
) {
  for (const [guardrail, value] of entries) {
    const result = validatePositiveSafeIntegerResult(guardrail, value);
    if (Result.isError(result)) {
      return result;
    }
  }

  return Result.ok(undefined);
}

export function validateQueryGuardrailsResult(guardrails: QueryGuardrails) {
  const result = validatePositiveSafeIntegerGuardrailsResult([
    ["maxPlannerNodes", guardrails.maxPlannerNodes],
    ["maxExecutionRows", guardrails.maxExecutionRows],
    ["maxLookupKeysPerBatch", guardrails.maxLookupKeysPerBatch],
    ["maxLookupBatches", guardrails.maxLookupBatches],
  ]);
  if (Result.isError(result)) {
    return result;
  }

  return Result.ok(guardrails);
}

export function validateExecutionGuardrailsResult(guardrails: {
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}) {
  const result = validatePositiveSafeIntegerGuardrailsResult([
    ["maxExecutionRows", guardrails.maxExecutionRows],
    ["maxLookupKeysPerBatch", guardrails.maxLookupKeysPerBatch],
    ["maxLookupBatches", guardrails.maxLookupBatches],
  ]);
  if (Result.isError(result)) {
    return result;
  }

  return Result.ok(guardrails);
}

export function enforceMaterializationLimitResult<TRows extends readonly unknown[]>(
  rows: TRows,
  guardrails: ExecutionRowGuardrails,
) {
  const countResult = enforceMaterializationCountResult(rows.length, guardrails);
  if (Result.isError(countResult)) {
    return countResult;
  }

  return Result.ok(rows);
}

export function enforceMaterializationCountResult(
  actual: number,
  guardrails: ExecutionRowGuardrails,
) {
  if (actual > guardrails.maxExecutionRows) {
    return Result.err(
      new TuplGuardrailError({
        guardrail: "maxExecutionRows",
        limit: guardrails.maxExecutionRows,
        actual,
        message: `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${actual} rows.`,
      }),
    );
  }

  return Result.ok(actual);
}

export function appendMaterializedRowResult<T>(
  target: T[],
  row: T,
  guardrails: ExecutionRowGuardrails,
) {
  const actual = target.length + 1;
  const countResult = enforceMaterializationCountResult(actual, guardrails);
  if (Result.isError(countResult)) {
    return countResult;
  }

  target.push(row);
  return Result.ok(undefined);
}

export function appendMaterializedRowsResult<T>(
  target: T[],
  rows: readonly T[],
  guardrails: ExecutionRowGuardrails,
) {
  const actual = target.length + rows.length;
  const countResult = enforceMaterializationCountResult(actual, guardrails);
  if (Result.isError(countResult)) {
    return countResult;
  }

  for (const row of rows) {
    target.push(row);
  }
  return Result.ok(undefined);
}

export function enforcePlannerNodeLimitResult(
  plannerNodeCount: number,
  guardrails: QueryGuardrails,
) {
  if (plannerNodeCount > guardrails.maxPlannerNodes) {
    return Result.err(
      new TuplGuardrailError({
        guardrail: "maxPlannerNodes",
        limit: guardrails.maxPlannerNodes,
        actual: plannerNodeCount,
        message: `Query exceeded maxPlannerNodes guardrail (${guardrails.maxPlannerNodes}). Planned ${plannerNodeCount} nodes.`,
      }),
    );
  }

  return Result.ok(plannerNodeCount);
}

export function isPromiseLike<T>(value: unknown): value is PromiseLike<T> {
  return !!value && typeof value === "object" && "then" in value;
}
