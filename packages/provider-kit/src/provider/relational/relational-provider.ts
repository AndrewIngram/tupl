import { bindAdapterEntities } from "../entity-handles";
import { AdapterResult } from "../operations";
import type {
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  ProviderPlanDescription,
} from "../contracts";
import {
  canExecuteRelationalFragment,
  resolveRelationalCapabilityContext,
} from "./relational-capabilities";
import { buildRelationalEntityHandles } from "./relational-entities";
import { type RelationalProviderCompileRelArgs } from "./relational-adapter-types";
import type {
  RelationalProviderAdapter,
  RelationalProviderAdapterOptions,
  RelationalProviderAdapterOptionsWithLookup,
  RelationalProviderAdapterWithLookup,
  RelationalProviderEntityConfig,
  RelationalProviderRelCompileStrategy,
} from "./relational-adapter-types";

const DEFAULT_RELATIONAL_ROUTE_FAMILIES = [
  "scan",
  "aggregate",
  "rel-core",
  "rel-advanced",
] as const;
const LOOKUP_CAPABILITY_ATOM = "lookup.bulk" as const;

export type {
  RelationalProviderAdapterOptions,
  RelationalProviderAdapterOptionsWithLookup,
  RelationalProviderCapabilityContext,
  RelationalProviderCompileRelArgs,
  RelationalProviderDescribeArgs,
  RelationalProviderEntityColumnsArgs,
  RelationalProviderEntityConfig,
  RelationalProviderExecuteArgs,
  RelationalProviderLookupArgs,
  RelationalProviderRelCompileStrategy,
  RelationalProviderSupportArgs,
} from "./relational-adapter-types";

function getCapabilityAtoms<TAtoms extends readonly string[] | undefined>(
  declaredAtoms: TAtoms,
  includeLookupAtom: boolean,
) {
  const atoms = [
    ...new Set([...(declaredAtoms ?? []), ...(includeLookupAtom ? [LOOKUP_CAPABILITY_ATOM] : [])]),
  ];
  return atoms.length > 0 ? atoms : undefined;
}

function describeRelationalCompiledPlan(
  name: string,
  plan: ProviderCompiledPlan,
): ProviderPlanDescription {
  switch (plan.kind) {
    case "rel": {
      const payload = plan.payload as { strategy?: unknown; sql?: unknown } | null;
      const strategy =
        payload && typeof payload === "object" && typeof payload.strategy === "string"
          ? payload.strategy
          : undefined;
      const sql =
        payload && typeof payload === "object" && typeof payload.sql === "string"
          ? payload.sql
          : undefined;
      return {
        kind: "rel_fragment",
        summary: strategy ? `${name} rel fragment (${strategy})` : `${name} rel fragment`,
        operations: [
          {
            kind: sql ? "sql" : "rel",
            ...(sql ? { sql } : {}),
            target: name,
            ...(strategy ? { summary: strategy } : {}),
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
    }
    default:
      return {
        kind: "compiled_plan",
        summary: `${name} compiled plan`,
        operations: [
          {
            kind: "compiled_plan",
            target: name,
            raw: plan.payload,
          },
        ],
        raw: plan.payload,
      };
  }
}

export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>,
): RelationalProviderAdapter<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options: RelationalProviderAdapterOptionsWithLookup<TContext, TEntities, TStrategy>,
): RelationalProviderAdapterWithLookup<TContext, TEntities>;
export function createRelationalProviderAdapter<
  TContext,
  TEntities extends Record<string, RelationalProviderEntityConfig>,
  TStrategy extends RelationalProviderRelCompileStrategy,
>(
  options:
    | RelationalProviderAdapterOptions<TContext, TEntities, TStrategy>
    | RelationalProviderAdapterOptionsWithLookup<TContext, TEntities, TStrategy>,
):
  | RelationalProviderAdapter<TContext, TEntities>
  | RelationalProviderAdapterWithLookup<TContext, TEntities> {
  const capabilityAtoms = getCapabilityAtoms(options.declaredAtoms, Boolean(options.lookupMany));
  const adapter = {
    name: options.name,
    routeFamilies: [
      ...(options.routeFamilies ??
        (options.lookupMany
          ? [...DEFAULT_RELATIONAL_ROUTE_FAMILIES, "lookup"]
          : DEFAULT_RELATIONAL_ROUTE_FAMILIES)),
    ],
    ...(capabilityAtoms ? { capabilityAtoms } : {}),
    ...(options.fallbackPolicy ? { fallbackPolicy: options.fallbackPolicy } : {}),
    canExecute(fragment: ProviderFragment, context: TContext) {
      return canExecuteRelationalFragment(options, fragment, context);
    },
    async compile(fragment: ProviderFragment, context: TContext) {
      const capabilityContext = await resolveRelationalCapabilityContext(
        options,
        fragment,
        context,
      );
      if (!capabilityContext.strategy) {
        return AdapterResult.err(
          new Error(
            options.unsupportedRelCompileMessage ??
              `Unsupported relational fragment for ${options.name} provider.`,
          ),
        );
      }

      const support = await options.isRelStrategySupported?.(capabilityContext);
      if (support !== undefined && support !== true) {
        return AdapterResult.err(
          new Error(typeof support === "string" ? support : (support.reason ?? "Unsupported.")),
        );
      }

      const compileArgs = {
        context,
        entities: options.entities,
        fragment,
        name: options.name,
        strategy: capabilityContext.strategy,
      } satisfies RelationalProviderCompileRelArgs<TContext, TEntities, TStrategy>;

      if (options.compileRelFragment) {
        return options.compileRelFragment(compileArgs);
      }

      return AdapterResult.ok({
        provider: options.name,
        kind: "rel",
        payload: options.buildRelPlanPayload?.(compileArgs) ?? {
          strategy: capabilityContext.strategy,
          rel: fragment.rel,
        },
      } satisfies ProviderCompiledPlan);
    },
    async execute(plan: ProviderCompiledPlan, context: TContext) {
      return options.executeCompiledPlan({
        context,
        entities: options.entities,
        name: options.name,
        plan,
      });
    },
    async describeCompiledPlan(plan: ProviderCompiledPlan, context: TContext) {
      return (
        options.describeCompiledPlan?.({
          context,
          entities: options.entities,
          name: options.name,
          plan,
        }) ?? describeRelationalCompiledPlan(options.name, plan)
      );
    },
  };

  const entities = buildRelationalEntityHandles(adapter, options);
  const boundAdapter = {
    ...adapter,
    entities,
    ...(options.lookupMany
      ? {
          async lookupMany(request: ProviderLookupManyRequest, context: TContext) {
            return options.lookupMany({
              context,
              entities: options.entities,
              name: options.name,
              request,
            });
          },
        }
      : {}),
  };

  return bindAdapterEntities(boundAdapter);
}
