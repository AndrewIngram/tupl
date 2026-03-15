import type { ProviderCompiledPlan, QueryRow } from "@tupl/provider-kit";

import { executeDrizzleRelSingleQuery } from "../planning/rel-builder";
import type { DrizzleRelCompiledPlan } from "../planning/rel-strategy";
import type { CreateDrizzleProviderOptions } from "../types";
import type { DrizzleQueryExecutor } from "../types";

export async function executeCompiledPlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
  db: DrizzleQueryExecutor,
): Promise<QueryRow[]> {
  if (plan.kind !== "rel") {
    throw new Error(`Unsupported Drizzle compiled plan kind: ${plan.kind}`);
  }

  const compiled = plan.payload as DrizzleRelCompiledPlan;
  return executeDrizzleRelSingleQuery(compiled.rel, compiled.strategy, options, context, db);
}
