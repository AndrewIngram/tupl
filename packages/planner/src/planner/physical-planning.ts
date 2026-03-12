import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import type { PhysicalPlan } from "./physical/physical";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";
import { assignConventions } from "./provider/conventions";
import { createPhysicalPlanningState } from "./physical/physical-plan-state";
import { planPhysicalNodeResult } from "./physical/local-step-planning";
import { expandRelViewsResult } from "./view-expansion";

/**
 * Physical planning owns remote-fragment planning and the step graph built from conventioned RelNode trees.
 */
export async function planPhysicalQuery<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
): Promise<PhysicalPlan> {
  const result = await planPhysicalQueryResult(rel, schema, providers, context, _sql);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

export async function planPhysicalQueryResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  _sql: string,
) {
  return Result.gen(async function* () {
    const expandedRel = yield* expandRelViewsResult(rel, schema, context);
    const plannedRel = assignConventions(expandedRel, schema);
    const state = createPhysicalPlanningState();

    const rootStepId = yield* Result.await(
      planPhysicalNodeResult(plannedRel, schema, providers, context, state),
    );

    return Result.ok({
      rel: plannedRel,
      rootStepId,
      steps: state.steps,
    });
  });
}
