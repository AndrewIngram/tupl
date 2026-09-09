import { resolveLookupJoinCandidate } from "@tupl/planner";
import type { RelJoinNode } from "@tupl/foundation";
import type { QuerySessionInput } from "../session/contracts";

/** Session plans use the same lookup eligibility as physical planning and execution. */
export function resolveSyncLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  input: QuerySessionInput<TContext>,
) {
  return (
    resolveLookupJoinCandidate(join, input.preparedSchema.schema, input.preparedSchema.providers)
      ?.description ?? null
  );
}
