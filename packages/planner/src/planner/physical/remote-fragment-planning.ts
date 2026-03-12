import { Result, type Result as BetterResult } from "better-result";

import { TuplPlanningError, type RelNode } from "@tupl/foundation";
import { normalizeCapability, type ProviderMap } from "@tupl/provider-kit";
import type { SchemaDefinition } from "@tupl/schema-model";

import { resolveSingleProvider } from "../provider/conventions";
import { buildProviderFragmentForNodeResult } from "../provider-fragments";
import { nextPhysicalStepId } from "../physical/planner-ids";
import { toTuplPlanningError } from "../planner-errors";
import type { PhysicalStep } from "../physical/physical";
import { recordPhysicalStep, type PhysicalPlanningState } from "./physical-plan-state";

/**
 * Remote fragment planning owns provider capability checks and remote-fragment step creation.
 */
export async function tryPlanRemoteFragmentResult<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProviderMap<TContext>,
  context: TContext,
  state: PhysicalPlanningState,
): Promise<BetterResult<string | null, TuplPlanningError>> {
  const provider = resolveSingleProvider(node, schema);
  if (!provider) {
    return Result.ok(null);
  }

  const remoteProvider = providers[provider];
  if (!remoteProvider) {
    return Result.err(
      new TuplPlanningError({
        operation: "plan remote fragment",
        message: `Missing provider: ${provider}`,
      }),
    );
  }

  const fragmentResult = buildProviderFragmentForNodeResult(node, schema, provider);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const capabilityResult = await Result.tryPromise({
    try: () => Promise.resolve(remoteProvider.canExecute(fragmentResult.value, context)),
    catch: (error) => toTuplPlanningError(error, "plan remote fragment"),
  });
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  const step: PhysicalStep = {
    id: nextPhysicalStepId("remote_fragment"),
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${provider})`,
    provider,
    fragment: fragmentResult.value,
  };

  return Result.ok(recordPhysicalStep(state, step));
}
