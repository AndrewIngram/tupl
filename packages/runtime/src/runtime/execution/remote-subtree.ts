import { Result } from "better-result";

import { TuplExecutionError, type RelNode } from "@tupl/foundation";
import {
  resolveRelProviderAdapter,
  normalizeCapability,
  unwrapProviderOperationResult,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";
import { mapProviderRowsToRelOutput } from "@tupl/schema-model/mapping";

import {
  tryExecutionStep,
  tryExecutionStepAsync,
  type RemoteExecutionResult,
  type RelExecutionContext,
} from "./local-execution";
import { enforceMaterializationLimitResult } from "../policy";
import {
  describeProviderFragmentExecution,
  describeRelExecution,
  type RelExecutionObservation,
} from "./execution-observer";

/**
 * Remote subtree owns provider pushdown for non-scan relational subtrees.
 */
export async function tryExecuteRemoteSubtreeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
  observation?: RelExecutionObservation,
): Promise<RemoteExecutionResult> {
  if (node.kind === "scan") {
    return Result.ok(null);
  }

  const fragmentResult = buildProviderFragmentForRelResult(node, context.schema, context.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }
  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok(null);
  }

  observation?.updateDescriptor(describeProviderFragmentExecution(fragment.provider));

  const provider = resolveRelProviderAdapter(node, fragment.provider, context.providers);
  if (!provider) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute relational node",
        message: `Missing provider: ${fragment.provider}`,
      }),
    );
  }

  const capabilityResult = await tryExecutionStepAsync("check subtree provider capability", () =>
    Promise.resolve(provider.canExecute(fragment.rel, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    observation?.updateDescriptor(describeRelExecution(node));
    return Result.ok(null);
  }

  const compiledResult = await tryExecutionStepAsync("compile subtree provider fragment", () =>
    Promise.resolve(provider.compile(fragment.rel, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }

  const rowsResult = await tryExecutionStepAsync("execute subtree provider fragment", () =>
    Promise.resolve(provider.execute(compiledResult.value, context.context)).then(
      unwrapProviderOperationResult,
    ),
  );
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }
  const providerRowsLimitResult = enforceMaterializationLimitResult(
    rowsResult.value,
    context.guardrails,
  );
  if (Result.isError(providerRowsLimitResult)) {
    return providerRowsLimitResult;
  }

  return tryExecutionStep("map provider rows to logical rel output rows", () =>
    mapProviderRowsToRelOutput(rowsResult.value, fragment.rel, context.schema),
  );
}
