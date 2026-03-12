import { Result } from "better-result";

import { TuplExecutionError, type RelNode } from "@tupl/foundation";
import {
  getDataEntityAdapter,
  normalizeCapability,
  supportsFragmentExecution,
  unwrapProviderOperationResult,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";
import { mapProviderRowsToRelOutput } from "@tupl/schema-model";

import {
  tryExecutionStep,
  tryExecutionStepAsync,
  type RemoteExecutionResult,
  type RelExecutionContext,
} from "./local-execution";

/**
 * Remote subtree owns provider pushdown for non-scan relational subtrees.
 */
export async function tryExecuteRemoteSubtreeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<RemoteExecutionResult> {
  if (node.kind === "sql" || node.kind === "scan") {
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

  const provider = resolveProviderForNode(node, fragment.provider, context);
  if (!provider) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute relational node",
        message: `Missing provider adapter: ${fragment.provider}`,
      }),
    );
  }

  const capabilityResult = await tryExecutionStepAsync("check subtree provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, context.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = normalizeCapability(capabilityResult.value);
  if (!capability.supported) {
    return Result.ok(null);
  }

  if (!supportsFragmentExecution(provider)) {
    return Result.err(
      new TuplExecutionError({
        operation: "execute relational node",
        message: `Provider ${fragment.provider} does not support compiled fragment execution.`,
      }),
    );
  }

  const compiledResult = await tryExecutionStepAsync("compile subtree provider fragment", () =>
    Promise.resolve(provider.compile(fragment, context.context)).then(
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

  if (fragment.kind === "rel") {
    return tryExecutionStep("map provider rows to logical rel output rows", () =>
      mapProviderRowsToRelOutput(rowsResult.value, fragment.rel, context.schema),
    );
  }

  return Result.ok(rowsResult.value);
}

function resolveProviderForNode<TContext>(
  node: RelNode,
  providerName: string,
  context: RelExecutionContext<TContext>,
): ProviderAdapter<TContext> | undefined {
  return context.providers[providerName] ?? findNodeProviderAdapter(node, providerName);
}

function findNodeProviderAdapter<TContext>(
  node: RelNode,
  providerName: string,
): ProviderAdapter<TContext> | undefined {
  switch (node.kind) {
    case "scan": {
      if (!node.entity || node.entity.provider !== providerName) {
        return undefined;
      }
      return getDataEntityAdapter(node.entity) as ProviderAdapter<TContext> | undefined;
    }
    case "filter":
    case "project":
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return findNodeProviderAdapter(node.input, providerName);
    case "join":
    case "set_op":
      return (
        findNodeProviderAdapter(node.left, providerName) ??
        findNodeProviderAdapter(node.right, providerName)
      );
    case "with":
      return (
        node.ctes.map((cte) => findNodeProviderAdapter(cte.query, providerName)).find(Boolean) ??
        findNodeProviderAdapter(node.body, providerName)
      );
    case "sql":
      return undefined;
  }
}
