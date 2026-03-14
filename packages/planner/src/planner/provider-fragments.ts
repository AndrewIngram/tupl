import { Result } from "better-result";

import { type RelNode } from "@tupl/foundation";
import { type SchemaDefinition } from "@tupl/schema-model";
import type { ProviderFragment } from "@tupl/provider-kit";
import { toProviderFragmentBuildError } from "./planner-errors";
import { resolveSingleProvider } from "./provider/conventions";
import { normalizeRelForProvider } from "./provider/provider-rel-normalization";
import { expandRelViewsResult } from "./view-expansion";

/**
 * Provider fragments own normalization of planner nodes into provider-facing fragment requests.
 */
export function buildProviderFragmentForRelResult<TContext = unknown>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.gen(function* () {
    const expanded = yield* expandRelViewsResult(node, schema, context);
    const provider = resolveSingleProvider(expanded, schema);
    if (!provider) {
      return Result.ok(null);
    }

    return buildProviderFragmentForNodeResult(expanded, schema, provider);
  });
}

export function buildProviderFragmentForNodeResult(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
) {
  return Result.try({
    try: () => buildProviderFragmentForNode(node, schema, provider),
    catch: (error) => toProviderFragmentBuildError(error, "build provider fragment"),
  });
}

function buildProviderFragmentForNode(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment {
  return {
    kind: "rel",
    provider,
    rel: normalizeRelForProvider(node, schema),
  };
}
