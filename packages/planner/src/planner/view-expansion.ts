import type { RelNode } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import { Result } from "better-result";

import { toRelRewriteError } from "./planner-errors";
import type { ViewExpansionResult } from "./views/view-expansion-types";
import { rewriteExpandedViewNode } from "./views/view-node-rewriting";
import { expandViewScanNode } from "./views/view-scan-expansion";

/**
 * View expansion owns planner-side lowering of normalized view bindings into ordinary RelNode trees.
 */
export function expandRelViewsResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
) {
  return Result.try({
    try: () => expandRelViewsInternal(rel, schema, context).node,
    catch: (error) => toRelRewriteError(error, "expand relational views"),
  });
}

function expandRelViewsInternal<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  context?: TContext,
): ViewExpansionResult {
  switch (node.kind) {
    case "scan":
      return expandViewScanNode(node, schema, context, expandRelViewsInternal);
    case "values":
      return {
        node,
        aliases: new Map(),
      };
    default:
      return rewriteExpandedViewNode(node, schema, context, expandRelViewsInternal);
  }
}
