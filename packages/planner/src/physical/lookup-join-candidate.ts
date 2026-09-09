import type { RelJoinNode } from "@tupl/foundation";
import { resolveRelProviderAdapter, type ProvidersMap } from "@tupl/provider-kit";
import { supportsLookupMany } from "@tupl/provider-kit/shapes";
import type { SchemaDefinition } from "@tupl/schema-model";
import { getNormalizedTableBinding } from "@tupl/schema-model/normalization";
import { resolveScanProviderName, resolveSingleProvider } from "../provider/provider-ownership";

/** Shared eligibility for physical plans, session plans, and execution. */
export function resolveLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
) {
  if (join.joinType !== "inner" && join.joinType !== "left") return null;
  const rightScan = join.right.kind === "scan" ? join.right : null;
  // Lookup requests preserve predicates, but cannot replay a scan's global page boundary.
  if (!rightScan || rightScan.limit != null || rightScan.offset != null) return null;
  if (getNormalizedTableBinding(schema, rightScan.table)?.kind === "view") return null;
  const rightAlias = rightScan.alias ?? rightScan.table;
  if ((join.rightKey.alias ?? join.rightKey.table ?? rightAlias) !== rightAlias) return null;
  const leftQualifier = join.leftKey.alias ?? join.leftKey.table;
  const leftKey = leftQualifier ? `${leftQualifier}.${join.leftKey.column}` : join.leftKey.column;
  if (!join.left.output.some((column) => column.name === leftKey)) return null;
  const rightProviderName = resolveScanProviderName(rightScan, schema);
  if (!rightProviderName) return null;
  const rightProvider = resolveRelProviderAdapter(rightScan, rightProviderName, providers);
  if (!rightProvider || !supportsLookupMany(rightProvider)) return null;
  return {
    rightScan,
    rightProvider,
    description: {
      leftProvider: resolveSingleProvider(join.left, schema) ?? "local",
      rightProvider: rightProviderName,
      leftTable: leftQualifier ?? join.left.id,
      rightTable: rightScan.table,
      leftKey: join.leftKey.column,
      rightKey: join.rightKey.column,
      joinType: join.joinType,
    },
  };
}
