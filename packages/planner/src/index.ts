/**
 * Planner owns SQL parsing, relational lowering, and physical plan construction.
 * Callers must not couple to runtime execution policy when using this package.
 */
export * from "./parser";
export * from "./physical/physical";
export {
  expandRelViewsResult,
  lowerSqlToRelResult,
  planPhysicalQueryResult,
  type RelLoweringResult,
} from "./sql-lowering";
export { buildProviderFragmentForRelResult, type ProviderRelTarget } from "./provider-fragments";
export { buildLogicalQueryPlanResult, buildPhysicalQueryPlanResult } from "./planner-pipeline";
export {
  normalizePhysicalPlanForSnapshot,
  normalizeRelForSnapshot,
} from "./translation-normalization";
