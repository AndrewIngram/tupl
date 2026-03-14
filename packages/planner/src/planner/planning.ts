/**
 * Planning is the curated planner surface for SQL lowering, view expansion, and physical planning.
 * Internal planner modules should depend on narrower implementation modules rather than this root.
 */
export {
  buildProviderFragmentForRelResult,
  expandRelViewsResult,
  lowerSqlToRelResult,
  planPhysicalQueryResult,
  type RelLoweringResult,
} from "./sql-lowering";
export { buildLogicalQueryPlanResult, buildPhysicalQueryPlanResult } from "./planner-pipeline";
export {
  normalizePhysicalPlanForSnapshot,
  normalizeRelForSnapshot,
} from "./translation-normalization";
