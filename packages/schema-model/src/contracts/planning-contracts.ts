import type { TableColumns } from "./schema-contracts";
import type { ScanFilterClause, ScanOrderBy, TableAggregateMetric } from "./query-contracts";

/**
 * Planning contracts define table-method planning inputs and remote-vs-residual decisions.
 */
export interface PlannedFilterTerm<
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  id: string;
  clause: ScanFilterClause<TColumn, TColumns>;
}

export interface PlannedOrderTerm<TColumn extends string = string> {
  id: string;
  term: ScanOrderBy<TColumn>;
}

export interface PlannedAggregateMetricTerm<TColumn extends string = string> {
  id: string;
  metric: TableAggregateMetric<TColumn>;
}

export interface PlannedScanRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  select: TColumn[];
  where?: PlannedFilterTerm<TColumn, TColumns>[];
  orderBy?: PlannedOrderTerm<TColumn>[];
  limit?: number;
  offset?: number;
}

export interface PlannedLookupRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  key: TColumn;
  values: unknown[];
  select: TColumn[];
  where?: PlannedFilterTerm<TColumn, TColumns>[];
}

export interface PlannedAggregateRequest<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  table: TTable;
  alias?: string;
  where?: PlannedFilterTerm<TColumn, TColumns>[];
  groupBy?: TColumn[];
  metrics: PlannedAggregateMetricTerm<TColumn>[];
  limit?: number;
}

export interface PlanRejectDecision {
  code: string;
  message: string;
}

export interface ScanPlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  orderByIds?: string[];
  limitOffset?: "push" | "residual";
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface ScanPlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    orderBy?: ScanOrderBy<TColumn>[];
    limit?: number;
    offset?: number;
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    orderBy?: ScanOrderBy<TColumn>[];
    limit?: number;
    offset?: number;
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type ScanPlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = ScanPlanDecisionById | ScanPlanDecisionRemoteResidual<TTable, TColumn, TColumns>;

export interface LookupPlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface LookupPlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type LookupPlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = LookupPlanDecisionById | LookupPlanDecisionRemoteResidual<TTable, TColumn, TColumns>;

export interface AggregatePlanDecisionById {
  mode?: "by_id";
  whereIds?: string[];
  metricIds?: string[];
  groupBy?: "push" | "residual";
  limit?: "push" | "residual";
  reject?: PlanRejectDecision;
  notes?: string[];
}

export interface AggregatePlanDecisionRemoteResidual<
  _TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> {
  mode: "remote_residual";
  remote?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    groupBy?: TColumn[];
    metrics?: TableAggregateMetric<TColumn>[];
    limit?: number;
  };
  residual?: {
    where?: ScanFilterClause<TColumn, TColumns>[];
    groupBy?: TColumn[];
    metrics?: TableAggregateMetric<TColumn>[];
    limit?: number;
  };
  reject?: PlanRejectDecision;
  notes?: string[];
}

export type AggregatePlanDecision<
  TTable extends string = string,
  TColumn extends string = string,
  TColumns extends TableColumns = any,
> = AggregatePlanDecisionById | AggregatePlanDecisionRemoteResidual<TTable, TColumn, TColumns>;
