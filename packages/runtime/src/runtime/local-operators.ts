import { Result } from "better-result";

import {
  isRelProjectColumnMapping,
  type RelJoinNode,
  type RelNode,
  type RelProjectNode,
} from "@tupl/foundation";
import type { QueryRow } from "@tupl/schema-model";

import { evaluateAggregateMetricResult, evaluateRelExprResult } from "./expression-eval";
import { maybeExecuteLookupJoinResult, applyLocalHashJoin } from "./lookup-join";
import {
  executeRelNodeResult,
  type RelExecutionContext,
  type RelExecutionResult,
} from "./local-execution";
import {
  compareNullableValues,
  dedupeRows,
  matchesClause,
  readRowValue,
  stableRowKey,
  toColumnKey,
  type InternalRow,
} from "./row-ops";

/**
 * Local operators own in-memory relational operators once inputs have been materialized.
 */
export async function executeFilterResult<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(filter.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let out = [...(rowsResult.value as InternalRow[])];
  for (const clause of filter.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (!filter.expr) {
    return Result.ok(out);
  }

  const filtered: InternalRow[] = [];
  for (const row of out) {
    const exprResult = evaluateRelExprResult(filter.expr, row, context.subqueryResults);
    if (Result.isError(exprResult)) {
      return exprResult;
    }
    if (exprResult.value) {
      filtered.push(row);
    }
  }

  return Result.ok(filtered);
}

export async function executeJoinResult<TContext>(
  join: RelJoinNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const leftRowsResult = await executeRelNodeResult(join.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }

  const lookupResult = await maybeExecuteLookupJoinResult(
    join,
    leftRowsResult.value as InternalRow[],
    context,
  );
  if (Result.isError(lookupResult)) {
    return lookupResult;
  }
  if (lookupResult.value) {
    return Result.ok(lookupResult.value);
  }

  const rightRowsResult = await executeRelNodeResult(join.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  return Result.ok(
    applyLocalHashJoin(
      join,
      leftRowsResult.value as InternalRow[],
      rightRowsResult.value as InternalRow[],
    ),
  );
}

export async function executeProjectResult<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(project.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const out: QueryRow[] = [];
  for (const row of rowsResult.value as InternalRow[]) {
    const projected: QueryRow = {};
    for (const mapping of project.columns) {
      if (isRelProjectColumnMapping(mapping)) {
        projected[mapping.output] = readRowValue(row, toColumnKey(mapping.source)) ?? null;
        continue;
      }

      const exprResult = evaluateRelExprResult(mapping.expr, row, context.subqueryResults);
      if (Result.isError(exprResult)) {
        return exprResult;
      }
      projected[mapping.output] = exprResult.value;
    }
    out.push(projected);
  }

  return Result.ok(out);
}

export async function executeAggregateResult<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(aggregate.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const rows = rowsResult.value as InternalRow[];
  const groups = new Map<string, InternalRow[]>();

  for (const row of rows) {
    const key = JSON.stringify(
      aggregate.groupBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && aggregate.groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];
  for (const [groupKey, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (aggregate.groupBy.length > 0) {
      const values = JSON.parse(groupKey) as unknown[];
      aggregate.groupBy.forEach((ref, index) => {
        const outputName = aggregate.output[index]?.name ?? ref.column;
        row[outputName] = values[index] ?? null;
      });
    }

    for (const metric of aggregate.metrics) {
      const values = metric.column
        ? bucket.map((entry) => readRowValue(entry, toColumnKey(metric.column!)) ?? null)
        : bucket.map(() => 1);
      const metricValues = metric.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      const metricResult = evaluateAggregateMetricResult(
        metric.fn,
        metricValues,
        bucket.length,
        metric.column != null,
      );
      if (Result.isError(metricResult)) {
        return metricResult;
      }
      row[metric.as] = metricResult.value;
    }

    out.push(row);
  }

  return Result.ok(out);
}

export async function executeSortResult<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(sort.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  const sorted = [...(rowsResult.value as InternalRow[])];
  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const comparison = compareNullableValues(
        readRowValue(left, toColumnKey(term.source)),
        readRowValue(right, toColumnKey(term.source)),
      );
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return Result.ok(sorted);
}

export async function executeLimitOffsetResult<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const rowsResult = await executeRelNodeResult(limitOffset.input, context);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  let rows = rowsResult.value;
  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }
  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return Result.ok(rows);
}

export async function executeSetOpResult<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const leftRowsResult = await executeRelNodeResult(setOp.left, context);
  if (Result.isError(leftRowsResult)) {
    return leftRowsResult;
  }
  const rightRowsResult = await executeRelNodeResult(setOp.right, context);
  if (Result.isError(rightRowsResult)) {
    return rightRowsResult;
  }

  const leftRows = leftRowsResult.value;
  const rightRows = rightRowsResult.value;

  switch (setOp.op) {
    case "union_all":
      return Result.ok([...leftRows, ...rightRows]);
    case "union":
      return Result.ok(dedupeRows([...leftRows, ...rightRows]));
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row)))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return Result.ok(dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row)))));
    }
  }
}

export async function executeWithResult<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const cteRows = new Map(context.cteRows);
  const nested: RelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rowsResult = await executeRelNodeResult(cte.query, nested);
    if (Result.isError(rowsResult)) {
      return rowsResult;
    }
    cteRows.set(cte.name, rowsResult.value);
  }

  return executeRelNodeResult(withNode.body, nested);
}
