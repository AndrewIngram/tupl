import type { RelColumnRef, RelNode, RelProjectExprMapping } from "@tupl/foundation";
import type { FromEntryAst, SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import {
  getAggregateMetricSignature,
  hasAggregateProjection,
  lowerHavingExpr,
  parseAggregateProjections,
  parseGroupBy,
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
} from "./aggregate-lowering";
import { nextRelId } from "./planner-ids";
import type { Binding, SelectWindowProjection } from "./planner-types";
import {
  collectRelExprRefs,
  isCorrelatedSubquery,
  parseLimitAndOffset,
  type SqlExprLoweringContext,
} from "./sql-expr-lowering";
import {
  appendProjectExpressions,
  parseNamedWindows,
  parseProjection,
  toParsedOrderSource,
} from "./select-projections";
import { appearsInRel, parseJoins, parseRelColumnRef } from "./select-from-lowering";
import {
  combineAndExprs,
  getPushableWhereAliases,
  literalFilterToRelExpr,
  parseWhereFilters,
  validateEnumLiteralFilters,
} from "./where-lowering";

/**
 * Simple-select lowering owns lowering a single SELECT core into relational nodes.
 */
export function tryLowerSimpleSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
  tryLowerSelect: (ast: SelectAst) => RelNode | null,
): RelNode | null {
  if (ast.type !== "select" || ast.with || ast.set_op || ast._next) {
    return null;
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (from.length === 0) {
    return null;
  }

  if (
    from.some(
      (entry) => typeof (entry as FromEntryAst).table !== "string" || (entry as FromEntryAst).stmt,
    )
  ) {
    return null;
  }

  const bindings: Binding[] = from.map((entry, index) => {
    const table = (entry as FromEntryAst).table;
    if (typeof table !== "string" || (!schema.tables[table] && !cteNames.has(table))) {
      throw new Error(`Unknown table: ${String(table)}`);
    }

    const alias =
      typeof (entry as FromEntryAst).as === "string" && (entry as FromEntryAst).as
        ? ((entry as FromEntryAst).as as string)
        : table;

    return { table, alias, index };
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));
  const lowerExprContext: SqlExprLoweringContext = {
    schema,
    cteNames,
    tryLowerSelect,
  };

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return null;
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding, lowerExprContext);
  if (!whereFilters) {
    return null;
  }
  validateEnumLiteralFilters(whereFilters.literals, bindings, schema);

  const distinctMode = ast.distinct === "DISTINCT";
  const aggregateMode = ast.groupby || hasAggregateProjection(ast.columns) || distinctMode;

  const projections = aggregateMode
    ? null
    : parseProjection(
        ast.columns,
        bindings,
        aliasToBinding,
        parseNamedWindows(ast.window),
        lowerExprContext,
      );
  if (!aggregateMode && projections == null) {
    return null;
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding, lowerExprContext)
    : null;
  if (aggregateMode && aggregateProjections == null) {
    return null;
  }

  const safeAggregateProjections = aggregateMode ? (aggregateProjections ?? []) : [];
  const safeProjections = aggregateMode ? [] : (projections ?? []);
  const groupByTerms = aggregateMode ? parseGroupBy(ast.groupby, bindings, aliasToBinding) : [];
  if (aggregateMode && groupByTerms == null) {
    return null;
  }
  const windowFunctions = safeProjections
    .filter((projection): projection is SelectWindowProjection => projection.kind === "window")
    .map((projection) => projection.function);
  const aggregateGroupByResolution = aggregateMode
    ? resolveAggregateGroupBy(groupByTerms ?? [], safeAggregateProjections)
    : { groupBy: [], materializations: [] };
  let effectiveGroupBy = aggregateGroupByResolution.groupBy;

  if (distinctMode && effectiveGroupBy.length === 0) {
    const distinctGroupBy: RelColumnRef[] = [];
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group" || !projection.source) {
        return null;
      }
      distinctGroupBy.push(projection.source);
    }

    if (distinctGroupBy.length === 0) {
      return null;
    }
    effectiveGroupBy = distinctGroupBy;
  }

  if (
    aggregateMode &&
    !validateAggregateProjectionGroupBy(safeAggregateProjections, effectiveGroupBy)
  ) {
    return null;
  }

  const aggregateMetrics = safeAggregateProjections
    .filter(
      (projection): projection is import("./planner-types").ParsedAggregateMetricProjection =>
        projection.kind === "metric",
    )
    .map((projection) => projection.metric);

  const aggregateMetricAliases = new Map<string, string>(
    aggregateMetrics.map((metric) => [getAggregateMetricSignature(metric), metric.as]),
  );
  const hiddenHavingMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"] = [];
  const havingExpr =
    aggregateMode && ast.having
      ? lowerHavingExpr(
          ast.having,
          bindings,
          aliasToBinding,
          aggregateMetricAliases,
          hiddenHavingMetrics,
        )
      : null;
  if (ast.having && (!aggregateMode || !havingExpr)) {
    return null;
  }
  const allAggregateMetrics = [...aggregateMetrics, ...hiddenHavingMetrics];

  const orderByTerms = parseOrderBy(
    ast.orderby,
    bindings,
    aliasToBinding,
    new Set(
      (aggregateMode ? safeAggregateProjections : safeProjections).map(
        (projection) => projection.output,
      ),
    ),
  );
  if (orderByTerms == null) {
    return null;
  }
  const { orderBy, materializations: orderByMaterializations } = aggregateMode
    ? {
        orderBy: resolveAggregateOrderBy(orderByTerms, safeAggregateProjections),
        materializations: [] as RelProjectExprMapping[],
      }
    : resolveNonAggregateOrderBy(orderByTerms, safeProjections, toParsedOrderSource);

  const rootBinding = bindings[0];
  if (!rootBinding) {
    return null;
  }
  const pushableWhereAliases = getPushableWhereAliases(rootBinding.alias, joins);
  const pushableLiteralFilters = whereFilters.literals.filter((filter) =>
    pushableWhereAliases.has(filter.alias),
  );
  const residualExpr = combineAndExprs([
    ...whereFilters.literals
      .filter((filter) => !pushableWhereAliases.has(filter.alias))
      .map(literalFilterToRelExpr),
    ...(whereFilters.residualExpr ? [whereFilters.residualExpr] : []),
  ]);

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  const columnsByAlias = new Map<string, Set<string>>();
  for (const binding of bindings) {
    columnsByAlias.set(binding.alias, new Set<string>());
  }

  if (aggregateMode) {
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group") {
        continue;
      }

      if (projection.source?.alias) {
        columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
      }
      if (projection.expr) {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
      }
    }

    for (const metric of allAggregateMetrics) {
      if (!metric.column) {
        continue;
      }
      const alias = metric.column.alias ?? metric.column.table;
      if (alias) {
        columnsByAlias.get(alias)?.add(metric.column.column);
      }
    }

    for (const ref of effectiveGroupBy) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  } else {
    for (const projection of safeProjections) {
      if (projection.kind === "column") {
        if (projection.source.alias) {
          columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
        }
        continue;
      }
      if (projection.kind === "expr") {
        for (const ref of collectRelExprRefs(projection.expr)) {
          if (ref.alias) {
            columnsByAlias.get(ref.alias)?.add(ref.column);
          }
        }
        continue;
      }
      for (const partition of projection.function.partitionBy) {
        if (partition.alias) {
          columnsByAlias.get(partition.alias)?.add(partition.column);
        }
      }
      if ("column" in projection.function && projection.function.column?.alias) {
        columnsByAlias
          .get(projection.function.column.alias)
          ?.add(projection.function.column.column);
      }
      for (const orderTerm of projection.function.orderBy) {
        if (orderTerm.source.alias) {
          columnsByAlias.get(orderTerm.source.alias)?.add(orderTerm.source.column);
        }
      }
    }
  }

  for (const join of joins) {
    columnsByAlias.get(join.leftAlias)?.add(join.leftColumn);
    columnsByAlias.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of pushableLiteralFilters) {
    columnsByAlias.get(filter.alias)?.add(filter.clause.column);
  }
  for (const filter of whereFilters.inSubqueries) {
    columnsByAlias.get(filter.alias)?.add(filter.column);
  }
  if (residualExpr) {
    for (const ref of collectRelExprRefs(residualExpr)) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  }

  for (const term of orderBy) {
    if (term.source.alias) {
      columnsByAlias.get(term.source.alias)?.add(term.source.column);
    }
  }

  for (const binding of bindings) {
    const columns = columnsByAlias.get(binding.alias);
    if (!columns || columns.size > 0) {
      continue;
    }

    if (schema.tables[binding.table]) {
      const schemaColumns = Object.keys(schema.tables[binding.table]?.columns ?? {});
      const first = schemaColumns[0];
      if (first) {
        columns.add(first);
      }
    }
  }

  const filtersByAlias = new Map<string, import("@tupl/schema-model").ScanFilterClause[]>();
  for (const filter of pushableLiteralFilters) {
    const current = filtersByAlias.get(filter.alias) ?? [];
    current.push(filter.clause);
    filtersByAlias.set(filter.alias, current);
  }

  const scansByAlias = new Map<string, Extract<RelNode, { kind: "scan" }>>();
  for (const binding of bindings) {
    const select = [...(columnsByAlias.get(binding.alias) ?? new Set<string>())];
    const scanWhere = filtersByAlias.get(binding.alias);

    scansByAlias.set(binding.alias, {
      id: nextRelId("scan"),
      kind: "scan",
      convention: "local",
      table: binding.table,
      alias: binding.alias,
      select,
      ...(scanWhere && scanWhere.length > 0 ? { where: scanWhere } : {}),
      output: select.map((column) => ({
        name: `${binding.alias}.${column}`,
      })),
    });
  }

  let current: RelNode = scansByAlias.get(rootBinding.alias)!;

  for (const join of joins) {
    const right = scansByAlias.get(join.alias);
    if (!right) {
      return null;
    }

    const joinLeftOnCurrent = appearsInRel(current, join.leftAlias);
    const leftKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.leftAlias, column: join.leftColumn }
      : { alias: join.rightAlias, column: join.rightColumn };

    const rightKey: RelColumnRef = joinLeftOnCurrent
      ? { alias: join.rightAlias, column: join.rightColumn }
      : { alias: join.leftAlias, column: join.leftColumn };

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: join.joinType,
      left: current,
      right,
      leftKey,
      rightKey,
      output: [...current.output, ...right.output],
    };
  }

  for (const inFilter of whereFilters.inSubqueries) {
    const outerAliases = new Set(bindings.map((binding) => binding.alias));
    if (isCorrelatedSubquery(inFilter.subquery, outerAliases)) {
      return null;
    }

    const subqueryRel = tryLowerSelect(inFilter.subquery);
    if (!subqueryRel || subqueryRel.output.length !== 1) {
      return null;
    }
    const rightOutput = subqueryRel.output[0]?.name;
    if (!rightOutput) {
      return null;
    }

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: "semi",
      left: current,
      right: subqueryRel,
      leftKey: {
        alias: inFilter.alias,
        column: inFilter.column,
      },
      rightKey: parseRelColumnRef(rightOutput),
      output: current.output,
    };
  }

  if (residualExpr) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: residualExpr,
      output: current.output,
    };
  }

  if (aggregateMode && aggregateGroupByResolution.materializations.length > 0) {
    current = appendProjectExpressions(current, aggregateGroupByResolution.materializations);
  }

  if (aggregateMode) {
    current = {
      id: nextRelId("aggregate"),
      kind: "aggregate",
      convention: "local",
      input: current,
      groupBy: effectiveGroupBy,
      metrics: allAggregateMetrics,
      output: [
        ...effectiveGroupBy.map((ref: RelColumnRef) => ({
          name: ref.column,
        })),
        ...allAggregateMetrics.map((metric) => ({
          name: metric.as,
        })),
      ],
    };
  }

  if (havingExpr) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      expr: havingExpr,
      output: current.output,
    };
  }

  if (!aggregateMode && windowFunctions.length > 0) {
    current = {
      id: nextRelId("window"),
      kind: "window",
      convention: "local",
      input: current,
      functions: windowFunctions,
      output: [...current.output, ...windowFunctions.map((fn) => ({ name: fn.as }))],
    };
  }

  if (!aggregateMode && orderByMaterializations.length > 0) {
    current = appendProjectExpressions(current, orderByMaterializations);
  }

  if (orderBy.length > 0) {
    current = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: current,
      orderBy: orderBy.map(
        (term: { source: { alias?: string; column: string }; direction: "asc" | "desc" }) => ({
          source: term.source.alias
            ? {
                alias: term.source.alias,
                column: term.source.column,
              }
            : {
                column: term.source.column,
              },
          direction: term.direction,
        }),
      ),
      output: current.output,
    };
  }

  if (limit != null || offset != null) {
    current = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: current,
      ...(limit != null ? { limit } : {}),
      ...(offset != null ? { offset } : {}),
      output: current.output,
    };
  }

  return {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: aggregateMode
      ? safeAggregateProjections.map((projection) =>
          projection.kind === "group" && projection.source
            ? {
                kind: "column" as const,
                source: { column: projection.source.column },
                output: projection.output,
              }
            : projection.kind === "metric"
              ? {
                  kind: "column" as const,
                  source: {
                    column: projection.metric.as,
                  },
                  output: projection.output,
                }
              : {
                  kind: "expr" as const,
                  expr: projection.expr!,
                  output: projection.output,
                },
        )
      : safeProjections.map((projection) => ({
          ...(projection.kind === "expr" && !projection.source
            ? {
                kind: "expr" as const,
                expr: projection.expr,
              }
            : {
                kind: "column" as const,
                source:
                  projection.kind === "column"
                    ? {
                        ...(projection.source.alias ? { alias: projection.source.alias } : {}),
                        column: projection.source.column,
                      }
                    : projection.kind === "expr"
                      ? {
                          column: projection.source!.column,
                        }
                      : {
                          column: projection.function.as,
                        },
              }),
          output: projection.output,
        })),
    output: (aggregateMode ? safeAggregateProjections : safeProjections).map((projection) => ({
      name: projection.output,
    })),
  };
}
