import { Result, type Result as BetterResult } from "better-result";

import {
  RelLoweringError,
  type RelColumnRef,
  type RelExpr,
  type RelNode,
  type RelProjectExprMapping,
} from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import {
  getAggregateMetricSignature,
  hasAggregateProjection,
  isWindowProjection,
  lowerHavingExpr,
  parseAggregateProjections,
  parseGroupBy,
  parseOrderBy,
  resolveAggregateGroupBy,
  resolveAggregateOrderBy,
  resolveNonAggregateOrderBy,
  validateAggregateProjectionGroupBy,
} from "../aggregate-lowering";
import type {
  Binding,
  ParsedAggregateMetricProjection,
  ParsedAggregateProjection,
  ResolvedOrderTerm,
  SelectProjection,
  SelectWindowProjection,
} from "../planner-types";
import type { FromEntryAst, SelectAst, WindowSpecificationAst } from "../sqlite-parser/ast";
import type { SqlExprLoweringContext } from "../sql-expr-lowering";
import { parseLimitAndOffset, toRawColumnRef } from "../sql-expr-lowering";
import {
  parseNamedWindows,
  parseProjection,
  parseWindowProjections,
  toParsedOrderSource,
} from "./select-projections";
import { nextRelId } from "../physical/planner-ids";
import { parseJoins } from "./select-from-lowering";
import { parseWhereFilters, validateEnumLiteralFilters } from "../where-lowering";

export interface PreparedSimpleSelect {
  bindings: Binding[];
  aggregateMode: boolean;
  safeAggregateProjections: ParsedAggregateProjection[];
  safeProjections: SelectProjection[];
  aggregateSelectProjections: Array<ParsedAggregateProjection | SelectWindowProjection>;
  aggregateGroupByResolution: {
    groupBy: RelColumnRef[];
    materializations: RelProjectExprMapping[];
  };
  effectiveGroupBy: RelColumnRef[];
  aggregateGroupOutputs: string[];
  allAggregateMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"];
  havingExpr: import("@tupl/foundation").RelExpr | null;
  orderBy: ResolvedOrderTerm[];
  orderByMaterializations: RelProjectExprMapping[];
  limit?: number;
  offset?: number;
  joins: NonNullable<ReturnType<typeof parseJoins>>;
  whereFilters: NonNullable<ReturnType<typeof parseWhereFilters>>;
  windowFunctions: SelectWindowProjection["function"][];
  rootBinding: Binding | null;
}

/**
 * Select-shape preparation owns the analysis pass that turns a parsed SELECT into
 * binding state, aggregate/window mode, and the normalized projection/filter metadata
 * needed by lower phases.
 */
export function prepareSimpleSelectLowering(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteNames: Set<string>,
  tryLowerSelect: (ast: SelectAst) => RelNode | null,
  expandProjection: (ast: SelectAst) => SelectAst,
  enclosingOutputNames?: string[],
): BetterResult<PreparedSimpleSelect | null, RelLoweringError> {
  if (ast.type !== "select" || ast.with || ast.set_op || ast._next) {
    return Result.ok(null);
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (
    from.some(
      (entry) => typeof (entry as FromEntryAst).table !== "string" || (entry as FromEntryAst).stmt,
    )
  ) {
    return Result.ok(null);
  }

  const bindings: Binding[] = [];
  for (const [index, entry] of from.entries()) {
    const table = (entry as FromEntryAst).table;
    const isCte = typeof table === "string" && cteNames.has(table);
    if (typeof table !== "string" || (!schema.tables[table] && !isCte)) {
      return Result.err(
        new RelLoweringError({
          operation: "prepare simple select lowering",
          message: `Unknown table: ${String(table)}`,
        }),
      );
    }

    const alias =
      typeof (entry as FromEntryAst).as === "string" && (entry as FromEntryAst).as
        ? ((entry as FromEntryAst).as as string)
        : table;

    bindings.push({
      table,
      alias,
      index,
      sourceKind: isCte ? "cte" : "table",
    });
  }

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));
  const lowerExprContext: SqlExprLoweringContext = {
    schema,
    cteNames,
    tryLowerSelect,
    expandProjection,
  };

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return Result.ok(null);
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding, lowerExprContext);
  if (!whereFilters) {
    return Result.ok(null);
  }
  const enumFilterValidation = validateEnumLiteralFilters(whereFilters.literals, bindings, schema);
  if (Result.isError(enumFilterValidation)) {
    return enumFilterValidation;
  }

  const distinctMode = ast.distinct === "DISTINCT";
  const aggregateMode = Boolean(ast.groupby || hasAggregateProjection(ast.columns) || distinctMode);
  const namedWindows = parseNamedWindows(ast.window);

  const projections = aggregateMode
    ? null
    : parseProjection(ast.columns, bindings, aliasToBinding, namedWindows, lowerExprContext);
  if (!aggregateMode && projections == null) {
    return Result.ok(null);
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding, lowerExprContext)
    : null;
  if (aggregateMode && aggregateProjections == null) {
    return Result.ok(null);
  }

  const safeAggregateProjections = aggregateMode ? (aggregateProjections ?? []) : [];
  const safeProjections = aggregateMode ? [] : (projections ?? []);
  const aggregateWindowProjections = aggregateMode
    ? parseAggregateWindowProjections(
        ast.columns,
        safeAggregateProjections,
        lowerExprContext,
        namedWindows,
        aliasToBinding,
      )
    : [];
  if (aggregateMode && aggregateWindowProjections == null) {
    return Result.ok(null);
  }
  const aggregateSelectProjections: PreparedSimpleSelect["aggregateSelectProjections"] = [];
  if (aggregateMode && Array.isArray(ast.columns)) {
    let aggregateIndex = 0;
    let windowIndex = 0;
    for (const entry of ast.columns) {
      const projection = isWindowProjection(entry)
        ? aggregateWindowProjections?.[windowIndex++]
        : safeAggregateProjections[aggregateIndex++];
      // Unsupported window specifications may not produce a parsed projection.
      if (!projection) return Result.ok(null);
      aggregateSelectProjections.push(projection);
    }
  }
  const projectionsWithOutputs = aggregateMode ? aggregateSelectProjections : safeProjections;
  const reservedNames = new Set(projectionsWithOutputs.map((projection) => projection.output));
  const outputNames = new Set<string>();
  for (const projection of projectionsWithOutputs) {
    if (outputNames.has(projection.output) && enclosingOutputNames) {
      // Distinct enclosing names allow duplicate SELECT outputs. Keep the first alias
      // visible to SELECT-local clauses and give later values distinct internal names.
      let internalName = nextRelId("select_value");
      while (reservedNames.has(internalName)) internalName = nextRelId("select_value");
      reservedNames.add(internalName);
      projection.output = internalName;
      if (projection.kind === "metric") projection.metric.as = internalName;
      if (projection.kind === "window") projection.function.as = internalName;
    }
    if (outputNames.has(projection.output)) {
      return Result.err(
        new RelLoweringError({
          operation: "validate SELECT outputs",
          message: `Duplicate output column: ${projection.output}. Use explicit projections with unique aliases.`,
        }),
      );
    }
    outputNames.add(projection.output);
  }
  const groupByTerms = aggregateMode ? parseGroupBy(ast.groupby, bindings, aliasToBinding) : [];
  if (aggregateMode && groupByTerms == null) {
    return Result.ok(null);
  }

  const windowFunctions = safeProjections
    .filter((projection): projection is SelectWindowProjection => projection.kind === "window")
    .map((projection) => projection.function)
    .concat((aggregateWindowProjections ?? []).map((projection) => projection.function));

  const aggregateGroupByResolutionResult = aggregateMode
    ? resolveAggregateGroupBy(groupByTerms ?? [], safeAggregateProjections)
    : Result.ok({
        groupBy: [] as RelColumnRef[],
        materializations: [] as RelProjectExprMapping[],
      });
  if (Result.isError(aggregateGroupByResolutionResult)) {
    return aggregateGroupByResolutionResult;
  }
  let effectiveGroupBy = aggregateGroupByResolutionResult.value.groupBy;

  if (distinctMode && effectiveGroupBy.length === 0) {
    const distinctGroupBy: RelColumnRef[] = [];
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group" || !projection.source) {
        return Result.ok(null);
      }
      distinctGroupBy.push(projection.source);
    }
    if (distinctGroupBy.length === 0) {
      return Result.ok(null);
    }
    effectiveGroupBy = distinctGroupBy;
  }

  if (
    aggregateMode &&
    !validateAggregateProjectionGroupBy(safeAggregateProjections, effectiveGroupBy)
  ) {
    return Result.ok(null);
  }

  const aggregateMetrics = safeAggregateProjections
    .filter(
      (projection): projection is ParsedAggregateMetricProjection => projection.kind === "metric",
    )
    .map((projection) => projection.metric);
  const aggregateMetricAliases = new Map<string, string>(
    aggregateMetrics.map((metric) => [getAggregateMetricSignature(metric), metric.as]),
  );
  const havingInputColumnCounts = new Map<string, number>();
  if (aggregateMode && ast.having && from.length > 0) {
    const inputs = expandProjection({ type: "select", columns: "*", from });
    if (Array.isArray(inputs.columns))
      for (const entry of inputs.columns) {
        const ref = toRawColumnRef(entry.expr);
        if (ref)
          havingInputColumnCounts.set(
            ref.column,
            (havingInputColumnCounts.get(ref.column) ?? 0) + 1,
          );
      }
  }
  const hiddenHavingMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"] = [];
  let havingExpr =
    aggregateMode && ast.having
      ? lowerHavingExpr(
          ast.having,
          bindings,
          aliasToBinding,
          aggregateMetricAliases,
          hiddenHavingMetrics,
          (raw) => {
            const ref = toRawColumnRef(raw);
            if (!ref) return null;
            // Grouping one source does not disambiguate a name shared by joined inputs.
            const inputCount = havingInputColumnCounts.get(ref.column) ?? 0;
            if (!ref.table && inputCount > 1) return null;
            const grouped = effectiveGroupBy.filter(
              (source) =>
                source.column === ref.column &&
                (!ref.table || ref.table === (source.alias ?? source.table)),
            );
            if (grouped.length === 1) return { kind: "column", ref: grouped[0]! };
            if (ref.table || grouped.length > 1 || inputCount > 0) return null;
            const projection = safeAggregateProjections.find((item) => item.output === ref.column);
            if (projection?.kind === "group" && projection.source)
              return { kind: "column", ref: projection.source };
            if (projection?.kind === "metric")
              return { kind: "column", ref: { column: projection.metric.as } };
            return null;
          },
        )
      : null;
  if (ast.having && (!aggregateMode || !havingExpr)) {
    return Result.ok(null);
  }
  const allAggregateMetrics = [...aggregateMetrics, ...hiddenHavingMetrics];
  const reservedAggregateNames = new Set([
    ...effectiveGroupBy.map((ref) => ref.column),
    ...allAggregateMetrics.map((metric) => metric.as),
    ...windowFunctions.map((fn) => fn.as),
  ]);
  const usedAggregateNames = new Set([
    ...allAggregateMetrics.map((metric) => metric.as),
    ...windowFunctions.map((fn) => fn.as),
  ]);
  const aggregateGroupOutputs = effectiveGroupBy.map((ref) => {
    let name = ref.column;
    if (usedAggregateNames.has(name)) {
      do {
        name = nextRelId("group_value");
      } while (reservedAggregateNames.has(name));
    }
    usedAggregateNames.add(name);
    reservedAggregateNames.add(name);
    return name;
  });
  const groupOutputBySource = new Map(
    effectiveGroupBy.map((ref, index) => [
      `${ref.alias ?? ref.table ?? ""}.${ref.column}`,
      aggregateGroupOutputs[index]!,
    ]),
  );
  const havingRef = (ref: RelColumnRef): RelColumnRef => ({
    column: groupOutputBySource.get(`${ref.alias ?? ref.table ?? ""}.${ref.column}`) ?? ref.column,
  });
  if (havingExpr) havingExpr = mapExpressionRefs(havingExpr, havingRef);
  const windowRef = (ref: RelColumnRef): RelColumnRef => {
    const qualifier = ref.alias || ref.table;
    const index = effectiveGroupBy.findIndex(
      (source) =>
        source.column === ref.column &&
        (!qualifier || qualifier === (source.alias ?? source.table)),
    );
    if (index >= 0) return { column: aggregateGroupOutputs[index]! };
    if (qualifier) return ref;
    const projected = safeAggregateProjections.find(
      (projection) => projection.output === ref.column,
    );
    if (projected?.kind === "metric") return { column: projected.metric.as };
    if (projected?.kind === "group" && projected.source) return havingRef(projected.source);
    return ref;
  };
  for (const fn of windowFunctions) {
    fn.partitionBy = fn.partitionBy.map(windowRef);
    fn.orderBy = fn.orderBy.map((term) => ({ ...term, source: windowRef(term.source) }));
    if ("column" in fn && fn.column) fn.column = windowRef(fn.column);
    if ("value" in fn) fn.value = mapExpressionRefs(fn.value, windowRef);
    if ("defaultExpr" in fn && fn.defaultExpr)
      fn.defaultExpr = mapExpressionRefs(fn.defaultExpr, windowRef);
  }

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
    return Result.ok(null);
  }

  const orderResolution = aggregateMode
    ? Result.gen(function* () {
        const orderBy = yield* resolveAggregateOrderBy(
          orderByTerms,
          safeAggregateProjections,
          groupOutputBySource,
        );
        return Result.ok({
          orderBy,
          materializations: [] as RelProjectExprMapping[],
        });
      })
    : resolveNonAggregateOrderBy(orderByTerms, safeProjections, toParsedOrderSource);
  if (Result.isError(orderResolution)) {
    return orderResolution;
  }
  const { orderBy, materializations: orderByMaterializations } = orderResolution.value;

  const rootBinding = bindings[0] ?? null;

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  return Result.ok({
    bindings,
    aggregateMode,
    safeAggregateProjections,
    safeProjections,
    aggregateSelectProjections,
    aggregateGroupByResolution: aggregateGroupByResolutionResult.value,
    effectiveGroupBy,
    aggregateGroupOutputs,
    allAggregateMetrics,
    havingExpr,
    orderBy,
    orderByMaterializations,
    ...(limit != null ? { limit } : {}),
    ...(offset != null ? { offset } : {}),
    joins,
    whereFilters,
    windowFunctions,
    rootBinding,
  });
}

function parseAggregateWindowProjections(
  rawColumns: unknown,
  aggregateProjections: ParsedAggregateProjection[],
  lowerExprContext: SqlExprLoweringContext,
  namedWindows: Map<string, WindowSpecificationAst>,
  aliasToBinding: Map<string, Binding>,
) {
  const bindings: Binding[] = [
    {
      table: "__aggregate__",
      alias: "",
      index: 0,
      sourceKind: "table",
    },
  ];
  const windowProjections = parseWindowProjections(
    rawColumns,
    bindings,
    aliasToBinding,
    namedWindows,
    lowerExprContext,
  );
  if (windowProjections == null) {
    return null;
  }

  const availableColumns = new Set(
    aggregateProjections.flatMap((projection) => [
      projection.output,
      ...(projection.kind === "group" && projection.source ? [projection.source.column] : []),
    ]),
  );
  for (const projection of windowProjections) {
    const refs = [
      ...projection.function.partitionBy,
      ...projection.function.orderBy.map((term) => term.source),
      ...("column" in projection.function && projection.function.column
        ? [projection.function.column]
        : []),
    ];
    if ("value" in projection.function)
      mapExpressionRefs(projection.function.value, (ref) => {
        refs.push(ref);
        return ref;
      });
    if ("defaultExpr" in projection.function && projection.function.defaultExpr)
      mapExpressionRefs(projection.function.defaultExpr, (ref) => {
        refs.push(ref);
        return ref;
      });
    for (const ref of refs) {
      const qualifier = ref.alias || ref.table;
      const available = qualifier
        ? aggregateProjections.some(
            (projection) =>
              projection.kind === "group" &&
              projection.source?.column === ref.column &&
              (projection.source.alias ?? projection.source.table) === qualifier,
          )
        : availableColumns.has(ref.column);
      if (!available) {
        return null;
      }
    }
  }

  return windowProjections;
}

function mapExpressionRefs(expr: RelExpr, map: (ref: RelColumnRef) => RelColumnRef): RelExpr {
  if (expr.kind === "column") return { ...expr, ref: map(expr.ref) };
  if (expr.kind === "function" || expr.kind === "local")
    return { ...expr, args: expr.args.map((arg) => mapExpressionRefs(arg, map)) };
  return expr;
}
