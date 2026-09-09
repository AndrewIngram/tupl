import type { RelNode } from "@tupl/foundation";
import {
  SqlRelationalWithOrderTerm,
  SqlRelationalMetricOrderTerm,
  SqlRelationalQueryTranslationBackend,
  SqlRelationalSelection,
  UnsupportedSqlRelationalPlanError,
} from "@tupl/provider-kit/relational-sql";

import {
  applyWhereClause,
  applyWindowFunction,
  createScopedSource,
  resolveQualifiedColumnRef,
  resolveWithBodyColumnRef,
  toRef,
  executeQuery,
} from "../backend/query-helpers";
import type { KnexLike, KnexLikeQueryBuilder, ResolvedEntityConfig, ScanBinding } from "../types";

export interface ObjectionTranslatedQuery {
  builder: KnexLikeQueryBuilder;
}

/**
 * Objection/Knex query translation owns only Knex-specific query-builder primitives.
 * Provider-kit owns recursive rel lowering, set-op/CTE traversal, and filter replay.
 */
export const objectionQueryTranslationBackend: SqlRelationalQueryTranslationBackend<
  unknown,
  ResolvedEntityConfig<unknown>,
  ScanBinding<unknown>,
  KnexLike,
  ObjectionTranslatedQuery
> = {
  createRootQuery({ runtime, root, context }) {
    return {
      builder: runtime.queryBuilder().from(createScopedSource(root.resolved, context, root.alias)),
    };
  },
  applyRegularJoin({ query: { builder: query }, join, context }) {
    const joinMethod =
      join.joinType === "inner"
        ? "innerJoin"
        : join.joinType === "left"
          ? "leftJoin"
          : join.joinType === "right"
            ? "rightJoin"
            : "fullOuterJoin";

    const fn = (query as unknown as Record<string, unknown>)[joinMethod];
    if (typeof fn !== "function") {
      throw new UnsupportedSqlRelationalPlanError(
        `Knex query builder does not support ${joinMethod} in this dialect.`,
      );
    }

    const rightSource = createScopedSource(join.right.resolved, context, join.right.alias);
    return {
      builder: (fn as (...args: unknown[]) => KnexLikeQueryBuilder).call(
        query,
        rightSource,
        `${join.leftKey.alias}.${join.leftKey.column}`,
        `${join.rightKey.alias}.${join.rightKey.column}`,
      ),
    };
  },
  applySemiJoin({ query: { builder: query }, leftKey, subquery }) {
    return { builder: query.whereIn(`${leftKey.alias}.${leftKey.column}`, subquery.builder) };
  },
  applyWhereClause({ query: { builder: query }, clause, aliases }) {
    return { builder: applyWhereClause(query, clause, aliases) };
  },
  applySelection({ query: { builder: query }, selection, aliases }) {
    const next = query.clearSelect?.() ?? query;
    applySelection(next, selection, aliases);
    return { builder: next };
  },
  applyGroupBy({ query: { builder: query }, groupBy, aliases }) {
    return {
      builder: query.groupBy(
        ...groupBy.map((ref) =>
          resolveQualifiedColumnRef(aliases, {
            ...toRef(ref.alias ?? ref.table, ref.column),
          }),
        ),
      ),
    };
  },
  applyOrderBy({ query: { builder: query }, orderBy, aliases }) {
    let next = query;
    for (const term of orderBy) {
      next =
        term.kind === "metric"
          ? applyMetricOrder(next, term, aliases)
          : next.orderBy(resolveOrderTerm(term, aliases), term.direction);
    }
    return { builder: next };
  },
  applyLimit({ query: { builder: query }, limit }) {
    return { builder: query.limit(limit) };
  },
  applyOffset({ query: { builder: query }, offset }) {
    return { builder: query.offset(offset) };
  },
  applySetOp({ left: { builder: left }, right: { builder: right }, wrapper }) {
    const methodName =
      wrapper.setOp.op === "union_all"
        ? "unionAll"
        : wrapper.setOp.op === "union"
          ? "union"
          : wrapper.setOp.op === "intersect"
            ? "intersect"
            : "except";

    const applySetOp = (left as unknown as Record<string, unknown>)[methodName];
    if (typeof applySetOp !== "function") {
      throw new UnsupportedSqlRelationalPlanError(
        `Knex query builder does not support ${methodName} for single-query pushdown.`,
      );
    }

    return { builder: applySetOp.call(left, [right]) as KnexLikeQueryBuilder };
  },
  buildWithQuery({ body, ctes, projection, orderBy, runtime }) {
    let query = runtime.queryBuilder();

    for (const cte of ctes) {
      const withFn = (query as { with?: unknown }).with;
      if (typeof withFn !== "function") {
        throw new UnsupportedSqlRelationalPlanError(
          "Knex query builder does not support CTE builders required for WITH pushdown.",
        );
      }

      query = withFn.call(query, cte.name, cte.query.builder) as KnexLikeQueryBuilder;
    }

    const scanAlias = body.cteRef.alias ?? body.cteRef.name;
    const fromSource = body.cteRef.alias
      ? ({ [body.cteRef.alias]: body.cteRef.name } as Record<string, string>)
      : body.cteRef.name;
    query = query.from(fromSource);

    const aliases = new Map<string, ScanBinding<unknown>>([
      [
        scanAlias,
        {
          alias: scanAlias,
          entity: body.cteRef.name,
          table: body.cteRef.name,
          scan: {
            ...body.cteRef,
            kind: "scan",
            table: body.cteRef.name,
          },
          resolved: {
            entity: body.cteRef.name,
            table: body.cteRef.name,
            config: {},
          },
        },
      ],
    ]);

    for (const clause of body.cteRef.where ?? []) {
      query = applyWhereClause(query, clause, aliases);
    }
    for (const filter of body.filters) {
      for (const clause of filter.where ?? []) {
        query = applyWhereClause(query, clause, aliases);
      }
    }

    query = query.clearSelect?.() ?? query;

    for (const entry of projection) {
      if (entry.kind === "window") {
        query = applyWindowFunction(query, { ...entry.window, as: entry.output }, scanAlias);
        continue;
      }

      const source = resolveWithBodyColumnRef(entry.source, scanAlias);
      query = query.select({ [entry.output]: source });
    }

    for (const term of orderBy) {
      const source =
        term.kind === "qualified" ? resolveWithBodyColumnRef(term.source, scanAlias) : term.column;
      query = query.orderBy(source, term.direction);
    }

    return { builder: query };
  },
  describeQuery({ query: { builder } }) {
    const compiled = builder.toSQL?.();
    if (!compiled) return undefined;
    const native = compiled.toNative?.() ?? compiled;
    return { sql: native.sql, bindings: native.bindings ?? [] };
  },
  executeQuery({ query: { builder: query } }) {
    return executeQuery(query);
  },
};

function applySelection<TContext>(
  query: KnexLikeQueryBuilder,
  selection: SqlRelationalSelection[],
  aliases: Map<string, ScanBinding<TContext>>,
): void {
  for (const entry of selection) {
    switch (entry.kind) {
      case "column": {
        const source = resolveQualifiedColumnRef(aliases, {
          ...toRef(entry.source.alias ?? entry.source.table, entry.source.column),
        });
        query.select({ [entry.output]: source });
        break;
      }
      case "metric":
        applyMetricSelection(query, aliases, entry.metric, entry.output);
        break;
      case "expr":
        throw new UnsupportedSqlRelationalPlanError(
          "Computed projections are not supported in Objection single-query pushdown.",
        );
    }
  }
}

function applyMetricSelection<TContext>(
  query: KnexLikeQueryBuilder,
  aliases: Map<string, ScanBinding<TContext>>,
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  output: string,
): void {
  if (metric.fn === "count" && !metric.column) {
    query.count({ [output]: "*" });
    return;
  }

  if (!metric.column) {
    throw new UnsupportedSqlRelationalPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveQualifiedColumnRef(aliases, {
    ...toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  if (metric.fn === "count" && metric.distinct) {
    query.countDistinct({ [output]: source });
    return;
  }

  if (metric.fn === "sum" && metric.distinct) {
    throw new UnsupportedSqlRelationalPlanError(
      "Knex sum(distinct ...) is not supported in this adapter yet.",
    );
  }

  if (metric.fn === "avg" && metric.distinct) {
    throw new UnsupportedSqlRelationalPlanError(
      "Knex avg(distinct ...) is not supported in this adapter yet.",
    );
  }

  switch (metric.fn) {
    case "count":
      query.count({ [output]: source });
      break;
    case "sum":
      query.sum({ [output]: source });
      break;
    case "avg":
      query.avg({ [output]: source });
      break;
    case "min":
      query.min({ [output]: source });
      break;
    case "max":
      query.max({ [output]: source });
      break;
  }
}

function resolveOrderTerm<TContext>(
  term: SqlRelationalWithOrderTerm,
  aliases: Map<string, ScanBinding<TContext>>,
): string {
  if (term.kind === "qualified") {
    return resolveQualifiedColumnRef(aliases, {
      ...toRef(term.source.alias ?? term.source.table, term.source.column),
    });
  }

  return term.column;
}

function applyMetricOrder<TContext>(
  query: KnexLikeQueryBuilder,
  term: SqlRelationalMetricOrderTerm,
  aliases: Map<string, ScanBinding<TContext>>,
) {
  if (!query.orderByRaw) {
    throw new UnsupportedSqlRelationalPlanError(
      "Knex query builder does not support aggregate ORDER BY expressions.",
    );
  }
  const direction = term.direction === "asc" ? "asc" : "desc";
  const metric = term.metric;
  if (metric.fn === "count" && !metric.column) {
    return query.orderByRaw(`count(*) ${direction}`);
  }
  if (!metric.column) {
    throw new UnsupportedSqlRelationalPlanError(`Aggregate ${metric.fn} requires a column.`);
  }
  const source = resolveQualifiedColumnRef(
    aliases,
    toRef(metric.column.alias ?? metric.column.table, metric.column.column),
  );
  switch (metric.fn) {
    case "count":
      return query.orderByRaw(`count(${metric.distinct ? "distinct " : ""}??) ${direction}`, [
        source,
      ]);
    case "sum":
      return query.orderByRaw(`sum(${metric.distinct ? "distinct " : ""}??) ${direction}`, [
        source,
      ]);
    case "avg":
      return query.orderByRaw(`avg(${metric.distinct ? "distinct " : ""}??) ${direction}`, [
        source,
      ]);
    case "min":
      return query.orderByRaw(`min(??) ${direction}`, [source]);
    case "max":
      return query.orderByRaw(`max(??) ${direction}`, [source]);
  }
}
