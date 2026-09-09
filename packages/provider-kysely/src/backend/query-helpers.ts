import type { ScanFilterClause } from "@tupl/provider-kit";
import { resolveColumnFromFilterColumn, resolveColumnRef } from "@tupl/provider-kit/shapes";

import type {
  KyselyDatabaseLike,
  KyselyQueryBuilderLike,
  ResolvedEntityConfig,
  ScanBinding,
} from "../types";

export function toRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  if (alias) {
    return { alias, column };
  }
  return { column };
}

/** The only physical-source constructor, shared by relational reads and lookups. */
export async function createScopedSource<TContext>(
  db: KyselyDatabaseLike,
  binding: {
    entity: string;
    table: string;
    alias: string;
    resolved: ResolvedEntityConfig<TContext>;
  },
  context: TContext,
) {
  const from = `${binding.table} as ${binding.alias}`;
  const base = binding.resolved.config.base;
  if (!base) return from;
  const query = await base({
    db,
    query: db.selectFrom(from),
    context,
    entity: binding.entity,
    alias: binding.alias,
  });
  return query.selectAll().as(binding.alias);
}

export function applyWhereClause<TContext>(
  query: KyselyQueryBuilderLike,
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): KyselyQueryBuilderLike {
  const column = resolveFilterColumn(aliases, clause.column);

  switch (clause.op) {
    case "eq":
      return query.where(column, "=", clause.value);
    case "neq":
      return query.where(column, "!=", clause.value);
    case "gt":
      return query.where(column, ">", clause.value);
    case "gte":
      return query.where(column, ">=", clause.value);
    case "lt":
      return query.where(column, "<", clause.value);
    case "lte":
      return query.where(column, "<=", clause.value);
    case "in":
      return query.where(column, "in", clause.values.length > 0 ? clause.values : [null]);
    case "not_in":
      return query.where(column, "not in", clause.values.length > 0 ? clause.values : [null]);
    case "like":
      return query.where(column, "like", clause.value);
    case "not_like":
      return query.where(column, "not like", clause.value);
    case "is_distinct_from":
      return query.where(column, "is distinct from", clause.value);
    case "is_not_distinct_from":
      return query.where(column, "is not distinct from", clause.value);
    case "is_null":
      return query.where(column, "is", null);
    case "is_not_null":
      return query.where(column, "is not", null);
  }
}

export function resolveFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): string {
  return resolveColumnFromFilterColumn(aliases, column);
}

export function resolveQualifiedColumnRef<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): string {
  return resolveColumnRef(aliases, ref);
}
