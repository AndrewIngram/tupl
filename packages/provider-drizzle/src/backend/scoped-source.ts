import { and, getTableName, is, Table, Subquery, sql, type SQL } from "drizzle-orm";
import { UnsupportedSqlRelationalPlanError } from "@tupl/provider-kit/relational-sql";
import type { DrizzleColumnMap, DrizzleProviderTableConfig } from "../types";

/** All provider reads obtain their physical input here, before applying user predicates. */
export function createScopedSource(
  table: DrizzleProviderTableConfig<unknown>["table"],
  columns: DrizzleColumnMap,
  scope: SQL | SQL[] | undefined,
) {
  const condition = and(...(Array.isArray(scope) ? scope : [scope]));
  if (!condition) return table;
  if (!is(table, Table)) {
    throw new UnsupportedSqlRelationalPlanError("Scoped Drizzle sources must be Drizzle tables.");
  }
  return new Subquery(sql`select * from ${table} where ${condition}`, columns, getTableName(table));
}
