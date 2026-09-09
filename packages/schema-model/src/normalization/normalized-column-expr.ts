import type { RelExpr } from "@tupl/foundation";

import { isSchemaDataEntityHandle, isSchemaDslTableToken } from "../dsl/dsl-tokens";
import type { SchemaDataEntityHandle, SchemaDslTableToken } from "../contracts/schema-contracts";

/**
 * Normalized column expr owns rewriting DSL column references into normalized RelExpr refs.
 */
export function resolveColumnExpr(
  expr: RelExpr,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
  resolveLocal?: (expr: Extract<RelExpr, { kind: "local" }>) => RelExpr,
): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "local":
      if (resolveLocal) return resolveLocal(expr);
      return expr;
    case "function":
      return {
        ...expr,
        args: expr.args.map((arg) =>
          resolveColumnExpr(arg, resolveTableToken, resolveEntityToken, resolveLocal),
        ),
      };
    case "column": {
      const tableOrAlias = (expr.ref as { table?: unknown; alias?: unknown }).table;
      if (isSchemaDslTableToken(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveTableToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      if (isSchemaDataEntityHandle(tableOrAlias)) {
        return {
          kind: "column",
          ref: {
            table: resolveEntityToken(tableOrAlias),
            column: expr.ref.column,
          },
        };
      }
      return expr;
    }
    case "subquery":
      return expr;
  }
}
