import { Result, type Result as BetterResult } from "better-result";

import {
  RelLoweringError,
  type DataEntityHandle,
  type RelNode,
  type RelExpr,
} from "@tupl/foundation";

import type { SchemaDefinition } from "./contracts/schema-contracts";

/**
 * Rel-schema validation owns checking that relational plans only reference schema-backed tables and columns.
 */
export function validateRelAgainstSchema(
  node: RelNode,
  schema: SchemaDefinition,
): BetterResult<void, RelLoweringError> {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): BetterResult<void, RelLoweringError> => {
    if (entity?.columns) {
      const logicalColumn = column.includes(".")
        ? column.slice(column.lastIndexOf(".") + 1)
        : column;
      if (!Object.hasOwn(entity.columns, logicalColumn)) {
        return Result.err(
          new RelLoweringError({
            operation: "validate relational plan against schema",
            message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
          }),
        );
      }
      return Result.ok(undefined);
    }

    const table = Object.hasOwn(schema.tables, tableName) ? schema.tables[tableName] : undefined;
    if (!table) {
      return Result.ok(undefined);
    }
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;
    if (!Object.hasOwn(table.columns, logicalColumn)) {
      return Result.err(
        new RelLoweringError({
          operation: "validate relational plan against schema",
          message: `Unknown column in relational plan: ${tableName}.${logicalColumn}`,
        }),
      );
    }
    return Result.ok(undefined);
  };

  const visitExpr = (expr: RelExpr): BetterResult<void, RelLoweringError> => {
    if (expr.kind === "subquery") return visit(expr.rel);
    if (expr.kind === "function" || expr.kind === "local") {
      for (const arg of expr.args) {
        const result = visitExpr(arg);
        if (Result.isError(result)) return result;
      }
    }
    return Result.ok(undefined);
  };

  const visit = (current: RelNode): BetterResult<void, RelLoweringError> => {
    switch (current.kind) {
      case "scan":
        if (!Object.hasOwn(schema.tables, current.table) && !current.entity) {
          return Result.err(
            new RelLoweringError({
              operation: "validate relational plan against schema",
              message: `Unknown table in relational plan: ${current.table}`,
            }),
          );
        }
        for (const column of current.select) {
          const result = validateScanColumn(current.table, column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        for (const clause of current.where ?? []) {
          const result = validateScanColumn(current.table, clause.column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        for (const term of current.orderBy ?? []) {
          const result = validateScanColumn(current.table, term.column, current.entity);
          if (Result.isError(result)) {
            return result;
          }
        }
        return Result.ok(undefined);
      case "values":
      case "cte_ref":
        return Result.ok(undefined);
      case "filter":
        return Result.gen(function* () {
          if (current.expr) yield* visitExpr(current.expr);
          yield* visit(current.input);
          return Result.ok(undefined);
        });
      case "project":
        return Result.gen(function* () {
          for (const column of current.columns) {
            if ("expr" in column) yield* visitExpr(column.expr);
          }
          yield* visit(current.input);
          return Result.ok(undefined);
        });
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input);
      case "correlate":
        return Result.gen(function* () {
          yield* visit(current.left);
          yield* visit(current.right);
          return Result.ok(undefined);
        });
      case "join":
      case "set_op":
        return Result.gen(function* () {
          yield* visit(current.left);
          yield* visit(current.right);
          return Result.ok(undefined);
        });
      case "repeat_union":
        return Result.gen(function* () {
          yield* visit(current.seed);
          yield* visit(current.iterative);
          return Result.ok(undefined);
        });
      case "with":
        return Result.gen(function* () {
          for (const cte of current.ctes) {
            yield* visit(cte.query);
          }
          yield* visit(current.body);
          return Result.ok(undefined);
        });
    }
  };

  return visit(node);
}
