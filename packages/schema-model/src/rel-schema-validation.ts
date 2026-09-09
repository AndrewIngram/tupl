import { Result, type Result as BetterResult } from "better-result";

import {
  RelLoweringError,
  type DataEntityHandle,
  type RelNode,
  type RelExpr,
  type RelColumnRef,
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

  type CteScope = ReadonlyMap<string, ReadonlySet<string>>;
  const validateRef = (
    ref: RelColumnRef,
    columns: ReadonlySet<string>,
  ): BetterResult<void, RelLoweringError> => {
    const qualifier = ref.alias || ref.table;
    const name = qualifier ? `${qualifier}.${ref.column}` : ref.column;
    if (columns.has(name)) return Result.ok(undefined);
    if (
      !qualifier &&
      [...columns].filter((column) => column.endsWith(`.${ref.column}`)).length === 1
    )
      return Result.ok(undefined);
    return Result.err(
      new RelLoweringError({
        operation: "validate relational input reference",
        message: `Unavailable column in relational input: ${name}`,
      }),
    );
  };
  const visitExpr = (
    expr: RelExpr,
    ctes: CteScope,
    columns: ReadonlySet<string>,
  ): BetterResult<void, RelLoweringError> => {
    if (expr.kind === "column") return validateRef(expr.ref, columns);
    if (expr.kind === "subquery") return visit(expr.rel, ctes, columns);
    if (expr.kind === "function" || expr.kind === "local") {
      for (const arg of expr.args) {
        const result = visitExpr(arg, ctes, columns);
        if (Result.isError(result)) return result;
      }
    }
    return Result.ok(undefined);
  };

  const visit = (
    current: RelNode,
    ctes: CteScope,
    outer: ReadonlySet<string> = new Set(),
  ): BetterResult<void, RelLoweringError> => {
    const inputColumns = new Set([
      ...outer,
      ...("input" in current ? current.input.output.map((column) => column.name) : []),
    ]);
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
        return Result.ok(undefined);
      case "cte_ref": {
        const columns = ctes.get(current.name);
        if (!columns)
          return Result.err(
            new RelLoweringError({
              operation: "validate CTE reference",
              message: `Unknown CTE: ${current.name}`,
            }),
          );
        for (const column of [
          ...current.select,
          ...(current.where ?? []).map((clause) => clause.column),
          ...(current.orderBy ?? []).map((term) => term.column),
        ]) {
          if (!columns.has(column))
            return Result.err(
              new RelLoweringError({
                operation: "validate CTE reference",
                message: `Unknown column in CTE: ${current.name}.${column}`,
              }),
            );
        }
        return Result.ok(undefined);
      }
      case "filter":
        return Result.gen(function* () {
          if (current.expr) yield* visitExpr(current.expr, ctes, inputColumns);
          for (const clause of current.where ?? [])
            yield* validateRef({ column: clause.column }, inputColumns);
          yield* visit(current.input, ctes, outer);
          return Result.ok(undefined);
        });
      case "project":
        return Result.gen(function* () {
          for (const column of current.columns) {
            if ("expr" in column) yield* visitExpr(column.expr, ctes, inputColumns);
            else yield* validateRef(column.source, inputColumns);
          }
          yield* visit(current.input, ctes, outer);
          return Result.ok(undefined);
        });
      case "aggregate":
        return Result.gen(function* () {
          for (const ref of current.groupBy) yield* validateRef(ref, inputColumns);
          for (const metric of current.metrics)
            if (metric.column) yield* validateRef(metric.column, inputColumns);
          yield* visit(current.input, ctes, outer);
          return Result.ok(undefined);
        });
      case "window":
        return Result.gen(function* () {
          for (const fn of current.functions) {
            for (const ref of fn.partitionBy) yield* validateRef(ref, inputColumns);
            for (const term of fn.orderBy) yield* validateRef(term.source, inputColumns);
            if ("column" in fn && fn.column) yield* validateRef(fn.column, inputColumns);
            if ("value" in fn) yield* visitExpr(fn.value, ctes, inputColumns);
            if ("defaultExpr" in fn && fn.defaultExpr)
              yield* visitExpr(fn.defaultExpr, ctes, inputColumns);
          }
          yield* visit(current.input, ctes, outer);
          return Result.ok(undefined);
        });
      case "sort":
        return Result.gen(function* () {
          for (const term of current.orderBy) yield* validateRef(term.source, inputColumns);
          yield* visit(current.input, ctes, outer);
          return Result.ok(undefined);
        });
      case "limit_offset":
        return visit(current.input, ctes, outer);
      case "correlate":
        return Result.gen(function* () {
          yield* visit(current.left, ctes, outer);
          yield* visit(current.right, ctes, outer);
          return Result.ok(undefined);
        });
      case "join":
        return Result.gen(function* () {
          yield* validateRef(
            current.leftKey,
            new Set(current.left.output.map((column) => column.name)),
          );
          yield* validateRef(
            current.rightKey,
            new Set(current.right.output.map((column) => column.name)),
          );
          yield* visit(current.left, ctes, outer);
          yield* visit(current.right, ctes, outer);
          return Result.ok(undefined);
        });
      case "set_op":
        return Result.gen(function* () {
          yield* visit(current.left, ctes, outer);
          yield* visit(current.right, ctes, outer);
          return Result.ok(undefined);
        });
      case "repeat_union":
        return Result.gen(function* () {
          yield* visit(current.seed, ctes);
          const recursiveScope = new Map(ctes);
          recursiveScope.set(
            current.cteName,
            new Set(current.seed.output.map((column) => column.name)),
          );
          yield* visit(current.iterative, recursiveScope);
          return Result.ok(undefined);
        });
      case "with":
        return Result.gen(function* () {
          const scope = new Map(ctes);
          for (const cte of current.ctes) {
            yield* visit(cte.query, scope);
            scope.set(cte.name, new Set(cte.query.output.map((column) => column.name)));
          }
          yield* visit(current.body, scope, outer);
          return Result.ok(undefined);
        });
    }
  };

  return visit(node, new Map());
}
