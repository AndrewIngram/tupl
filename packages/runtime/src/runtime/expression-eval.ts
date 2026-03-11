import { Result, type Result as BetterResult } from "better-result";

import { stringifyUnknownValue, TuplExecutionError, type RelExpr } from "@tupl/foundation";

import {
  compareNonNull,
  compareNullableValues,
  readRowValue,
  testSqlLikePattern,
  toColumnKey,
  type InternalRow,
} from "./row-ops";

/**
 * Expression eval owns local relational expression execution, casts, numeric coercion, and aggregate metrics.
 */
export function evaluateRelExprResult(
  expr: RelExpr,
  row: InternalRow,
  subqueryResults: Map<string, unknown>,
): BetterResult<unknown, TuplExecutionError> {
  switch (expr.kind) {
    case "literal":
      return Result.ok(expr.value);
    case "column":
      return Result.ok(readRowValue(row, toColumnKey(expr.ref)) ?? null);
    case "subquery":
      return Result.ok(subqueryResults.get(expr.id) ?? null);
    case "function": {
      const args: unknown[] = [];
      for (const arg of expr.args) {
        const argResult = evaluateRelExprResult(arg, row, subqueryResults);
        if (Result.isError(argResult)) {
          return argResult;
        }
        args.push(argResult.value);
      }
      switch (expr.name) {
        case "eq":
          return Result.ok(args[0] != null && args[0] === args[1]);
        case "neq":
          return Result.ok(args[0] != null && args[0] !== args[1]);
        case "gt":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) > 0,
          );
        case "gte":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) >= 0,
          );
        case "lt":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) < 0,
          );
        case "lte":
          return Result.ok(
            args[0] != null && args[1] != null && compareNonNull(args[0], args[1]) <= 0,
          );
        case "and":
          return Result.ok(args.every(Boolean));
        case "or":
          return Result.ok(args.some(Boolean));
        case "not":
          return Result.ok(!args[0]);
        case "add": {
          const leftResult = toFiniteNumberResult(args[0], "ADD");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "ADD");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value + rightResult.value);
        }
        case "subtract": {
          const leftResult = toFiniteNumberResult(args[0], "SUBTRACT");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "SUBTRACT");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value - rightResult.value);
        }
        case "multiply": {
          const leftResult = toFiniteNumberResult(args[0], "MULTIPLY");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "MULTIPLY");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value * rightResult.value);
        }
        case "divide": {
          const leftResult = toFiniteNumberResult(args[0], "DIVIDE");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "DIVIDE");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value / rightResult.value);
        }
        case "mod": {
          const leftResult = toFiniteNumberResult(args[0], "MOD");
          if (Result.isError(leftResult)) {
            return leftResult;
          }
          const rightResult = toFiniteNumberResult(args[1], "MOD");
          if (Result.isError(rightResult)) {
            return rightResult;
          }
          return Result.ok(leftResult.value % rightResult.value);
        }
        case "concat":
          return Result.ok(args.map((arg) => stringifyUnknownValue(arg)).join(""));
        case "like":
          return Result.ok(
            typeof args[0] === "string" && typeof args[1] === "string"
              ? testSqlLikePattern(args[0], args[1])
              : false,
          );
        case "not_like":
          return Result.ok(
            typeof args[0] === "string" && typeof args[1] === "string"
              ? !testSqlLikePattern(args[0], args[1])
              : false,
          );
        case "in":
          return Result.ok(args[0] != null && args.slice(1).some((arg) => arg === args[0]));
        case "not_in":
          return Result.ok(args[0] != null && args.slice(1).every((arg) => arg !== args[0]));
        case "is_null":
          return Result.ok(args[0] == null);
        case "is_not_null":
          return Result.ok(args[0] != null);
        case "is_distinct_from":
          return Result.ok(args[0] !== args[1]);
        case "is_not_distinct_from":
          return Result.ok(args[0] === args[1]);
        case "between":
          return Result.ok(
            args[0] != null && args[1] != null && args[2] != null
              ? compareNonNull(args[0], args[1]) >= 0 && compareNonNull(args[0], args[2]) <= 0
              : false,
          );
        case "lower":
          return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).toLowerCase());
        case "upper":
          return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).toUpperCase());
        case "trim":
          return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).trim());
        case "length":
          return Result.ok(args[0] == null ? null : stringifyUnknownValue(args[0]).length);
        case "substr": {
          if (args[0] == null || args[1] == null) {
            return Result.ok(null);
          }
          const input = stringifyUnknownValue(args[0]);
          const startResult = toFiniteNumberResult(args[1], "SUBSTR");
          if (Result.isError(startResult)) {
            return startResult;
          }
          const start = Math.trunc(startResult.value);
          const begin = start >= 0 ? Math.max(0, start - 1) : Math.max(input.length + start, 0);
          if (args[2] == null) {
            return Result.ok(input.slice(begin));
          }
          const lengthResult = toFiniteNumberResult(args[2], "SUBSTR");
          if (Result.isError(lengthResult)) {
            return lengthResult;
          }
          const length = Math.max(0, Math.trunc(lengthResult.value));
          return Result.ok(input.slice(begin, begin + length));
        }
        case "coalesce":
          return Result.ok(args.find((arg) => arg != null) ?? null);
        case "nullif":
          return Result.ok(args[0] === args[1] ? null : (args[0] ?? null));
        case "abs": {
          if (args[0] == null) {
            return Result.ok(null);
          }
          const valueResult = toFiniteNumberResult(args[0], "ABS");
          if (Result.isError(valueResult)) {
            return valueResult;
          }
          return Result.ok(Math.abs(valueResult.value));
        }
        case "round": {
          if (args[0] == null) {
            return Result.ok(null);
          }
          const valueResult = toFiniteNumberResult(args[0], "ROUND");
          if (Result.isError(valueResult)) {
            return valueResult;
          }
          let precision = 0;
          if (args[1] != null) {
            const precisionResult = toFiniteNumberResult(args[1], "ROUND");
            if (Result.isError(precisionResult)) {
              return precisionResult;
            }
            precision = Math.trunc(precisionResult.value);
          }
          const scale = 10 ** precision;
          return Result.ok(Math.round(valueResult.value * scale) / scale);
        }
        case "cast":
          return castValueResult(args[0], args[1]);
        case "case": {
          const lastIndex = args.length - 1;
          const hasElse = args.length % 2 === 1;
          for (let index = 0; index < (hasElse ? lastIndex : args.length); index += 2) {
            if (args[index]) {
              return Result.ok(args[index + 1] ?? null);
            }
          }
          return Result.ok(hasElse ? (args[lastIndex] ?? null) : null);
        }
        default:
          return Result.err(
            new TuplExecutionError({
              operation: "evaluate relational expression",
              message: `Unsupported computed expression function: ${expr.name}`,
            }),
          );
      }
    }
  }
}

function castValueResult(value: unknown, target: unknown) {
  if (value == null) {
    return Result.ok(null);
  }
  const normalized = typeof target === "string" ? target.trim().toLowerCase() : "";
  switch (normalized) {
    case "text":
      return Result.ok(stringifyUnknownValue(value));
    case "integer":
    case "int":
      return Result.ok(Math.trunc(Number(value)));
    case "real":
    case "numeric":
    case "float":
      return Result.ok(Number(value));
    case "boolean":
      return Result.ok(Boolean(value));
    default:
      return Result.err(
        new TuplExecutionError({
          operation: "evaluate relational expression",
          message: `Unsupported CAST target type: ${String(target)}`,
        }),
      );
  }
}

export function evaluateAggregateMetricResult(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): BetterResult<unknown, TuplExecutionError> {
  switch (fn) {
    case "count":
      return Result.ok(hasColumn ? values.filter((value) => value != null).length : bucketSize);
    case "sum": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "SUM");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null);
    }
    case "avg": {
      const numeric: number[] = [];
      for (const value of values.filter((entry) => entry != null)) {
        const numericResult = toFiniteNumberResult(value, "AVG");
        if (Result.isError(numericResult)) {
          return numericResult;
        }
        numeric.push(numericResult.value);
      }
      return Result.ok(
        numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length : null,
      );
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) <= 0 ? left : right,
            )
          : null,
      );
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return Result.ok(
        candidates.length > 0
          ? candidates.reduce((left, right) =>
              compareNullableValues(left, right) >= 0 ? left : right,
            )
          : null,
      );
    }
  }
}

function toFiniteNumberResult(
  value: unknown,
  functionName:
    | "SUM"
    | "AVG"
    | "ADD"
    | "SUBTRACT"
    | "MULTIPLY"
    | "DIVIDE"
    | "MOD"
    | "SUBSTR"
    | "ABS"
    | "ROUND",
) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return Result.err(
      new TuplExecutionError({
        operation: "evaluate relational expression",
        message: `${functionName} expects numeric values.`,
      }),
    );
  }
  return Result.ok(parsed);
}
