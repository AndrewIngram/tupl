import type { RelExpr } from "@tupl/foundation";

import { mapBinaryOperatorToRelFunction, parseLiteral } from "../sql-expr-lowering";

type LowerHavingExprFn = (raw: unknown) => RelExpr | null;

/**
 * Having function exprs own recursive binary/function lowering for HAVING predicates.
 */
export function lowerHavingBinaryExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  lowerHavingExpr: LowerHavingExprFn,
): RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerHavingExpr(expr.left);
    const low = lowerHavingExpr(range.value[0]);
    const high = lowerHavingExpr(range.value[1]);
    if (!left || !low || !high) {
      return null;
    }
    return {
      kind: "function",
      name: "between",
      args: [left, low, high],
    };
  }

  if (operator === "IN" || operator === "NOT IN") {
    const left = lowerHavingExpr(expr.left);
    const values = parseHavingExprListToRelExprArgs(expr.right, lowerHavingExpr);
    if (!left || !values) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "NOT IN" ? "not_in" : "in",
      args: [left, ...values],
    };
  }

  if (operator === "IS" || operator === "IS NOT") {
    const left = lowerHavingExpr(expr.left);
    const rightLiteral = parseLiteral(expr.right);
    if (!left || rightLiteral !== null) {
      return null;
    }
    return {
      kind: "function",
      name: operator === "IS NOT" ? "is_not_null" : "is_null",
      args: [left],
    };
  }

  const left = lowerHavingExpr(expr.left);
  const right = lowerHavingExpr(expr.right);
  if (!left || !right) {
    return null;
  }

  const mapped = mapBinaryOperatorToRelFunction(operator);
  if (!mapped) {
    return null;
  }

  return {
    kind: "function",
    name: mapped,
    args: [left, right],
  };
}

export function lowerHavingFunctionExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  lowerHavingExpr: LowerHavingExprFn,
): RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  const args = parseHavingFunctionArgsToRelExpr(expr.args?.value, lowerHavingExpr);
  if (!args) {
    return null;
  }

  if (normalized === "not") {
    return args.length === 1 ? { kind: "function", name: "not", args } : null;
  }

  if (
    normalized !== "lower" &&
    normalized !== "upper" &&
    normalized !== "trim" &&
    normalized !== "length" &&
    normalized !== "substr" &&
    normalized !== "substring" &&
    normalized !== "coalesce" &&
    normalized !== "nullif" &&
    normalized !== "abs" &&
    normalized !== "round" &&
    normalized !== "cast" &&
    normalized !== "case"
  ) {
    return null;
  }

  return {
    kind: "function",
    name: normalized === "substring" ? "substr" : normalized,
    args,
  };
}

function parseHavingFunctionArgsToRelExpr(
  raw: unknown,
  lowerHavingExpr: LowerHavingExprFn,
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerHavingExpr(value);
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseHavingExprListToRelExprArgs(
  raw: unknown,
  lowerHavingExpr: LowerHavingExprFn,
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseHavingFunctionArgsToRelExpr(expr.value, lowerHavingExpr);
}
