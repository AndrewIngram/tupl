import type { RelNode } from "@tupl/foundation";
import type { Binding } from "./planner-types";
import { getAggregateMetricSignature, parseAggregateMetric } from "./aggregate-lowering";
import {
  mapBinaryOperatorToRelFunction,
  parseLiteral,
  resolveColumnRef,
} from "./sql-expr-lowering";

/**
 * Having lowering owns aggregate-aware expression lowering for HAVING clauses.
 */
export function lowerHavingExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const expr = raw as {
    type?: unknown;
    value?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
    name?: unknown;
    args?: { value?: unknown; expr?: unknown; distinct?: unknown };
    ast?: unknown;
  };

  switch (expr.type) {
    case "string":
      return { kind: "literal", value: typeof expr.value === "string" ? expr.value : "" };
    case "number":
      return typeof expr.value === "number" ? { kind: "literal", value: expr.value } : null;
    case "bool":
      return typeof expr.value === "boolean" ? { kind: "literal", value: expr.value } : null;
    case "null":
      return { kind: "literal", value: null };
    case "column_ref": {
      const resolved = resolveColumnRef(expr, bindings, aliasToBinding);
      if (!resolved) {
        return null;
      }
      return {
        kind: "column",
        ref: {
          column: resolved.column,
        },
      };
    }
    case "aggr_func": {
      const metric = parseAggregateMetric(
        expr,
        deriveDefaultAggregateOutputName(expr),
        bindings,
        aliasToBinding,
      );
      if (!metric) {
        return null;
      }
      const signature = getAggregateMetricSignature(metric);
      let alias = aggregateMetricAliases.get(signature);
      if (!alias) {
        alias = `__having_metric_${aggregateMetricAliases.size + 1}`;
        aggregateMetricAliases.set(signature, alias);
        hiddenMetrics.push({
          ...metric,
          as: alias,
        });
      }
      return {
        kind: "column",
        ref: {
          column: alias,
        },
      };
    }
    case "binary_expr":
      return lowerHavingBinaryExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    case "function":
      return lowerHavingFunctionExpr(
        expr,
        bindings,
        aliasToBinding,
        aggregateMetricAliases,
        hiddenMetrics,
      );
    default:
      if ("ast" in expr) {
        return null;
      }
      return null;
  }
}

function lowerHavingBinaryExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
  const operator = typeof expr.operator === "string" ? expr.operator.toUpperCase() : null;
  if (!operator) {
    return null;
  }

  if (operator === "BETWEEN") {
    const range = expr.right as { type?: unknown; value?: unknown } | undefined;
    if (range?.type !== "expr_list" || !Array.isArray(range.value) || range.value.length !== 2) {
      return null;
    }
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const low = lowerHavingExpr(
      range.value[0],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const high = lowerHavingExpr(
      range.value[1],
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
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
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    const values = parseHavingExprListToRelExprArgs(
      expr.right,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
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
    const left = lowerHavingExpr(
      expr.left,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
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

  const left = lowerHavingExpr(
    expr.left,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
  const right = lowerHavingExpr(
    expr.right,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
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

function lowerHavingFunctionExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr | null {
  if (expr.over) {
    return null;
  }

  const rawName = (expr.name as { name?: Array<{ value?: unknown }> } | undefined)?.name?.[0]
    ?.value;
  if (typeof rawName !== "string") {
    return null;
  }

  const normalized = rawName.toLowerCase();
  const args = parseHavingFunctionArgsToRelExpr(
    expr.args?.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
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
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: import("@tupl/foundation").RelExpr[] = [];
  for (const value of values) {
    const arg = lowerHavingExpr(
      value,
      bindings,
      aliasToBinding,
      aggregateMetricAliases,
      hiddenMetrics,
    );
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseHavingExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  aggregateMetricAliases: Map<string, string>,
  hiddenMetrics: Extract<RelNode, { kind: "aggregate" }>["metrics"],
): import("@tupl/foundation").RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseHavingFunctionArgsToRelExpr(
    expr.value,
    bindings,
    aliasToBinding,
    aggregateMetricAliases,
    hiddenMetrics,
  );
}

function deriveDefaultAggregateOutputName(raw: unknown): string {
  const expr = raw as { name?: unknown };
  const fn = typeof expr.name === "string" ? expr.name.toLowerCase() : "agg";
  return `${fn}_value`;
}
