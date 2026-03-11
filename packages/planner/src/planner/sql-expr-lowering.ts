import type { RelExpr } from "@tupl/foundation";
import type { SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import type { Binding } from "./planner-types";
import { nextRelId } from "./planner-ids";
import {
  collectRelExprRefs,
  collectTablesFromSelectAst,
  isCorrelatedSubquery,
  mapBinaryOperatorToRelFunction,
  parseLimitAndOffset,
  parseLiteral,
  parseNamedWindowSpecifications,
  parsePositiveOrdinalLiteral,
  parseSubqueryAst,
  parseWindowOver,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  toRawColumnRef,
  tryParseLiteralExpressionList,
} from "./sql-expr-utils";

export interface SqlExprLoweringContext {
  schema: SchemaDefinition;
  cteNames: Set<string>;
  tryLowerSelect(ast: SelectAst): import("@tupl/foundation").RelNode | null;
}

/**
 * SQL expression lowering owns translation from parser AST fragments into RelExpr.
 * Callers provide a structured-select callback so subqueries stay planner-owned without
 * coupling this module back to the higher-level select lowering implementation.
 */
export function lowerSqlAstToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
): RelExpr | null {
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
    args?: { value?: unknown };
    ast?: unknown;
  };

  if ("ast" in expr) {
    return lowerScalarSubqueryExpr(raw, bindings, context);
  }

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
          alias: resolved.alias,
          column: resolved.column,
        },
      };
    }
    case "binary_expr":
      return lowerBinaryExprToRelExpr(expr, bindings, aliasToBinding, context);
    case "function":
      return lowerFunctionExprToRelExpr(expr, bindings, aliasToBinding, context);
    default:
      return null;
  }
}

function lowerBinaryExprToRelExpr(
  expr: {
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
    const low = lowerSqlAstToRelExpr(range.value[0], bindings, aliasToBinding, context);
    const high = lowerSqlAstToRelExpr(range.value[1], bindings, aliasToBinding, context);
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
    const values = parseExprListToRelExprArgs(expr.right, bindings, aliasToBinding, context);
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
    const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
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

  const left = lowerSqlAstToRelExpr(expr.left, bindings, aliasToBinding, context);
  const right = lowerSqlAstToRelExpr(expr.right, bindings, aliasToBinding, context);
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

function lowerFunctionExprToRelExpr(
  expr: {
    name?: unknown;
    args?: { value?: unknown };
    over?: unknown;
  },
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
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
  if (normalized === "exists") {
    const values = Array.isArray(expr.args?.value) ? expr.args?.value : [expr.args?.value];
    if (values.length !== 1) {
      return null;
    }
    return lowerExistsSubqueryExpr(values[0], bindings, context);
  }

  const args = parseFunctionArgsToRelExpr(expr.args?.value, bindings, aliasToBinding, context);
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

function parseFunctionArgsToRelExpr(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
): RelExpr[] | null {
  if (raw == null) {
    return [];
  }
  const values = Array.isArray(raw) ? raw : [raw];
  const args: RelExpr[] = [];
  for (const value of values) {
    const arg = lowerSqlAstToRelExpr(value, bindings, aliasToBinding, context);
    if (!arg) {
      return null;
    }
    args.push(arg);
  }
  return args;
}

function parseExprListToRelExprArgs(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  context: SqlExprLoweringContext,
): RelExpr[] | null {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return null;
  }
  return parseFunctionArgsToRelExpr(expr.value, bindings, aliasToBinding, context);
}

function lowerExistsSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
  if (!rel) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "exists",
    rel,
  };
}

function lowerScalarSubqueryExpr(
  raw: unknown,
  bindings: Binding[],
  context: SqlExprLoweringContext,
): RelExpr | null {
  const subquery = parseSubqueryAst(raw);
  if (!subquery) {
    return null;
  }

  const outerAliases = new Set(bindings.map((binding) => binding.alias));
  if (isCorrelatedSubquery(subquery, outerAliases)) {
    return null;
  }

  const rel = context.tryLowerSelect(subquery);
  if (!rel || rel.output.length !== 1) {
    return null;
  }

  const outputColumn = rel.output[0]?.name;
  if (!outputColumn) {
    return null;
  }

  return {
    kind: "subquery",
    id: nextRelId("subquery_expr"),
    mode: "scalar",
    rel,
    outputColumn,
  };
}

export {
  collectRelExprRefs,
  collectTablesFromSelectAst,
  isCorrelatedSubquery,
  mapBinaryOperatorToRelFunction,
  parseLimitAndOffset,
  parseLiteral,
  parseNamedWindowSpecifications,
  parsePositiveOrdinalLiteral,
  parseSubqueryAst,
  parseWindowOver,
  readWindowFunctionName,
  resolveColumnRef,
  supportsRankWindowArgs,
  toRawColumnRef,
  tryParseLiteralExpressionList,
};
