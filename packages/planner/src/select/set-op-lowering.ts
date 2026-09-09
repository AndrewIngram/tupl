import { RelLoweringError, type RelNode } from "@tupl/foundation";
import { Result } from "better-result";
import type { SelectAst } from "../sqlite-parser/ast";
import { parseLimitAndOffset } from "../sql-expr-lowering";
import { nextRelId } from "../physical/planner-ids";

export function applyCompoundModifiers(input: RelNode, ast: SelectAst, branches: SelectAst[]) {
  let node = input;
  const orderBy = [];
  for (const term of ast.orderby ?? []) {
    const expr = term.expr;
    let index = "type" in expr && expr.type === "number" ? expr.value - 1 : -1;
    if ("type" in expr && expr.type === "column_ref") {
      // Resolve in each branch's SQL scope before applying enclosing CTE names.
      for (const branch of branches) {
        if (!Array.isArray(branch.columns)) continue;
        index = !expr.table ? branch.columns.findIndex((entry) => entry.as === expr.column) : -1;
        if (index < 0)
          index = branch.columns.findIndex((entry) => {
            const source = entry.expr;
            if (
              !("type" in source) ||
              source.type !== "column_ref" ||
              source.column !== expr.column
            )
              return false;
            if (!expr.table) return true;
            const qualifier =
              source.table ??
              (branch.from?.length === 1
                ? (branch.from[0]?.as ?? branch.from[0]?.table)
                : undefined);
            return qualifier === expr.table;
          });
        if (index >= 0) break;
      }
    }
    const output = Number.isInteger(index) && index >= 0 ? input.output[index] : undefined;
    if (!output)
      return Result.err(
        new RelLoweringError({
          operation: "lower compound ordering",
          message: "Compound ORDER BY must reference an output column or a valid output ordinal.",
        }),
      );
    orderBy.push({
      source: { column: output.name },
      direction: term.type === "DESC" ? ("desc" as const) : ("asc" as const),
    });
  }
  if (orderBy.length)
    node = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: node,
      orderBy,
      output: node.output,
    };
  const { limit, offset } = parseLimitAndOffset(ast.limit);
  if (limit != null || offset != null)
    node = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: node,
      ...(limit != null ? { limit } : {}),
      ...(offset != null ? { offset } : {}),
      output: node.output,
    };
  return Result.ok(node);
}

/**
 * Set-op lowering owns parser-level normalization of SQL set operators.
 */
export function parseSetOp(raw: string): Extract<RelNode, { kind: "set_op" }>["op"] | null {
  const normalized = raw.trim().toUpperCase();
  switch (normalized) {
    case "UNION ALL":
      return "union_all";
    case "UNION":
      return "union";
    case "INTERSECT":
      return "intersect";
    case "EXCEPT":
      return "except";
    default:
      return null;
  }
}
