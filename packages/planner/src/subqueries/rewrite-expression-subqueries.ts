import { Result, type Result as BetterResult } from "better-result";
import type { RelExpr, RelNode, TuplError } from "@tupl/foundation";

/** Expression subqueries have their own relation scope and need the full rewrite pipeline. */
export function rewriteExpressionSubqueries(
  node: RelNode,
  rewrite: (rel: RelNode, mode: "scalar" | "exists") => BetterResult<RelNode, TuplError>,
): BetterResult<RelNode, TuplError> {
  const expr = (value: RelExpr): BetterResult<RelExpr, TuplError> =>
    Result.gen(function* () {
      if (value.kind === "subquery")
        return Result.ok({ ...value, rel: yield* rewrite(value.rel, value.mode) });
      if (value.kind === "function" || value.kind === "local") {
        const args: RelExpr[] = [];
        for (const arg of value.args) args.push(yield* expr(arg));
        return Result.ok({ ...value, args });
      }
      return Result.ok(value);
    });
  const child = (value: RelNode) => rewriteExpressionSubqueries(value, rewrite);
  return Result.gen(function* () {
    if (node.kind === "project") {
      const columns: typeof node.columns = [];
      for (const column of node.columns)
        columns.push("expr" in column ? { ...column, expr: yield* expr(column.expr) } : column);
      return Result.ok({ ...node, columns, input: yield* child(node.input) });
    }
    if (node.kind === "filter")
      return Result.ok({
        ...node,
        input: yield* child(node.input),
        ...(node.expr ? { expr: yield* expr(node.expr) } : {}),
      });
    if (node.kind === "window") {
      const functions: typeof node.functions = [];
      for (const fn of node.functions) {
        if ("value" in fn)
          functions.push({
            ...fn,
            value: yield* expr(fn.value),
            ...(fn.defaultExpr ? { defaultExpr: yield* expr(fn.defaultExpr) } : {}),
          });
        else functions.push(fn);
      }
      return Result.ok({ ...node, functions, input: yield* child(node.input) });
    }
    if ("input" in node) return Result.ok({ ...node, input: yield* child(node.input) });
    if ("left" in node)
      return Result.ok({ ...node, left: yield* child(node.left), right: yield* child(node.right) });
    if (node.kind === "with") {
      const ctes: typeof node.ctes = [];
      for (const cte of node.ctes) ctes.push({ ...cte, query: yield* child(cte.query) });
      return Result.ok({ ...node, ctes, body: yield* child(node.body) });
    }
    if (node.kind === "repeat_union")
      return Result.ok({
        ...node,
        seed: yield* child(node.seed),
        iterative: yield* child(node.iterative),
      });
    return Result.ok(node);
  });
}
