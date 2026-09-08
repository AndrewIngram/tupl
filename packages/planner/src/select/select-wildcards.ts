import { RelLoweringError } from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";
import type { SelectAst, SelectColumnAst } from "../sqlite-parser/ast";

/** Expand only projection stars, leaving aggregate arguments such as COUNT(*) intact. */
export function expandSelectWildcards(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteColumns: ReadonlyMap<string, readonly string[]>,
): SelectAst {
  const entries: SelectColumnAst[] =
    ast.columns === "*" ? [{ expr: { type: "star", value: "*" } }] : (ast.columns ?? []);
  const columns = entries.flatMap((entry): SelectColumnAst[] => {
    const expr = entry.expr;
    if (
      !("type" in expr) ||
      (expr.type !== "star" && !(expr.type === "column_ref" && expr.column === "*"))
    ) {
      return [entry];
    }
    const qualifier = expr.type === "column_ref" ? expr.table : null;
    const sources = (ast.from ?? []).filter(
      (source) => !qualifier || (source.as ?? source.table) === qualifier,
    );
    if (entry.as || sources.length === 0) {
      throw new RelLoweringError({
        operation: "expand SELECT wildcard",
        message: entry.as
          ? "Wildcard projections cannot have an alias"
          : qualifier
            ? `Unknown wildcard qualifier: ${qualifier}`
            : "SELECT * requires a source relation",
      });
    }
    return sources.flatMap((source) => {
      const table = source.table;
      const names = table
        ? (cteColumns.get(table) ??
          (schema.tables[table] ? Object.keys(schema.tables[table].columns) : undefined))
        : undefined;
      if (!names) {
        throw new RelLoweringError({
          operation: "expand SELECT wildcard",
          message: `Unknown table: ${String(table)}`,
        });
      }
      return names.map((column) => ({
        expr: { type: "column_ref", table: source.as ?? table ?? null, column },
      }));
    });
  });
  return { ...ast, columns };
}
