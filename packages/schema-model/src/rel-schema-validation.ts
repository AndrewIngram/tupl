import type { DataEntityHandle, RelNode } from "@tupl/foundation";

import type { SchemaDefinition } from "./types";

/**
 * Rel-schema validation owns checking that relational plans only reference schema-backed tables and columns.
 */
export function validateRelAgainstSchema(node: RelNode, schema: SchemaDefinition): void {
  const validateScanColumn = (
    tableName: string,
    column: string,
    entity?: DataEntityHandle<string>,
  ): void => {
    if (entity?.columns) {
      const logicalColumn = column.includes(".")
        ? column.slice(column.lastIndexOf(".") + 1)
        : column;
      if (!(logicalColumn in entity.columns)) {
        throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
      }
      return;
    }

    const table = schema.tables[tableName];
    if (!table) {
      return;
    }
    const logicalColumn = column.includes(".") ? column.slice(column.lastIndexOf(".") + 1) : column;
    if (!(logicalColumn in table.columns)) {
      throw new Error(`Unknown column in relational plan: ${tableName}.${logicalColumn}`);
    }
  };

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan":
        if (!schema.tables[current.table] && !current.entity) {
          throw new Error(`Unknown table in relational plan: ${current.table}`);
        }
        for (const column of current.select) {
          validateScanColumn(current.table, column, current.entity);
        }
        for (const clause of current.where ?? []) {
          validateScanColumn(current.table, clause.column, current.entity);
        }
        for (const term of current.orderBy ?? []) {
          validateScanColumn(current.table, term.column, current.entity);
        }
        return;
      case "values":
      case "cte_ref":
        return;
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "correlate":
        visit(current.left);
        visit(current.right);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "repeat_union":
        visit(current.seed);
        visit(current.iterative);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
    }
  };

  visit(node);
}
