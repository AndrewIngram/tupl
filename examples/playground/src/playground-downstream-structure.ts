import type { QueryRow, SchemaDefinition, SqlScalarType } from "@tupl/schema";

import { formatCellValue } from "./data-editing";
import { DOWNSTREAM_TABLE_NAMES } from "./downstream-model";
import { isColumnNullable, readColumnType } from "./types";

export interface EditableStructureColumn {
  name: string;
  type: SqlScalarType;
  physicalType: string;
  enumValues: string[];
  nullable: boolean;
  foreignTable: string;
  foreignColumn: string;
}

export function extractRowsForEditing(
  schema: SchemaDefinition | undefined,
  rowsText: string,
  parsedRows: Record<string, QueryRow[]> | undefined,
): Record<string, QueryRow[]> {
  if (!schema) {
    return {};
  }

  if (parsedRows) {
    return parsedRows;
  }

  const fallback = Object.fromEntries(
    Object.keys(schema.tables).map((tableName) => [tableName, [] as QueryRow[]]),
  );

  try {
    const parsed = JSON.parse(rowsText) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return fallback;
    }

    for (const tableName of Object.keys(schema.tables)) {
      const tableRows = (parsed as Record<string, unknown>)[tableName];
      if (!Array.isArray(tableRows)) {
        continue;
      }

      const onlyObjects = tableRows.filter(
        (entry) => entry != null && typeof entry === "object" && !Array.isArray(entry),
      );

      fallback[tableName] = onlyObjects as QueryRow[];
    }

    return fallback;
  } catch {
    return fallback;
  }
}

export function tableIssueLines(
  issues: Array<{ path: string; message: string }>,
  tableName: string,
): string[] {
  const prefix = `${tableName}`;
  return issues
    .filter((issue) => issue.path === "$" || issue.path.startsWith(prefix))
    .map((issue) => `${issue.path}: ${issue.message}`);
}

export function uniqueNonNullValues(rows: QueryRow[], columnName: string): string[] {
  const values = new Set<string>();
  for (const row of rows) {
    const value = row[columnName];
    if (value == null) {
      continue;
    }
    values.add(formatCellValue(value));
  }
  return [...values].sort((a, b) => a.localeCompare(b));
}

export function buildEditableStructureRows(
  schema: SchemaDefinition,
): Record<string, EditableStructureColumn[]> {
  const rowsByTable: Record<string, EditableStructureColumn[]> = {};
  for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
    const foreignKeysByColumn = new Map<string, { table: string; column: string }>();
    for (const foreignKey of tableDefinition.constraints?.foreignKeys ?? []) {
      if (foreignKey.columns.length !== 1 || foreignKey.references.columns.length !== 1) {
        continue;
      }
      const columnName = foreignKey.columns[0];
      const referencedColumn = foreignKey.references.columns[0];
      if (!columnName || !referencedColumn) {
        continue;
      }
      foreignKeysByColumn.set(columnName, {
        table: foreignKey.references.table,
        column: referencedColumn,
      });
    }

    rowsByTable[tableName] = Object.entries(tableDefinition.columns).map(
      ([columnName, columnDefinition]) => ({
        name: columnName,
        type: readColumnType(columnDefinition),
        physicalType:
          typeof columnDefinition === "string"
            ? mapScalarTypeToPostgresType(columnDefinition)
            : (columnDefinition.physicalType ?? mapScalarTypeToPostgresType(columnDefinition.type)),
        enumValues: typeof columnDefinition === "string" ? [] : [...(columnDefinition.enum ?? [])],
        nullable: isColumnNullable(columnDefinition),
        foreignTable:
          typeof columnDefinition !== "string" && columnDefinition.foreignKey
            ? columnDefinition.foreignKey.table
            : (foreignKeysByColumn.get(columnName)?.table ?? ""),
        foreignColumn:
          typeof columnDefinition !== "string" && columnDefinition.foreignKey
            ? columnDefinition.foreignKey.column
            : (foreignKeysByColumn.get(columnName)?.column ?? ""),
      }),
    );
  }
  return rowsByTable;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function mapScalarTypeToPostgresType(type: SqlScalarType): string {
  switch (type) {
    case "text":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "real":
      return "DOUBLE PRECISION";
    case "blob":
      return "BYTEA";
    case "boolean":
      return "BOOLEAN";
    case "date":
      return "DATE";
    case "datetime":
    case "timestamp":
      return "TIMESTAMP";
    case "json":
      return "JSONB";
  }
}

export function buildPostgresSchemaFromRows(
  rowsByTable: Record<string, EditableStructureColumn[]>,
): SchemaDefinition {
  const tables: SchemaDefinition["tables"] = {};
  for (const tableName of DOWNSTREAM_TABLE_NAMES) {
    const rows = rowsByTable[tableName] ?? [];
    tables[tableName] = {
      provider: "dbProvider",
      columns: Object.fromEntries(
        rows.map((row) => [
          row.name,
          {
            type: row.type,
            nullable: row.nullable,
            ...(row.physicalType.trim().length > 0
              ? { physicalType: row.physicalType.trim() }
              : {}),
            ...(row.enumValues.length > 0 ? { enum: row.enumValues } : {}),
            ...(row.foreignTable.trim().length > 0 && row.foreignColumn.trim().length > 0
              ? {
                  foreignKey: {
                    table: row.foreignTable,
                    column: row.foreignColumn,
                  },
                }
              : {}),
          },
        ]),
      ),
    };
  }

  return {
    tables,
  };
}

export function buildPostgresDdlFromRows(
  rowsByTable: Record<string, EditableStructureColumn[]>,
): string {
  const enumTypeStatements: string[] = [];
  const seenEnumTypes = new Set<string>();

  for (const rows of Object.values(rowsByTable)) {
    for (const row of rows) {
      const physicalType = row.physicalType.trim();
      if (physicalType.length === 0 || row.enumValues.length === 0) {
        continue;
      }
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(physicalType)) {
        continue;
      }
      const upper = physicalType.toUpperCase();
      if (
        ["TEXT", "INTEGER", "BOOLEAN", "TIMESTAMP", "TIMESTAMPTZ", "NUMERIC", "JSONB"].includes(
          upper,
        )
      ) {
        continue;
      }
      if (seenEnumTypes.has(physicalType)) {
        continue;
      }
      seenEnumTypes.add(physicalType);
      const valuesSql = row.enumValues
        .map((value) => `'${value.replaceAll("'", "''")}'`)
        .join(", ");
      enumTypeStatements.push(
        `DO $$ BEGIN CREATE TYPE ${quoteIdentifier(physicalType)} AS ENUM (${valuesSql}); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
      );
    }
  }

  const tableStatements = DOWNSTREAM_TABLE_NAMES.map((tableName) => {
    const rows = rowsByTable[tableName] ?? [];
    if (rows.length === 0) {
      return `CREATE TABLE IF NOT EXISTS ${quoteIdentifier(tableName)} (\n);\n`;
    }

    const columnLines = rows.map(
      (row) =>
        `  ${quoteIdentifier(row.name)} ${
          row.physicalType.trim().length > 0
            ? row.physicalType.trim()
            : mapScalarTypeToPostgresType(row.type)
        }${row.nullable ? "" : " NOT NULL"}`,
    );

    if (rows.some((row) => row.name === "id")) {
      columnLines.push(`  PRIMARY KEY (${quoteIdentifier("id")})`);
    }

    for (const row of rows) {
      const referencedTable = row.foreignTable.trim();
      const referencedColumn = row.foreignColumn.trim();
      if (referencedTable.length === 0 || referencedColumn.length === 0) {
        continue;
      }

      columnLines.push(
        `  FOREIGN KEY (${quoteIdentifier(row.name)}) REFERENCES ${quoteIdentifier(referencedTable)} (${quoteIdentifier(referencedColumn)})`,
      );
    }

    return `CREATE TABLE IF NOT EXISTS ${quoteIdentifier(tableName)} (\n${columnLines.join(",\n")}\n);\n`;
  }).join("\n");

  return [...enumTypeStatements, tableStatements].join("\n\n");
}

export function buildGeneratedDbModuleCode(schema: SchemaDefinition): string {
  const tableNames = Object.keys(schema.tables);
  const tableVarByName = new Map<string, string>();

  const toCamelCase = (value: string): string => {
    const normalized = value
      .replace(/[^A-Za-z0-9]+/gu, " ")
      .trim()
      .split(/\s+/u)
      .map((part, index) => {
        const lower = part.toLowerCase();
        return index === 0 ? lower : `${lower.slice(0, 1).toUpperCase()}${lower.slice(1)}`;
      })
      .join("");
    return normalized.length > 0 ? normalized : "table";
  };

  for (const tableName of tableNames) {
    tableVarByName.set(tableName, `${toCamelCase(tableName)}Table`);
  }

  const tableDefinitions = tableNames
    .map((tableName) => {
      const table = schema.tables[tableName];
      const tableVar = tableVarByName.get(tableName) ?? `${toCamelCase(tableName)}Table`;
      const columns = Object.entries(table?.columns ?? {})
        .map(([columnName, columnDefinition]) => {
          const scalarType = readColumnType(columnDefinition);
          const baseBuilder =
            scalarType === "timestamp"
              ? `timestamp(${JSON.stringify(columnName)}, { mode: "string" })`
              : `${scalarType}(${JSON.stringify(columnName)})`;

          const modifiers: string[] = [];
          if (typeof columnDefinition !== "string" && columnDefinition.primaryKey) {
            modifiers.push("primaryKey()");
          }
          if (!isColumnNullable(columnDefinition)) {
            modifiers.push("notNull()");
          }
          if (typeof columnDefinition !== "string" && columnDefinition.foreignKey) {
            const refVar = tableVarByName.get(columnDefinition.foreignKey.table);
            if (refVar) {
              modifiers.push(
                `references(() => ${refVar}[${JSON.stringify(columnDefinition.foreignKey.column)}])`,
              );
            }
          }

          const chain = modifiers.length > 0 ? `.${modifiers.join(".")}` : "";
          return `  ${JSON.stringify(columnName)}: ${baseBuilder}${chain},`;
        })
        .join("\n");

      return `const ${tableVar} = pgTable(${JSON.stringify(tableName)}, {\n${columns}\n});`;
    })
    .join("\n\n");

  const tableEntries = tableNames
    .map((tableName) => {
      const tableVar = tableVarByName.get(tableName) ?? `${toCamelCase(tableName)}Table`;
      return `  ${JSON.stringify(tableName)}: ${tableVar},`;
    })
    .join("\n");

  return `
// Generated from the downstream Postgres model used by the playground.
// This file is read-only in the editor.
import { boolean, integer, pgTable, text, timestamp } from "drizzle-orm/pg-core";

${tableDefinitions}

export const tables = {
${tableEntries}
} as const;
`.trim();
}
