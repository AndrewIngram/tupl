import { Result, type Result as BetterResult } from "better-result";
import type { TuplSchemaIssue, TuplSchemaValidationError } from "@tupl/foundation";

import {
  getTable,
  resolveColumnDefinition,
  resolveTableColumnDefinition,
  resolveTableForeignKeys,
  resolveTablePrimaryKeyConstraint,
  resolveTableUniqueConstraints,
  type ResolvedColumnDefinition,
} from "./definition";
import { createSchemaIssue, createSchemaValidationError } from "./schema-errors";
import type { SchemaDefinition } from "./types";

/**
 * Schema constraint validation owns logical schema invariants for tables, columns, and constraints.
 */
export function validateSchemaConstraints(
  schema: SchemaDefinition,
): BetterResult<void, TuplSchemaValidationError> {
  const issues = collectSchemaConstraintIssues(schema);
  if (issues.length > 0) {
    return Result.err(createSchemaValidationError(issues));
  }

  return Result.ok(undefined);
}

function collectSchemaConstraintIssues(schema: SchemaDefinition) {
  const issues: TuplSchemaIssue[] = [];

  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
      const resolved = resolveColumnDefinition(columnDefinition);
      validateColumnDefinition(tableName, columnName, resolved, issues);
    }

    const columnPrimaryKeyColumns = readColumnPrimaryKeyColumns(table);
    if (columnPrimaryKeyColumns.length > 1) {
      issues.push(
        createSchemaIssue({
          code: "primary_key.multiple_column_level",
          message: `Invalid primary key on ${tableName}: multiple column-level primaryKey declarations found (${columnPrimaryKeyColumns.join(", ")}). Use table.constraints.primaryKey for composite keys.`,
          table: tableName,
          constraint: "primary key",
          path: ["tables", tableName, "constraints", "primaryKey"],
        }),
      );
    }

    const tablePrimaryKey = table.constraints?.primaryKey;
    if (tablePrimaryKey && columnPrimaryKeyColumns.length === 1) {
      const columnPrimaryKeyColumn = columnPrimaryKeyColumns[0];
      if (!columnPrimaryKeyColumn) {
        continue;
      }
      const tablePrimaryKeyIsSameSingleColumn =
        tablePrimaryKey.columns.length === 1 &&
        tablePrimaryKey.columns[0] === columnPrimaryKeyColumn;
      if (!tablePrimaryKeyIsSameSingleColumn) {
        issues.push(
          createSchemaIssue({
            code: "primary_key.conflict",
            message: `Invalid primary key on ${tableName}: column-level primaryKey on "${columnPrimaryKeyColumn}" conflicts with table.constraints.primaryKey. Use one declaration style.`,
            table: tableName,
            column: columnPrimaryKeyColumn,
            constraint: "primary key",
            path: ["tables", tableName, "constraints", "primaryKey"],
          }),
        );
      }
    }

    const resolvedPrimaryKey = resolveTablePrimaryKeyConstraint(table);
    if (resolvedPrimaryKey) {
      validateConstraintColumns(
        schema,
        tableName,
        "primary key",
        resolvedPrimaryKey.columns,
        issues,
      );
      validateNoDuplicateColumns(tableName, "primary key", resolvedPrimaryKey.columns, issues);
    }

    resolveTableUniqueConstraints(table).forEach((uniqueConstraint, index) => {
      const label = uniqueConstraint.name ?? `unique constraint #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, uniqueConstraint.columns, issues);
      validateNoDuplicateColumns(tableName, label, uniqueConstraint.columns, issues);
    });

    const foreignKeys = resolveTableForeignKeys(table);
    foreignKeys.forEach((foreignKey, index) => {
      const label = foreignKey.name ?? `foreign key #${index + 1}`;
      validateConstraintColumns(schema, tableName, label, foreignKey.columns, issues);
      validateNoDuplicateColumns(tableName, label, foreignKey.columns, issues);

      const referencedTableName = foreignKey.references.table;
      const referencedTable = schema.tables[referencedTableName];
      if (!referencedTable) {
        issues.push(
          createSchemaIssue({
            code: "foreign_key.unknown_table",
            message: `Invalid ${label} on ${tableName}: referenced table "${referencedTableName}" does not exist.`,
            table: tableName,
            constraint: label,
            path: ["tables", tableName, "constraints", "foreignKeys"],
          }),
        );
      }

      if (foreignKey.columns.length !== foreignKey.references.columns.length) {
        issues.push(
          createSchemaIssue({
            code: "foreign_key.arity_mismatch",
            message: `Invalid ${label} on ${tableName}: local columns (${foreignKey.columns.length}) and referenced columns (${foreignKey.references.columns.length}) must have the same length.`,
            table: tableName,
            constraint: label,
            path: ["tables", tableName, "constraints", "foreignKeys"],
          }),
        );
      }

      if (foreignKey.references.columns.length === 0) {
        issues.push(
          createSchemaIssue({
            code: "foreign_key.empty_references",
            message: `Invalid ${label} on ${tableName}: referenced columns cannot be empty.`,
            table: tableName,
            constraint: label,
            path: ["tables", tableName, "constraints", "foreignKeys"],
          }),
        );
      }

      if (referencedTable) {
        for (const referencedColumn of foreignKey.references.columns) {
          if (!(referencedColumn in referencedTable.columns)) {
            issues.push(
              createSchemaIssue({
                code: "foreign_key.unknown_column",
                message: `Invalid ${label} on ${tableName}: referenced column "${referencedColumn}" does not exist on table "${referencedTableName}".`,
                table: tableName,
                column: referencedColumn,
                constraint: label,
                path: ["tables", tableName, "constraints", "foreignKeys", "references", "columns"],
              }),
            );
          }
        }
      }

      validateNoDuplicateColumns(
        `${tableName} -> ${referencedTableName}`,
        `${label} referenced columns`,
        foreignKey.references.columns,
        issues,
      );
    });

    table.constraints?.checks?.forEach((checkConstraint, index) => {
      const label = checkConstraint.name ?? `check constraint #${index + 1}`;
      if (checkConstraint.kind !== "in") {
        return;
      }

      const hasValidColumn = validateConstraintColumns(
        schema,
        tableName,
        label,
        [checkConstraint.column],
        issues,
      );
      if (checkConstraint.values.length === 0) {
        issues.push(
          createSchemaIssue({
            code: "check.empty_values",
            message: `Invalid ${label} on ${tableName}: values cannot be empty.`,
            table: tableName,
            column: checkConstraint.column,
            constraint: label,
            path: ["tables", tableName, "constraints", "checks"],
          }),
        );
      }

      if (!hasValidColumn) {
        return;
      }

      const columnType = resolveTableColumnDefinition(
        schema,
        tableName,
        checkConstraint.column,
      ).type;
      const valueTypes = new Set(
        checkConstraint.values
          .filter((value): value is string | number | boolean => value != null)
          .map((value) => typeof value),
      );
      for (const valueType of valueTypes) {
        if (!columnTypeAllowsValueType(columnType, valueType)) {
          issues.push(
            createSchemaIssue({
              code: "check.value_type_mismatch",
              message: `Invalid ${label} on ${tableName}: value type ${valueType} does not match column type ${columnType}.`,
              table: tableName,
              column: checkConstraint.column,
              constraint: label,
              path: ["tables", tableName, "constraints", "checks"],
            }),
          );
        }
      }
    });
  }

  return issues;
}

function readColumnPrimaryKeyColumns(table: SchemaDefinition["tables"][string]) {
  const primaryKeyColumns: string[] = [];

  for (const [columnName, columnDefinition] of Object.entries(table.columns)) {
    if (typeof columnDefinition === "string" || columnDefinition.primaryKey !== true) {
      continue;
    }
    primaryKeyColumns.push(columnName);
  }

  return primaryKeyColumns;
}

function validateColumnDefinition(
  tableName: string,
  columnName: string,
  definition: ResolvedColumnDefinition,
  issues: TuplSchemaIssue[],
) {
  if (definition.primaryKey && definition.unique) {
    issues.push(
      createSchemaIssue({
        code: "column.primary_key_unique_conflict",
        message: `Invalid column ${tableName}.${columnName}: primaryKey and unique cannot both be true.`,
        table: tableName,
        column: columnName,
        path: ["tables", tableName, "columns", columnName],
      }),
    );
  }

  if (definition.primaryKey && definition.nullable) {
    issues.push(
      createSchemaIssue({
        code: "column.primary_key_nullable",
        message: `Invalid column ${tableName}.${columnName}: primaryKey columns must be nullable: false.`,
        table: tableName,
        column: columnName,
        path: ["tables", tableName, "columns", columnName],
      }),
    );
  }

  if (definition.enum && definition.type !== "text") {
    issues.push(
      createSchemaIssue({
        code: "column.enum_non_text",
        message: `Invalid column ${tableName}.${columnName}: enum is only supported on text columns.`,
        table: tableName,
        column: columnName,
        path: ["tables", tableName, "columns", columnName],
      }),
    );
  }

  if (definition.enumFrom && definition.type !== "text") {
    issues.push(
      createSchemaIssue({
        code: "column.enum_from_non_text",
        message: `Invalid column ${tableName}.${columnName}: enumFrom is only supported on text columns.`,
        table: tableName,
        column: columnName,
        path: ["tables", tableName, "columns", columnName],
      }),
    );
  }

  if (definition.enumFrom && definition.enumFrom.trim().length === 0) {
    issues.push(
      createSchemaIssue({
        code: "column.enum_from_empty",
        message: `Invalid column ${tableName}.${columnName}: enumFrom cannot be empty.`,
        table: tableName,
        column: columnName,
        path: ["tables", tableName, "columns", columnName, "enumFrom"],
      }),
    );
  }

  if (definition.enum) {
    if (definition.enum.length === 0) {
      issues.push(
        createSchemaIssue({
          code: "column.enum_empty",
          message: `Invalid column ${tableName}.${columnName}: enum cannot be empty.`,
          table: tableName,
          column: columnName,
          path: ["tables", tableName, "columns", columnName, "enum"],
        }),
      );
    }

    const unique = new Set(definition.enum);
    if (unique.size !== definition.enum.length) {
      issues.push(
        createSchemaIssue({
          code: "column.enum_duplicates",
          message: `Invalid column ${tableName}.${columnName}: enum contains duplicate values.`,
          table: tableName,
          column: columnName,
          path: ["tables", tableName, "columns", columnName, "enum"],
        }),
      );
    }
  }

  if (definition.enumMap) {
    if (!definition.enumFrom) {
      issues.push(
        createSchemaIssue({
          code: "column.enum_map_requires_enum_from",
          message: `Invalid column ${tableName}.${columnName}: enumMap requires enumFrom.`,
          table: tableName,
          column: columnName,
          path: ["tables", tableName, "columns", columnName, "enumMap"],
        }),
      );
    }

    for (const [sourceValue, mappedValue] of Object.entries(definition.enumMap)) {
      if (sourceValue.length === 0) {
        issues.push(
          createSchemaIssue({
            code: "column.enum_map_empty_source",
            message: `Invalid column ${tableName}.${columnName}: enumMap contains an empty source key.`,
            table: tableName,
            column: columnName,
            path: ["tables", tableName, "columns", columnName, "enumMap"],
          }),
        );
      }
      if (mappedValue.length === 0) {
        issues.push(
          createSchemaIssue({
            code: "column.enum_map_empty_target",
            message: `Invalid column ${tableName}.${columnName}: enumMap contains an empty mapped value.`,
            table: tableName,
            column: columnName,
            path: ["tables", tableName, "columns", columnName, "enumMap"],
          }),
        );
      }
      if (definition.enum && !definition.enum.includes(mappedValue)) {
        issues.push(
          createSchemaIssue({
            code: "column.enum_map_unknown_target",
            message: `Invalid column ${tableName}.${columnName}: enumMap value "${mappedValue}" is not listed in enum.`,
            table: tableName,
            column: columnName,
            path: ["tables", tableName, "columns", columnName, "enumMap"],
          }),
        );
      }
    }
  }

  if (definition.foreignKey) {
    if (definition.foreignKey.table.trim().length === 0) {
      issues.push(
        createSchemaIssue({
          code: "column.foreign_key_table_empty",
          message: `Invalid column ${tableName}.${columnName}: foreignKey.table cannot be empty.`,
          table: tableName,
          column: columnName,
          path: ["tables", tableName, "columns", columnName, "foreignKey", "table"],
        }),
      );
    }
    if (definition.foreignKey.column.trim().length === 0) {
      issues.push(
        createSchemaIssue({
          code: "column.foreign_key_column_empty",
          message: `Invalid column ${tableName}.${columnName}: foreignKey.column cannot be empty.`,
          table: tableName,
          column: columnName,
          path: ["tables", tableName, "columns", columnName, "foreignKey", "column"],
        }),
      );
    }
  }
}

function validateConstraintColumns(
  schema: SchemaDefinition,
  tableName: string,
  label: string,
  columns: string[],
  issues: TuplSchemaIssue[],
) {
  let isValid = true;
  if (columns.length === 0) {
    issues.push(
      createSchemaIssue({
        code: "constraint.columns_empty",
        message: `Invalid ${label} on ${tableName}: columns cannot be empty.`,
        table: tableName,
        constraint: label,
        path: ["tables", tableName, "constraints"],
      }),
    );
    return false;
  }

  const table = getTable(schema, tableName);
  for (const column of columns) {
    if (!(column in table.columns)) {
      isValid = false;
      issues.push(
        createSchemaIssue({
          code: "constraint.unknown_column",
          message: `Invalid ${label} on ${tableName}: column "${column}" does not exist on table "${tableName}".`,
          table: tableName,
          column,
          constraint: label,
          path: ["tables", tableName, "constraints"],
        }),
      );
    }
  }

  return isValid;
}

function validateNoDuplicateColumns(
  tableName: string,
  label: string,
  columns: string[],
  issues: TuplSchemaIssue[],
) {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const column of columns) {
    if (seen.has(column)) {
      if (!duplicates.has(column)) {
        issues.push(
          createSchemaIssue({
            code: "constraint.duplicate_column",
            message: `Invalid ${label} on ${tableName}: duplicate column "${column}" in constraint definition.`,
            table: tableName,
            column,
            constraint: label,
            path: ["tables", tableName, "constraints"],
          }),
        );
        duplicates.add(column);
      }
      continue;
    }
    seen.add(column);
  }
}

function columnTypeAllowsValueType(
  columnType: ResolvedColumnDefinition["type"],
  valueType: string,
) {
  switch (columnType) {
    case "text":
    case "timestamp":
    case "date":
    case "datetime":
    case "json":
      return valueType === "string";
    case "integer":
    case "real":
      return valueType === "number";
    case "boolean":
      return valueType === "boolean";
    case "blob":
      return false;
  }
}
