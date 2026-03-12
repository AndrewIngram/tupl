import { stringifyUnknownValue } from "@tupl/foundation";
import type { QueryRow, ScanFilterClause, TableScanRequest } from "@tupl/schema-model";

/**
 * Row ops own row-shape normalization, local scan filtering/sorting, and comparison helpers.
 */
export type InternalRow = Record<string, unknown>;

export function prefixRow(row: QueryRow, alias: string): InternalRow {
  const out: InternalRow = {};
  for (const [column, value] of Object.entries(row)) {
    out[`${alias}.${column}`] = value;
  }
  return out;
}

export function scanLocalRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];
  for (const clause of request.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(left[term.column], right[term.column]);
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

export function matchesClause(row: Record<string, unknown>, clause: ScanFilterClause): boolean {
  const value = readRowValue(row, clause.column);

  switch (clause.op) {
    case "eq":
      return value != null && value === clause.value;
    case "neq":
      return value != null && value !== clause.value;
    case "gt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) > 0;
    case "gte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) >= 0;
    case "lt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) < 0;
    case "lte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) <= 0;
    case "in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && set.has(value);
    }
    case "not_in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && !set.has(value);
    }
    case "like":
      return typeof value === "string" && typeof clause.value === "string"
        ? testSqlLikePattern(value, clause.value)
        : false;
    case "not_like":
      return typeof value === "string" && typeof clause.value === "string"
        ? !testSqlLikePattern(value, clause.value)
        : false;
    case "is_distinct_from":
      return value !== clause.value;
    case "is_not_distinct_from":
      return value === clause.value;
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
    default:
      return false;
  }
}

export function testSqlLikePattern(value: string, pattern: string): boolean {
  const regex = new RegExp(
    `^${pattern
      .replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
      .replace(/%/g, ".*")
      .replace(/_/g, ".")}$`,
    "su",
  );
  return regex.test(value);
}

export function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const byKey = new Map<string, QueryRow>();
  for (const row of rows) {
    byKey.set(stableRowKey(row), row);
  }
  return [...byKey.values()];
}

export function stableRowKey(row: QueryRow): string {
  const entries = Object.entries(row).sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(entries);
}

export function readRowValue(row: Record<string, unknown>, column: string): unknown {
  if (column in row) {
    return row[column];
  }

  const suffix = `.${column}`;
  const candidates = Object.entries(row).filter(([key]) => key.endsWith(suffix));
  if (candidates.length === 1) {
    return candidates[0]?.[1];
  }

  return undefined;
}

export function toColumnKey(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
}

export function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = stringifyUnknownValue(left);
  const rightString = stringifyUnknownValue(right);
  return leftString < rightString ? -1 : 1;
}

export function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    return leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
  }

  const leftString = stringifyUnknownValue(left);
  const rightString = stringifyUnknownValue(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}
