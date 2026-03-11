import type { RelNode } from "@tupl/foundation";

import { resolveColumnDefinition } from "./definition";
import { getNormalizedColumnBindings } from "./normalization";
import { inferAndMapRelOutputRows } from "./rel-output-inference";
import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  QueryRow,
  SchemaDefinition,
  SchemaValueCoercion,
  TableColumnDefinition,
  TableDefinition,
} from "./types";

/**
 * Mapping owns provider-row coercion and logical-row projection against normalized bindings.
 */
export function coerceValue(value: unknown, coerce: SchemaValueCoercion): unknown {
  if (typeof coerce === "function") {
    return coerce(value);
  }

  switch (coerce) {
    case "isoTimestamp":
      if (value == null) {
        return value;
      }
      if (value instanceof Date) {
        return value.toISOString();
      }
      if (typeof value === "string") {
        return value;
      }
      throw new Error(`Built-in coercion "${coerce}" only supports Date or string values.`);
  }
}

export function normalizeProviderRowValue(
  value: unknown,
  binding: NormalizedColumnBinding | undefined,
  fallbackDefinition?: TableColumnDefinition,
  options: {
    enforceNotNull?: boolean;
    enforceEnum?: boolean;
  } = {},
): unknown {
  if (!binding) {
    return value;
  }

  const definition = resolveColumnDefinition(binding.definition ?? fallbackDefinition ?? "text");
  const coerced = binding.coerce ? coerceValue(value, binding.coerce) : value;
  const enforceNotNull = options.enforceNotNull ?? true;
  const enforceEnum = options.enforceEnum ?? true;

  if (coerced == null) {
    if (enforceNotNull && definition.nullable === false) {
      throw new Error(
        `Column ${describeNormalizedColumnBinding(binding)} is non-nullable but provider returned null.`,
      );
    }
    return null;
  }

  switch (definition.type) {
    case "text":
      if (typeof coerced !== "string") {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a string.`);
      }
      if (enforceEnum && definition.enum && !definition.enum.includes(coerced)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be one of ${definition.enum.join(", ")}.`,
        );
      }
      return coerced;
    case "integer":
      if (typeof coerced !== "number" || !Number.isFinite(coerced) || !Number.isInteger(coerced)) {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be an integer.`);
      }
      return coerced;
    case "real":
      if (typeof coerced !== "number" || !Number.isFinite(coerced)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be a finite number.`,
        );
      }
      return coerced;
    case "blob":
      if (!(coerced instanceof Uint8Array)) {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a Uint8Array.`);
      }
      return coerced;
    case "boolean":
      if (typeof coerced !== "boolean") {
        throw new Error(`Column ${describeNormalizedColumnBinding(binding)} must be a boolean.`);
      }
      return coerced;
    case "timestamp":
    case "date":
    case "datetime":
      if (!(typeof coerced === "string" || coerced instanceof Date)) {
        throw new Error(
          `Column ${describeNormalizedColumnBinding(binding)} must be a ${definition.type} string or Date.`,
        );
      }
      return coerced instanceof Date ? coerced.toISOString() : coerced;
    case "json":
      return coerced;
  }
}

export function mapProviderRowsToLogical(
  rows: QueryRow[],
  selectedLogicalColumns: string[],
  binding: NormalizedPhysicalTableBinding | null,
  tableDefinition?: TableDefinition,
  options: {
    enforceNotNull?: boolean;
    enforceEnum?: boolean;
  } = {},
): QueryRow[] {
  if (!binding) {
    return rows;
  }

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const logical of selectedLogicalColumns) {
      const columnBinding = getNormalizedColumnBindings(binding)[logical];
      const source = isNormalizedSourceColumnBinding(columnBinding)
        ? columnBinding.source
        : logical;
      const fallbackDefinition = tableDefinition?.columns[logical];
      out[logical] = normalizeProviderRowValue(
        row[source] ?? null,
        columnBinding,
        fallbackDefinition,
        options,
      );
    }
    return out;
  });
}

export function mapProviderRowsToRelOutput(
  rows: QueryRow[],
  rel: RelNode,
  schema: SchemaDefinition,
): QueryRow[] {
  return inferAndMapRelOutputRows(rows, rel, schema, (value, outputName, definition, coerce) =>
    normalizeProviderRowValue(
      value,
      definition
        ? {
            kind: "source",
            source: outputName,
            definition,
            ...(coerce ? { coerce } : {}),
          }
        : undefined,
      definition,
    ),
  );
}

export function isNormalizedSourceColumnBinding(
  binding: NormalizedColumnBinding | undefined,
): binding is NormalizedSourceColumnBinding {
  return !!binding && binding.kind === "source";
}

function describeNormalizedColumnBinding(binding: NormalizedColumnBinding): string {
  return binding.kind === "source" ? binding.source : "<expr>";
}
