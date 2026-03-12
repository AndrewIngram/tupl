import { validateSchemaConstraints } from "../constraints";
import type { SchemaDefinition } from "../types";
import { getNormalizedSchemaBindings, getNormalizedTableBinding } from "./normalized-schema-state";

/**
 * Schema finalization validation owns the invariants that make finalized logical tables line up
 * with their hidden physical/view bindings. Provider declarations on logical tables must match
 * the physical bindings assembled during schema build.
 */
export function finalizeSchemaDefinition<TSchema extends SchemaDefinition>(
  schema: TSchema,
): TSchema {
  validateNormalizedTableBindings(schema);
  validateTableProviders(schema);
  validateSchemaConstraints(schema);
  return schema;
}

function validateTableProviders(schema: SchemaDefinition): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    if (table.provider == null) {
      continue;
    }

    if (typeof table.provider !== "string" || table.provider.trim().length === 0) {
      throw new Error(
        `Table ${tableName} must define a non-empty provider binding (table.provider).`,
      );
    }
  }
}

function validateNormalizedTableBindings(schema: SchemaDefinition): void {
  const normalizedTables = getNormalizedSchemaBindings(schema);
  if (!normalizedTables) {
    throw new Error(
      "Physical tables must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).",
    );
  }

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const binding = normalizedTables[tableName];
    if (!binding) {
      throw new Error(
        `Table ${tableName} must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).`,
      );
    }

    if (binding.kind === "view") {
      continue;
    }

    if (typeof binding.entity !== "string" || binding.entity.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing an entity-backed physical binding.`);
    }

    if (typeof binding.provider !== "string" || binding.provider.trim().length === 0) {
      throw new Error(`Table ${tableName} is missing a provider-backed physical binding.`);
    }

    if (table.provider !== binding.provider) {
      throw new Error(
        `Table ${tableName} must define provider ${binding.provider} to match its entity-backed physical binding.`,
      );
    }
  }
}
