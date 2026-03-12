import type { NormalizedTableBinding, SchemaDefinition } from "../types";

/**
 * Normalized schema state owns the hidden binding map attached only to schemas built or finalized
 * through schema-model. Callers must not assume a plain SchemaDefinition has this package-owned state.
 */
const normalizedSchemaState = new WeakMap<
  SchemaDefinition,
  {
    tables: Record<string, NormalizedTableBinding>;
  }
>();

export function copyNormalizedSchemaBindings(from: SchemaDefinition, to: SchemaDefinition): void {
  const existingBindings = normalizedSchemaState.get(from);
  if (!existingBindings) {
    return;
  }

  normalizedSchemaState.set(to, {
    tables: { ...existingBindings.tables },
  });
}

export function getNormalizedTableBinding(
  schema: SchemaDefinition,
  tableName: string,
): NormalizedTableBinding | undefined {
  return normalizedSchemaState.get(schema)?.tables[tableName];
}

export function getNormalizedSchemaBindings(
  schema: SchemaDefinition,
): Record<string, NormalizedTableBinding> | undefined {
  return normalizedSchemaState.get(schema)?.tables;
}

export function setNormalizedSchemaBindings(
  schema: SchemaDefinition,
  tables: Record<string, NormalizedTableBinding>,
): void {
  normalizedSchemaState.set(schema, { tables });
}
