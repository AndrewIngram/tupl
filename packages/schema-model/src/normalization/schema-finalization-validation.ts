import { Result, type Result as BetterResult } from "better-result";
import type {
  TuplSchemaNormalizationError,
  TuplSchemaValidationError,
  TuplResult,
} from "@tupl/foundation";

import { validateSchemaConstraints } from "../constraints";
import { createSchemaNormalizationError } from "../schema-errors";
import type { SchemaDefinition } from "../types";
import { getNormalizedSchemaBindings, getNormalizedTableBinding } from "./normalized-schema-state";

/**
 * Schema finalization validation owns the invariants that make finalized logical tables line up
 * with their hidden physical/view bindings. Provider declarations on logical tables must match
 * the physical bindings assembled during schema build.
 */
export function finalizeSchemaDefinition<TSchema extends SchemaDefinition>(
  schema: TSchema,
): TuplResult<TSchema> {
  return Result.gen(function* () {
    yield* validateNormalizedTableBindings(schema);
    yield* validateTableProviders(schema);
    yield* validateSchemaConstraints(schema);
    return Result.ok(schema);
  });
}

function validateTableProviders(
  schema: SchemaDefinition,
): BetterResult<void, TuplSchemaNormalizationError> {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    if (table.provider == null) {
      continue;
    }

    if (typeof table.provider !== "string" || table.provider.trim().length === 0) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "finalize schema definition",
          message: `Table ${tableName} must define a non-empty provider binding (table.provider).`,
          table: tableName,
        }),
      );
    }
  }

  return Result.ok(undefined);
}

function validateNormalizedTableBindings(
  schema: SchemaDefinition,
): BetterResult<void, TuplSchemaNormalizationError | TuplSchemaValidationError> {
  const normalizedTables = getNormalizedSchemaBindings(schema);
  if (!normalizedTables) {
    return Result.err(
      createSchemaNormalizationError({
        operation: "finalize schema definition",
        message:
          "Physical tables must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).",
      }),
    );
  }

  for (const [tableName, table] of Object.entries(schema.tables)) {
    const binding = normalizedTables[tableName];
    if (!binding) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "finalize schema definition",
          message: `Table ${tableName} must be declared via createSchemaBuilder().table(name, provider.entities.someTable, config).`,
          table: tableName,
        }),
      );
    }

    if (binding.kind === "view") {
      continue;
    }

    if (typeof binding.entity !== "string" || binding.entity.trim().length === 0) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "finalize schema definition",
          message: `Table ${tableName} is missing an entity-backed physical binding.`,
          table: tableName,
        }),
      );
    }

    if (typeof binding.provider !== "string" || binding.provider.trim().length === 0) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "finalize schema definition",
          message: `Table ${tableName} is missing a provider-backed physical binding.`,
          table: tableName,
        }),
      );
    }

    if (table.provider !== binding.provider) {
      return Result.err(
        createSchemaNormalizationError({
          operation: "finalize schema definition",
          message: `Table ${tableName} must define provider ${binding.provider} to match its entity-backed physical binding.`,
          table: tableName,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
