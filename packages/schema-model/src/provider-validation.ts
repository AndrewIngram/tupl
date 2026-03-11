import { Result } from "better-result";
import { TuplProviderBindingError } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";

import type { SchemaDefinition } from "./schema-contracts";
import { getNormalizedTableBinding } from "./schema-finalization";

/**
 * Provider validation owns provider resolution and schema/provider registration checks.
 */
export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const result = resolveTableProviderResult(schema, table);
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

export function resolveTableProviderResult(schema: SchemaDefinition, table: string) {
  const normalized = getNormalizedTableBinding(schema, table);
  if (normalized?.kind === "physical" && normalized.provider) {
    return Result.ok(normalized.provider);
  }

  if (normalized?.kind === "view") {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `View table ${table} does not have a direct provider binding.`,
      }),
    );
  }

  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Unknown table: ${table}`,
      }),
    );
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    return Result.err(
      new TuplProviderBindingError({
        table,
        message: `Table ${table} is missing required provider mapping.`,
      }),
    );
  }

  return Result.ok(tableDefinition.provider);
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  const result = validateProviderBindingsResult(schema, providers);
  if (Result.isError(result)) {
    throw result.error;
  }
}

export function validateProviderBindingsResult<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
) {
  for (const tableName of Object.keys(schema.tables)) {
    const normalized = getNormalizedTableBinding(schema, tableName);
    if (normalized?.kind === "view") {
      continue;
    }

    const providerNameResult =
      normalized?.kind === "physical" && normalized.provider
        ? Result.ok(normalized.provider)
        : resolveTableProviderResult(schema, tableName);
    if (Result.isError(providerNameResult)) {
      return providerNameResult;
    }

    const providerName = providerNameResult.value;
    if (!providers[providerName]) {
      return Result.err(
        new TuplProviderBindingError({
          table: tableName,
          provider: providerName,
          message: `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
