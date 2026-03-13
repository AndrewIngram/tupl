import { Result, type Result as BetterResult } from "better-result";
import type { TuplSchemaNormalizationError } from "@tupl/foundation";
import type { DataEntityColumnMetadata } from "@tupl/foundation";
import { getDataEntityProvider } from "@tupl/provider-kit";

import { resolveColumnDefinition } from "../definition";
import { createSchemaNormalizationError } from "../schema-errors";
import type {
  NormalizedPhysicalTableBinding,
  NormalizedSourceColumnBinding,
  SchemaDataEntityHandle,
  SchemaValueCoercion,
  TableColumnDefinition,
  TableDefinition,
} from "../types";

/**
 * Entity bindings own conversion from data-entity metadata into physical schema bindings.
 */
export function resolveEntityColumnSource(
  column: string,
  entity: SchemaDataEntityHandle<string>,
): string {
  return entity.columns?.[column]?.source ?? column;
}

export function createTableDefinitionFromEntity(
  entity: SchemaDataEntityHandle<string>,
): TableDefinition {
  const columns = entity.columns
    ? Object.fromEntries(
        Object.entries(entity.columns).map(([columnName, metadata]) => [
          columnName,
          buildEntityColumnDefinition(metadata),
        ]),
      )
    : {};

  return {
    provider: entity.provider,
    columns,
  };
}

export function createPhysicalBindingFromEntity(
  entity: SchemaDataEntityHandle<string>,
): NormalizedPhysicalTableBinding {
  const tableDefinition = createTableDefinitionFromEntity(entity);
  const providerInstance = getDataEntityProvider(entity);
  return {
    kind: "physical",
    provider: entity.provider,
    entity: entity.entity,
    columnBindings: Object.fromEntries(
      Object.entries(tableDefinition.columns).map(([columnName, definition]) => [
        columnName,
        {
          kind: "source",
          source: resolveEntityColumnSource(columnName, entity),
          definition,
        } satisfies NormalizedSourceColumnBinding,
      ]),
    ),
    columnToSource: Object.fromEntries(
      Object.keys(tableDefinition.columns).map((columnName) => [
        columnName,
        resolveEntityColumnSource(columnName, entity),
      ]),
    ),
    ...(providerInstance ? { providerInstance } : {}),
  };
}

function buildEntityColumnDefinition(
  metadata: DataEntityColumnMetadata<any>,
): TableColumnDefinition {
  const base = {
    type: metadata.type ?? "text",
    ...(metadata.nullable != null ? { nullable: metadata.nullable } : {}),
    ...(metadata.enum ? { enum: metadata.enum } : {}),
    ...(metadata.physicalType ? { physicalType: metadata.physicalType } : {}),
    ...(metadata.physicalDialect ? { physicalDialect: metadata.physicalDialect } : {}),
  };

  if (metadata.primaryKey) {
    return {
      ...base,
      primaryKey: true,
    } satisfies TableColumnDefinition;
  }

  if (metadata.unique) {
    return {
      ...base,
      unique: true,
    } satisfies TableColumnDefinition;
  }

  return base satisfies TableColumnDefinition;
}

function sourceTypeMatchesTargetType(
  sourceType: TableColumnDefinition extends infer _ ? string | undefined : never,
  targetType: string,
): boolean {
  if (!sourceType) {
    return true;
  }
  switch (targetType) {
    case "real":
      return sourceType === "real" || sourceType === "integer";
    default:
      return sourceType === targetType;
  }
}

export function assertColumnCompatibility(
  logicalColumn: string,
  definition: TableColumnDefinition,
  coerce: SchemaValueCoercion | undefined,
  entity: SchemaDataEntityHandle<string> | undefined,
): BetterResult<void, TuplSchemaNormalizationError> {
  if (!entity || coerce) {
    return Result.ok(undefined);
  }

  const sourceMetadata = entity.columns?.[logicalColumn];
  if (!sourceMetadata?.type) {
    return Result.ok(undefined);
  }

  const targetType = resolveColumnDefinition(definition).type;
  if (!sourceTypeMatchesTargetType(sourceMetadata.type, targetType)) {
    return Result.err(
      createSchemaNormalizationError({
        operation: "normalize column binding",
        message: `Column ${entity.entity}.${sourceMetadata.source} is exposed as ${sourceMetadata.type}, but the schema declared ${targetType}. Add a coerce function or align the declared type.`,
        table: entity.entity,
        column: logicalColumn,
      }),
    );
  }

  return Result.ok(undefined);
}
