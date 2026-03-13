import { Result, type Result as BetterResult } from "better-result";
import type { TuplSchemaNormalizationError } from "@tupl/foundation";

import {
  isColumnLensDefinition,
  isSchemaCalculatedColumnDefinition,
  isSchemaColRefToken,
  isSchemaTypedColumnDefinition,
} from "../dsl/dsl-tokens";
import { assertColumnCompatibility, resolveEntityColumnSource } from "./entity-bindings";
import { resolveColumnExpr } from "./normalized-column-expr";
import type {
  NormalizedColumnBinding,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
  TableColumnDefinition,
} from "../types";
import { parseColumnSource, resolveColRefToken, resolveEnumRef } from "./view-normalization";

/**
 * Column binding normalizer owns turning raw DSL column inputs into normalized bindings plus definitions.
 */
export function normalizeColumnBinding(
  columnName: string,
  rawColumn: unknown,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    entity?: SchemaDataEntityHandle<string>;
  },
): BetterResult<
  {
    definition: TableColumnDefinition;
    binding: NormalizedColumnBinding;
  },
  TuplSchemaNormalizationError
> {
  if (isSchemaCalculatedColumnDefinition(rawColumn)) {
    return Result.ok({
      definition: rawColumn.definition,
      binding: {
        kind: "expr" as const,
        expr: resolveColumnExpr(
          rawColumn.expr,
          options.resolveTableToken,
          options.resolveEntityToken,
        ),
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    });
  }

  if (isSchemaTypedColumnDefinition(rawColumn)) {
    const source = options.entity
      ? resolveEntityColumnSource(rawColumn.sourceColumn, options.entity)
      : rawColumn.sourceColumn;
    const compatibilityResult = assertColumnCompatibility(
      rawColumn.sourceColumn,
      rawColumn.definition,
      rawColumn.coerce,
      options.entity,
    );
    if (Result.isError(compatibilityResult)) {
      return compatibilityResult;
    }

    return Result.ok({
      definition: rawColumn.definition,
      binding: {
        kind: "source" as const,
        source,
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    });
  }

  if (isSchemaColRefToken(rawColumn)) {
    const refResult = resolveColRefToken(
      rawColumn,
      options.resolveTableToken,
      options.resolveEntityToken,
    );
    if (Result.isError(refResult)) {
      return refResult;
    }

    const ref = refResult.value;
    return Result.ok({
      definition: "text",
      binding: {
        kind: "source" as const,
        source: options.preserveQualifiedRef ? ref : parseColumnSource(ref),
        definition: "text",
      },
    });
  }

  if (isColumnLensDefinition(rawColumn)) {
    const sourceRefResult = isSchemaColRefToken(rawColumn.source)
      ? resolveColRefToken(rawColumn.source, options.resolveTableToken, options.resolveEntityToken)
      : Result.ok(rawColumn.source);
    if (Result.isError(sourceRefResult)) {
      return sourceRefResult;
    }
    const enumFromRefResult = rawColumn.enumFrom
      ? resolveEnumRef(rawColumn.enumFrom, options.resolveTableToken, options.resolveEntityToken)
      : Result.ok<string | undefined, TuplSchemaNormalizationError>(undefined);
    if (Result.isError(enumFromRefResult)) {
      return enumFromRefResult;
    }

    const sourceRef = sourceRefResult.value;
    const enumFromRef = enumFromRefResult.value;
    const definition = {
      type: rawColumn.type ?? "text",
      ...(rawColumn.nullable != null ? { nullable: rawColumn.nullable } : {}),
      ...(rawColumn.primaryKey === true
        ? { primaryKey: true as const }
        : rawColumn.primaryKey === false
          ? { primaryKey: false as const }
          : {}),
      ...(rawColumn.unique === true
        ? { unique: true as const }
        : rawColumn.unique === false
          ? { unique: false as const }
          : {}),
      ...(rawColumn.enum ? { enum: rawColumn.enum } : {}),
      ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
      ...(rawColumn.enumMap ? { enumMap: rawColumn.enumMap } : {}),
      ...(rawColumn.physicalType ? { physicalType: rawColumn.physicalType } : {}),
      ...(rawColumn.physicalDialect ? { physicalDialect: rawColumn.physicalDialect } : {}),
      ...(rawColumn.foreignKey ? { foreignKey: rawColumn.foreignKey } : {}),
      ...(rawColumn.description ? { description: rawColumn.description } : {}),
    } as TableColumnDefinition;

    return Result.ok({
      definition,
      binding: {
        kind: "source" as const,
        source: options.preserveQualifiedRef ? sourceRef : parseColumnSource(sourceRef),
        definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    });
  }

  if (typeof rawColumn !== "string") {
    const definitionInput = rawColumn as Exclude<TableColumnDefinition, string> & {
      enumFrom?: SchemaColRefToken | string;
    };
    const enumFromRefResult = definitionInput.enumFrom
      ? resolveEnumRef(
          definitionInput.enumFrom,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : Result.ok<string | undefined, TuplSchemaNormalizationError>(undefined);
    if (Result.isError(enumFromRefResult)) {
      return enumFromRefResult;
    }

    const definition = {
      ...definitionInput,
      ...(enumFromRefResult.value ? { enumFrom: enumFromRefResult.value } : {}),
    } satisfies TableColumnDefinition;
    return Result.ok({
      definition,
      binding: {
        kind: "source" as const,
        source: columnName,
        definition,
      },
    });
  }

  return Result.ok({
    definition: rawColumn as TableColumnDefinition,
    binding: {
      kind: "source" as const,
      source: columnName,
      definition: rawColumn as TableColumnDefinition,
    },
  });
}
