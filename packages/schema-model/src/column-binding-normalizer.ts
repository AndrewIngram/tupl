import {
  isColumnLensDefinition,
  isSchemaCalculatedColumnDefinition,
  isSchemaColRefToken,
  isSchemaTypedColumnDefinition,
} from "./dsl-tokens";
import { assertColumnCompatibility, resolveEntityColumnSource } from "./entity-bindings";
import { resolveColumnExpr } from "./normalized-column-expr";
import type {
  NormalizedColumnBinding,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
  TableColumnDefinition,
} from "./types";
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
): {
  definition: TableColumnDefinition;
  binding: NormalizedColumnBinding;
} {
  if (isSchemaCalculatedColumnDefinition(rawColumn)) {
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "expr",
        expr: resolveColumnExpr(
          rawColumn.expr,
          options.resolveTableToken,
          options.resolveEntityToken,
        ),
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaTypedColumnDefinition(rawColumn)) {
    const source = options.entity
      ? resolveEntityColumnSource(rawColumn.sourceColumn, options.entity)
      : rawColumn.sourceColumn;
    assertColumnCompatibility(
      rawColumn.sourceColumn,
      rawColumn.definition,
      rawColumn.coerce,
      options.entity,
    );
    return {
      definition: rawColumn.definition,
      binding: {
        kind: "source",
        source,
        definition: rawColumn.definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (isSchemaColRefToken(rawColumn)) {
    const ref = resolveColRefToken(
      rawColumn,
      options.resolveTableToken,
      options.resolveEntityToken,
    );
    return {
      definition: "text",
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? ref : parseColumnSource(ref),
        definition: "text",
      },
    };
  }

  if (isColumnLensDefinition(rawColumn)) {
    const sourceRef = isSchemaColRefToken(rawColumn.source)
      ? resolveColRefToken(rawColumn.source, options.resolveTableToken, options.resolveEntityToken)
      : rawColumn.source;
    const enumFromRef = rawColumn.enumFrom
      ? resolveEnumRef(rawColumn.enumFrom, options.resolveTableToken, options.resolveEntityToken)
      : undefined;

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

    return {
      definition,
      binding: {
        kind: "source",
        source: options.preserveQualifiedRef ? sourceRef : parseColumnSource(sourceRef),
        definition,
        ...(rawColumn.coerce ? { coerce: rawColumn.coerce } : {}),
      },
    };
  }

  if (typeof rawColumn !== "string") {
    const definitionInput = rawColumn as Exclude<TableColumnDefinition, string> & {
      enumFrom?: SchemaColRefToken | string;
    };
    const enumFromRef = definitionInput.enumFrom
      ? resolveEnumRef(
          definitionInput.enumFrom,
          options.resolveTableToken,
          options.resolveEntityToken,
        )
      : undefined;
    const definition = {
      ...definitionInput,
      ...(enumFromRef ? { enumFrom: enumFromRef } : {}),
    } satisfies TableColumnDefinition;
    return {
      definition,
      binding: {
        kind: "source",
        source: columnName,
        definition,
      },
    };
  }

  return {
    definition: rawColumn as TableColumnDefinition,
    binding: {
      kind: "source",
      source: columnName,
      definition: rawColumn as TableColumnDefinition,
    },
  };
}
