import { Result, type Result as BetterResult } from "better-result";
import type { TuplResult, TuplSchemaNormalizationError } from "@tupl/foundation";
import { getDataEntityProvider } from "@tupl/provider-kit";

import { type SchemaBuilderState } from "../dsl/builder-state";
import {
  buildSchemaDslViewRelHelpers,
  isDslTableDefinition,
  isDslViewDefinition,
} from "../dsl/builder-helpers";
import { createSchemaNormalizationError } from "../schema-errors";
import type {
  NormalizedColumnBinding,
  NormalizedTableBinding,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaDslTableToken,
  SchemaDslViewRelHelpers,
  TableColumns,
  TableDefinition,
} from "../types";
import {
  buildColumnSourceMapFromBindings,
  normalizeColumnBinding,
  validateCalculatedColumnDependencies,
} from "./binding-normalization";
import { setNormalizedSchemaBindings } from "./normalized-schema-state";
import { finalizeSchemaDefinition } from "./schema-finalization-validation";
import { resolveViewRelDefinition } from "./view-normalization";

/**
 * Registered schema building owns the translation from registered DSL tables/views into logical
 * tables plus hidden normalized bindings. Physical and view bindings are assembled differently on
 * purpose, and the normalized bindings become the source of truth after build.
 */
export function buildRegisteredSchemaDefinition<TContext>(
  state: SchemaBuilderState<TContext>,
): TuplResult<SchemaDefinition> {
  const tables: Record<string, TableDefinition> = {};
  const bindings: Record<string, NormalizedTableBinding> = {};
  const tableTokenToName = buildTableTokenMap(state);
  const resolveTableToken = createTableTokenResolver(tableTokenToName);
  const resolveEntityToken = createEntityTokenResolver();
  const viewRelHelpers = buildSchemaDslViewRelHelpers() as SchemaDslViewRelHelpers;

  for (const [tableName, rawTable] of state.definitions.entries()) {
    if (isDslTableDefinition(rawTable)) {
      const builtResult = buildPhysicalTableDefinition(tableName, rawTable, {
        resolveTableToken,
        resolveEntityToken,
      });
      if (Result.isError(builtResult)) {
        return builtResult;
      }
      const { definition, binding } = builtResult.value;
      tables[tableName] = definition;
      bindings[tableName] = binding;
      continue;
    }

    if (isDslViewDefinition(rawTable)) {
      const builtResult = buildViewTableDefinition(tableName, rawTable, {
        resolveTableToken,
        resolveEntityToken,
        viewRelHelpers,
      });
      if (Result.isError(builtResult)) {
        return builtResult;
      }
      const { definition, binding } = builtResult.value;
      tables[tableName] = definition;
      bindings[tableName] = binding;
      continue;
    }

    tables[tableName] = rawTable as never;
  }

  const schema: SchemaDefinition = { tables };
  setNormalizedSchemaBindings(schema, bindings);
  return finalizeSchemaDefinition(schema);
}

function buildTableTokenMap<TContext>(state: SchemaBuilderState<TContext>) {
  const tableTokenToName = new Map<symbol, string>();

  for (const [tableName, rawTable] of state.definitions.entries()) {
    if (isDslTableDefinition(rawTable) || isDslViewDefinition(rawTable)) {
      tableTokenToName.set(rawTable.tableToken.__id, tableName);
    }
  }

  return tableTokenToName;
}

function createTableTokenResolver(tableTokenToName: Map<symbol, string>) {
  return (token: SchemaDslTableToken<string>) => {
    const tableName = tableTokenToName.get(token.__id);
    if (!tableName) {
      throw createSchemaNormalizationError({
        operation: "resolve schema table token",
        message: "Schema DSL table token could not be resolved to a table name.",
      });
    }
    return tableName;
  };
}

function createEntityTokenResolver() {
  return (entity: SchemaDataEntityHandle<string>) => {
    if (!entity.entity || entity.entity.length === 0) {
      throw createSchemaNormalizationError({
        operation: "resolve schema data entity",
        message: "Schema DSL data entity handle is missing entity name.",
      });
    }
    return entity.entity;
  };
}

function buildPhysicalTableDefinition<TContext>(
  tableName: string,
  rawTable: Extract<
    SchemaBuilderState<TContext>["definitions"] extends Map<any, infer T> ? T : never,
    { kind: "dsl_table" }
  >,
  resolvers: {
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
  },
): BetterResult<
  { definition: TableDefinition; binding: NormalizedTableBinding },
  TuplSchemaNormalizationError
> {
  const normalizedColumnsResult = normalizeTableColumns(rawTable.columns, {
    preserveQualifiedRef: false,
    ...resolvers,
    entity: rawTable.from,
  });
  if (Result.isError(normalizedColumnsResult)) {
    return normalizedColumnsResult;
  }

  const { normalizedColumns, columnBindings } = normalizedColumnsResult.value;
  const dependencyResult = validateCalculatedColumnDependencies(tableName, columnBindings);
  if (Result.isError(dependencyResult)) {
    return dependencyResult;
  }

  const definition: TableDefinition = {
    provider: rawTable.from.provider,
    columns: normalizedColumns,
    ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
  };
  const providerInstance = getDataEntityProvider(rawTable.from);

  return Result.ok({
    definition,
    binding: {
      kind: "physical" as const,
      provider: rawTable.from.provider,
      entity: rawTable.from.entity,
      columnBindings,
      columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      ...(providerInstance ? { providerInstance } : {}),
    },
  });
}

function buildViewTableDefinition<TContext>(
  tableName: string,
  rawTable: Extract<
    SchemaBuilderState<TContext>["definitions"] extends Map<any, infer T> ? T : never,
    { kind: "dsl_view" }
  >,
  input: {
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    viewRelHelpers: SchemaDslViewRelHelpers;
  },
): BetterResult<
  { definition: TableDefinition; binding: NormalizedTableBinding },
  TuplSchemaNormalizationError
> {
  const normalizedColumnsResult = normalizeTableColumns(rawTable.columns, {
    preserveQualifiedRef: true,
    resolveTableToken: input.resolveTableToken,
    resolveEntityToken: input.resolveEntityToken,
  });
  if (Result.isError(normalizedColumnsResult)) {
    return normalizedColumnsResult;
  }

  const { normalizedColumns, columnBindings } = normalizedColumnsResult.value;
  const dependencyResult = validateCalculatedColumnDependencies(tableName, columnBindings);
  if (Result.isError(dependencyResult)) {
    return dependencyResult;
  }

  return Result.ok({
    definition: {
      provider: "__view__",
      columns: normalizedColumns,
      ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
    },
    binding: {
      kind: "view" as const,
      rel: (context: unknown) => {
        const definition = rawTable.rel(context as TContext, input.viewRelHelpers);
        const relResult = resolveViewRelDefinition(
          definition,
          input.resolveTableToken,
          input.resolveEntityToken,
        );
        if (Result.isError(relResult)) {
          throw relResult.error;
        }
        return relResult.value;
      },
      columnBindings,
      columnToSource: buildColumnSourceMapFromBindings(columnBindings),
    },
  });
}

function normalizeTableColumns(
  columns: Record<string, unknown>,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    entity?: SchemaDataEntityHandle<string>;
  },
): BetterResult<
  {
    normalizedColumns: TableColumns;
    columnBindings: Record<string, NormalizedColumnBinding>;
  },
  TuplSchemaNormalizationError
> {
  const normalizedColumns: TableColumns = {};
  const columnBindings: Record<string, NormalizedColumnBinding> = {};

  for (const [columnName, rawColumn] of Object.entries(columns)) {
    const normalizedResult = normalizeColumnBinding(columnName, rawColumn, options);
    if (Result.isError(normalizedResult)) {
      return normalizedResult;
    }
    const normalized = normalizedResult.value;
    normalizedColumns[columnName] = normalized.definition;
    columnBindings[columnName] = normalized.binding;
  }

  return Result.ok({ normalizedColumns, columnBindings });
}
