import { getDataEntityAdapter } from "@tupl/provider-kit";

import { type SchemaBuilderState } from "../dsl/builder-state";
import {
  buildSchemaDslViewRelHelpers,
  isDslTableDefinition,
  isDslViewDefinition,
} from "../dsl/builder-helpers";
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
): SchemaDefinition {
  const tables: Record<string, TableDefinition> = {};
  const bindings: Record<string, NormalizedTableBinding> = {};
  const tableTokenToName = buildTableTokenMap(state);
  const resolveTableToken = createTableTokenResolver(tableTokenToName);
  const resolveEntityToken = createEntityTokenResolver();
  const viewRelHelpers = buildSchemaDslViewRelHelpers() as SchemaDslViewRelHelpers;

  for (const [tableName, rawTable] of state.definitions.entries()) {
    if (isDslTableDefinition(rawTable)) {
      const { definition, binding } = buildPhysicalTableDefinition(tableName, rawTable, {
        resolveTableToken,
        resolveEntityToken,
      });
      tables[tableName] = definition;
      bindings[tableName] = binding;
      continue;
    }

    if (isDslViewDefinition(rawTable)) {
      const { definition, binding } = buildViewTableDefinition(tableName, rawTable, {
        resolveTableToken,
        resolveEntityToken,
        viewRelHelpers,
      });
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

function buildTableTokenMap<TContext>(state: SchemaBuilderState<TContext>): Map<symbol, string> {
  const tableTokenToName = new Map<symbol, string>();

  for (const [tableName, rawTable] of state.definitions.entries()) {
    if (isDslTableDefinition(rawTable) || isDslViewDefinition(rawTable)) {
      tableTokenToName.set(rawTable.tableToken.__id, tableName);
    }
  }

  return tableTokenToName;
}

function createTableTokenResolver(tableTokenToName: Map<symbol, string>) {
  return (token: SchemaDslTableToken<string>): string => {
    const tableName = tableTokenToName.get(token.__id);
    if (!tableName) {
      throw new Error("Schema DSL table token could not be resolved to a table name.");
    }
    return tableName;
  };
}

function createEntityTokenResolver() {
  return (entity: SchemaDataEntityHandle<string>): string => {
    if (!entity.entity || entity.entity.length === 0) {
      throw new Error("Schema DSL data entity handle is missing entity name.");
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
): { definition: TableDefinition; binding: NormalizedTableBinding } {
  const { normalizedColumns, columnBindings } = normalizeTableColumns(rawTable.columns, {
    preserveQualifiedRef: false,
    ...resolvers,
    entity: rawTable.from,
  });
  validateCalculatedColumnDependencies(tableName, columnBindings);

  const definition: TableDefinition = {
    provider: rawTable.from.provider,
    columns: normalizedColumns,
    ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
  };
  const adapter = getDataEntityAdapter(rawTable.from);

  return {
    definition,
    binding: {
      kind: "physical",
      provider: rawTable.from.provider,
      entity: rawTable.from.entity,
      columnBindings,
      columnToSource: buildColumnSourceMapFromBindings(columnBindings),
      ...(adapter ? { adapter } : {}),
    },
  };
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
): { definition: TableDefinition; binding: NormalizedTableBinding } {
  const { normalizedColumns, columnBindings } = normalizeTableColumns(rawTable.columns, {
    preserveQualifiedRef: true,
    resolveTableToken: input.resolveTableToken,
    resolveEntityToken: input.resolveEntityToken,
  });
  validateCalculatedColumnDependencies(tableName, columnBindings);

  return {
    definition: {
      provider: "__view__",
      columns: normalizedColumns,
      ...(rawTable.constraints ? { constraints: rawTable.constraints } : {}),
    },
    binding: {
      kind: "view",
      rel: (context: unknown) => {
        const definition = rawTable.rel(context as TContext, input.viewRelHelpers);
        return resolveViewRelDefinition(
          definition,
          input.resolveTableToken,
          input.resolveEntityToken,
        );
      },
      columnBindings,
      columnToSource: buildColumnSourceMapFromBindings(columnBindings),
    },
  };
}

function normalizeTableColumns(
  columns: Record<string, unknown>,
  options: {
    preserveQualifiedRef: boolean;
    resolveTableToken: (token: SchemaDslTableToken<string>) => string;
    resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string;
    entity?: SchemaDataEntityHandle<string>;
  },
): {
  normalizedColumns: TableColumns;
  columnBindings: Record<string, NormalizedColumnBinding>;
} {
  const normalizedColumns: TableColumns = {};
  const columnBindings: Record<string, NormalizedColumnBinding> = {};

  for (const [columnName, rawColumn] of Object.entries(columns)) {
    const normalized = normalizeColumnBinding(columnName, rawColumn, options);
    normalizedColumns[columnName] = normalized.definition;
    columnBindings[columnName] = normalized.binding;
  }

  return { normalizedColumns, columnBindings };
}
