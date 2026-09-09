import type { RelExpr, RelLocalOperation } from "@tupl/foundation";
import {
  derivedExpression,
  getLocalImplementation,
  isDerivedValue,
  registerLocalOperation,
} from "../dsl/derive";
import { normalizeProviderRowValue } from "../mapping/row-coercion";
import { Result, type Result as BetterResult } from "better-result";
import type { TuplResult, TuplSchemaNormalizationError } from "@tupl/foundation";
import { getDataEntityProvider } from "@tupl/provider-kit";

import { type SchemaBuilderState } from "../dsl/builder-state";
import { buildSchemaDslViewRelHelpers } from "../dsl/dsl-view-helpers";
import { isDslTableDefinition, isDslViewDefinition } from "../dsl/dsl-tokens";
import { createSchemaNormalizationError } from "../schema-errors";
import type {
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaDslTableToken,
  TableColumns,
  TableDefinition,
} from "../contracts/schema-contracts";
import type {
  NormalizedColumnBinding,
  NormalizedTableBinding,
} from "../contracts/normalized-contracts";
import type { SchemaDslViewRelHelpers } from "../contracts/schema-view-contracts";
import { buildColumnSourceMapFromBindings } from "./normalized-column-sources";
import { normalizeColumnBinding } from "./column-binding-normalizer";
import { validateCalculatedColumnDependencies } from "./calculated-column-validation";
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
      sourceHandle: rawTable.from,
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

  const compiled = new Map<RelLocalOperation, RelExpr>();
  const visiting = new Set<RelLocalOperation>();
  const inputNames = new Map<object, string>();
  const dependencyExpressions = new Map<object, RelExpr>();
  const resolveLocal = (expr: Extract<RelExpr, { kind: "local" }>): RelExpr => {
    const cached = compiled.get(expr.operation);
    if (cached) return cached;
    if (visiting.has(expr.operation)) throw new Error("Cyclic derived dependencies.");
    visiting.add(expr.operation);
    const implementation = getLocalImplementation(expr.operation);
    const args = Object.values(implementation.dependencies).map((dependency): RelExpr => {
      if (isDerivedValue(dependency)) {
        const dependencyExpr = derivedExpression(dependency);
        if (dependencyExpr.kind !== "local") throw new Error("Invalid derive handle.");
        return resolveLocal(dependencyExpr);
      }
      if (!dependency || typeof dependency !== "object")
        throw new Error("Invalid derived dependency.");
      const cachedDependency = dependencyExpressions.get(dependency);
      if (cachedDependency) return cachedDependency;
      const normalized = normalizeColumnBinding("dependency", dependency, {
        ...options,
        resolveLocal,
      });
      if (Result.isError(normalized)) throw normalized.error;
      const binding = normalized.value.binding;
      let input: RelExpr;
      if (binding.kind === "expr") input = binding.expr;
      else if (options.preserveQualifiedRef) {
        const index = binding.source.lastIndexOf(".");
        input = {
          kind: "column",
          ref:
            index < 0
              ? { column: binding.source }
              : {
                  table: binding.source.slice(0, index),
                  column: binding.source.slice(index + 1),
                },
        };
      } else {
        let name = inputNames.get(dependency);
        if (!name) {
          name = `__derive_input_${inputNames.size}`;
          while (Object.hasOwn(columns, name) || Object.hasOwn(columnBindings, name)) name += "_";
          inputNames.set(dependency, name);
          columnBindings[name] = binding;
        }
        input = { kind: "column", ref: { column: name } };
      }
      const operation = registerLocalOperation("dependency", {
        dependencies: { value: dependency },
        evaluate: ({ value }) => normalizeProviderRowValue(value, binding),
      });
      const expression: RelExpr = { kind: "local", operation, args: [input] };
      dependencyExpressions.set(dependency, expression);
      return expression;
    });
    const result: RelExpr = { ...expr, args };
    visiting.delete(expr.operation);
    compiled.set(expr.operation, result);
    return result;
  };

  for (const [columnName, rawColumn] of Object.entries(columns)) {
    const attempt = Result.try({
      try: () => normalizeColumnBinding(columnName, rawColumn, { ...options, resolveLocal }),
      catch: (error) =>
        createSchemaNormalizationError({
          operation: "normalize derived column",
          column: columnName,
          message: error instanceof Error ? error.message : String(error),
        }),
    });
    if (Result.isError(attempt)) return attempt;
    const normalizedResult = attempt.value;
    if (Result.isError(normalizedResult)) {
      return normalizedResult;
    }
    const normalized = normalizedResult.value;
    normalizedColumns[columnName] = normalized.definition;
    columnBindings[columnName] = normalized.binding;
    if (normalized.binding.kind === "expr" && containsLocal(normalized.binding.expr)) {
      const binding = normalized.binding;
      const operation = registerLocalOperation(columnName, {
        dependencies: {
          value: {
            kind: "dsl_calculated_column",
            expr: binding.expr,
            definition: normalized.definition,
          },
        },
        evaluate: ({ value }) => normalizeProviderRowValue(value, binding),
      });
      columnBindings[columnName] = {
        ...binding,
        expr: { kind: "local", operation, args: [binding.expr] },
      };
    }
  }

  return Result.ok({ normalizedColumns, columnBindings });
}

function containsLocal(expr: RelExpr): boolean {
  return expr.kind === "local" || (expr.kind === "function" && expr.args.some(containsLocal));
}
