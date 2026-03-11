import type { RelExpr, RelNode } from "@tupl/foundation";

import type {
  SchemaBuilder,
  SchemaCalculatedColumnDefinition,
  SchemaColRefToken,
  SchemaColumnLensDefinition,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaDslTableToken,
  SchemaTypedColumnDefinition,
  SchemaViewAggregateMetric,
  SchemaViewEqExpr,
  SchemaViewRelNodeInput,
  TableColumnDefinition,
  TableConstraints,
  TableMethodsForSchema,
  TableMethodsMap,
} from "./types";
import {
  buildColumnExprHelpers as buildColumnExprHelpersInternal,
  buildSchemaColumnsColHelper as buildSchemaColumnsColHelperInternal,
  buildSchemaDslViewRelHelpers as buildSchemaDslViewRelHelpersInternal,
  createSchemaDslTableToken,
  isColumnLensDefinition as isColumnLensDefinitionInternal,
  isDslTableDefinition as isDslTableDefinitionInternal,
  isDslViewDefinition as isDslViewDefinitionInternal,
  isRelExpr as isRelExprInternal,
  isSchemaCalculatedColumnDefinition as isSchemaCalculatedColumnDefinitionInternal,
  isSchemaColRefToken as isSchemaColRefTokenInternal,
  isSchemaDataEntityHandle as isSchemaDataEntityHandleInternal,
  isSchemaDslTableToken as isSchemaDslTableTokenInternal,
  isSchemaTypedColumnDefinition as isSchemaTypedColumnDefinitionInternal,
} from "./builder-helpers";
import { buildRegisteredSchemaDefinition } from "./normalization";

/**
 * Builder owns the schema DSL entrypoints used to author logical tables and views.
 */
type DslTableColumnInput<TSourceColumns extends string = string> =
  | TableColumnDefinition
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

type DslViewColumnInput<TSourceColumns extends string = string> =
  | SchemaColumnLensDefinition
  | SchemaColRefToken
  | SchemaTypedColumnDefinition<TSourceColumns>
  | SchemaCalculatedColumnDefinition;

export type SchemaDslRelationRef<TColumns extends string> =
  | SchemaDslTableToken<TColumns>
  | DslTableDefinition<TColumns, string>
  | DslViewDefinition<any, TColumns, string>;

export interface DslTableDefinition<
  TMappedColumns extends string = string,
  TSourceColumns extends string = string,
> {
  kind: "dsl_table";
  tableToken: SchemaDslTableToken<TMappedColumns>;
  from: SchemaDataEntityHandle<TSourceColumns>;
  columns: Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>;
  constraints?: TableConstraints;
}

export interface DslViewDefinition<
  TContext,
  TColumns extends string = string,
  TRelColumns extends string = string,
> {
  kind: "dsl_view";
  tableToken: SchemaDslTableToken<TColumns>;
  rel: (
    context: TContext,
    helpers: SchemaDslViewRelHelpers,
  ) => SchemaViewRelNodeInput<TRelColumns> | RelNode;
  columns: Record<TColumns, DslViewColumnInput<TRelColumns>>;
  constraints?: TableConstraints;
}

interface SchemaDslRelExprHelpers {
  eq: (left: SchemaColRefToken, right: SchemaColRefToken) => SchemaViewEqExpr;
}

interface SchemaDslAggHelpers {
  count: () => SchemaViewAggregateMetric;
  countDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sum: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  sumDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avg: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  avgDistinct: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  min: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
  max: (column: SchemaColRefToken) => SchemaViewAggregateMetric;
}

interface SchemaDslRelHelpers {
  scan: {
    (table: string): SchemaViewRelNodeInput<string>;
    (table: SchemaDslTableToken<string>): SchemaViewRelNodeInput<string>;
    <TColumns extends string>(
      table: SchemaDslTableToken<TColumns>,
    ): SchemaViewRelNodeInput<TColumns>;
    <TColumns extends string>(
      entity: SchemaDataEntityHandle<TColumns>,
    ): SchemaViewRelNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslTableDefinition<TColumns, string>,
    ): SchemaViewRelNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslViewDefinition<any, TColumns, string>,
    ): SchemaViewRelNodeInput<TColumns>;
  };
  join: <TLeftColumns extends string, TRightColumns extends string>(input: {
    left: SchemaViewRelNodeInput<TLeftColumns>;
    right: SchemaViewRelNodeInput<TRightColumns>;
    on: SchemaViewEqExpr;
    type?: "inner" | "left" | "right" | "full";
  }) => SchemaViewRelNodeInput<TLeftColumns | TRightColumns>;
  aggregate: <
    TGroupBy extends Record<string, SchemaColRefToken>,
    TMeasures extends Record<string, SchemaViewAggregateMetric>,
  >(input: {
    from: SchemaViewRelNodeInput<string>;
    groupBy: TGroupBy;
    measures: TMeasures;
  }) => SchemaViewRelNodeInput<Extract<keyof TGroupBy | keyof TMeasures, string>>;
}

interface SchemaDslRelColHelpers {
  (ref: string): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    entity: SchemaDataEntityHandle<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
  <TColumns extends string, TColumn extends TColumns>(
    table: SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): SchemaColRefToken;
}

export interface SchemaDslViewRelHelpers extends SchemaDslRelHelpers {
  col: SchemaDslRelColHelpers;
  expr: SchemaDslRelExprHelpers;
  agg: SchemaDslAggHelpers;
}

type RegisteredSchemaDefinition<TContext> =
  | DslTableDefinition<string, string>
  | DslViewDefinition<TContext, string, string>;

export interface SchemaBuilderState<TContext> {
  definitions: Map<string, RegisteredSchemaDefinition<TContext>>;
}

export const schemaBuilderState = new WeakMap<object, SchemaBuilderState<any>>();

export function isSchemaDslTableToken(value: unknown): value is SchemaDslTableToken<string> {
  return isSchemaDslTableTokenInternal(value);
}

export function isSchemaDataEntityHandle(value: unknown): value is SchemaDataEntityHandle<string> {
  return isSchemaDataEntityHandleInternal(value);
}

export function isRelExpr(value: unknown): value is RelExpr {
  return isRelExprInternal(value);
}

export function isDslTableDefinition(value: unknown): value is DslTableDefinition {
  return isDslTableDefinitionInternal(value);
}

export function isDslViewDefinition<TContext>(
  value: unknown,
): value is DslViewDefinition<TContext, string, string> {
  return isDslViewDefinitionInternal(value);
}

export function isSchemaTypedColumnDefinition(
  value: unknown,
): value is SchemaTypedColumnDefinition<string> {
  return isSchemaTypedColumnDefinitionInternal(value);
}

export function isSchemaCalculatedColumnDefinition(
  value: unknown,
): value is SchemaCalculatedColumnDefinition {
  return isSchemaCalculatedColumnDefinitionInternal(value);
}

export function isSchemaColRefToken(value: unknown): value is SchemaColRefToken {
  return isSchemaColRefTokenInternal(value);
}

export function isColumnLensDefinition(value: unknown): value is SchemaColumnLensDefinition {
  return isColumnLensDefinitionInternal(value);
}

export function buildSchemaDslViewRelHelpers(): SchemaDslViewRelHelpers {
  return buildSchemaDslViewRelHelpersInternal() as SchemaDslViewRelHelpers;
}

export function createSchemaBuilder<TContext>(): SchemaBuilder<TContext> {
  const state: SchemaBuilderState<TContext> = {
    definitions: new Map(),
  };

  const registerDefinition = <TDefinition extends RegisteredSchemaDefinition<TContext>>(
    name: string,
    definition: TDefinition,
  ): TDefinition => {
    if (typeof name !== "string" || name.trim().length === 0) {
      throw new Error("Schema builder table/view name must be a non-empty string.");
    }
    if (state.definitions.has(name)) {
      throw new Error(`Schema builder already contains a table or view named ${name}.`);
    }
    state.definitions.set(name, definition);
    return definition;
  };

  const table = ((name: unknown, from: unknown, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      !isSchemaDataEntityHandle(from) ||
      !input
    ) {
      throw new Error(
        "Schema builder table(name, source, config) requires a non-empty name, a data entity handle, and a config object.",
      );
    }

    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelperInternal(),
            expr: buildColumnExprHelpersInternal(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_table",
      tableToken: createSchemaDslTableToken(),
      from,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["table"];

  const view = ((name: unknown, relFactory: unknown, input: any) => {
    if (
      typeof name !== "string" ||
      name.trim().length === 0 ||
      typeof relFactory !== "function" ||
      !input
    ) {
      throw new Error(
        "Schema builder view(name, source, config) requires a non-empty name, a rel function, and a config object.",
      );
    }
    const rel = (context: TContext, helpers: SchemaDslViewRelHelpers) =>
      (relFactory as Function).length === 0
        ? (relFactory as () => SchemaViewRelNodeInput<string> | RelNode)()
        : (
            relFactory as (
              helpers: SchemaDslViewRelHelpers,
              context: TContext,
            ) => SchemaViewRelNodeInput<string> | RelNode
          )(helpers, context);
    const columns =
      typeof input.columns === "function"
        ? input.columns({
            col: buildSchemaColumnsColHelperInternal(),
            expr: buildColumnExprHelpersInternal(),
          })
        : input.columns;

    return registerDefinition(name, {
      kind: "dsl_view",
      tableToken: createSchemaDslTableToken(),
      rel,
      columns,
      ...(input.constraints ? { constraints: input.constraints } : {}),
    });
  }) as SchemaBuilder<TContext>["view"];

  const builder: SchemaBuilder<TContext> = {
    table,
    view,
    build() {
      return buildRegisteredSchemaDefinition(state);
    },
  };

  schemaBuilderState.set(builder as object, state);
  return builder;
}

export function isSchemaBuilder<TContext = unknown>(
  value: unknown,
): value is SchemaBuilder<TContext> {
  return !!value && typeof value === "object" && schemaBuilderState.has(value as object);
}

export function defineTableMethods<TContext, TMethods extends TableMethodsMap<TContext>>(
  methods: TMethods,
): TMethods;

export function defineTableMethods<TSchema extends SchemaDefinition, TContext>(
  schema: TSchema,
  methods: TableMethodsForSchema<TSchema, TContext>,
): TableMethodsForSchema<TSchema, TContext>;

export function defineTableMethods(...args: unknown[]): unknown {
  if (args.length === 1) {
    return args[0];
  }

  if (args.length === 2) {
    return args[1];
  }

  throw new Error("defineTableMethods expects either (methods) or (schema, methods).");
}
