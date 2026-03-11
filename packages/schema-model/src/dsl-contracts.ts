import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
  RelNode,
} from "@tupl/foundation";

import type {
  ColumnForeignKeyReference,
  PhysicalDialect,
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaValueCoercion,
  SqlScalarType,
  TableColumnDefinition,
  TableConstraints,
} from "./schema-contracts";
import type { AggregateFunction } from "./query-contracts";

/**
 * DSL contracts define the builder-facing tokens, typed-column helpers, and view-rel authoring types.
 */
type CompatibleSourceScalarType<TTarget extends SqlScalarType> = TTarget extends "real"
  ? "real" | "integer"
  : TTarget;

type IsAny<T> = 0 extends 1 & T ? true : false;
type IsExactly<T, U> = [T] extends [U] ? ([U] extends [T] ? true : false) : false;
type StripNullish<T> = Exclude<T, null | undefined>;

type InferSourceScalarTypeFromRead<TRead> =
  IsAny<TRead> extends true
    ? never
    : [StripNullish<TRead>] extends [never]
      ? never
      : StripNullish<TRead> extends number
        ? "integer" | "real"
        : StripNullish<TRead> extends boolean
          ? "boolean"
          : StripNullish<TRead> extends Uint8Array
            ? "blob"
            : StripNullish<TRead> extends Date
              ? "timestamp"
              : StripNullish<TRead> extends string
                ? "text"
                : StripNullish<TRead> extends object
                  ? "json"
                  : never;

type ExplicitSourceScalarType<TMetadata> = Extract<
  TMetadata extends { type?: infer TType } ? TType : never,
  SqlScalarType
>;

type KnownSourceScalarType<TMetadata> = [ExplicitSourceScalarType<TMetadata>] extends [never]
  ? InferSourceScalarTypeFromRead<
      TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
    >
  : IsExactly<ExplicitSourceScalarType<TMetadata>, SqlScalarType> extends true
    ? InferSourceScalarTypeFromRead<
        TMetadata extends { readonly __read__?: infer TRead } ? TRead : never
      >
    : ExplicitSourceScalarType<TMetadata>;

type IsCompileTimeCompatibleSourceType<
  TSource extends SqlScalarType,
  TTarget extends SqlScalarType,
> = [Extract<TSource, CompatibleSourceScalarType<TTarget>>] extends [never] ? false : true;

type CompatibleColumnName<
  TColumns extends string,
  TColumnMetadata extends Partial<Record<TColumns, DataEntityColumnMetadata<any>>>,
  TTarget extends SqlScalarType,
> = Extract<
  {
    [K in TColumns]: [KnownSourceScalarType<TColumnMetadata[K]>] extends [never]
      ? K
      : IsCompileTimeCompatibleSourceType<
            KnownSourceScalarType<TColumnMetadata[K]>,
            TTarget
          > extends true
        ? K
        : never;
  }[TColumns],
  string
>;

export interface SchemaViewEqExpr {
  kind: "eq";
  left: SchemaColRefToken;
  right: SchemaColRefToken;
}

export interface SchemaViewScanNode {
  kind: "scan";
  table: string;
  entity?: SchemaDataEntityHandle<string>;
}

export interface SchemaViewJoinNode {
  kind: "join";
  left: SchemaViewRelNode;
  right: SchemaViewRelNode;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

export interface SchemaViewAggregateMetric {
  kind: "metric";
  fn: AggregateFunction;
  column?: SchemaColRefToken;
  distinct?: boolean;
}

export interface SchemaViewAggregateNode {
  kind: "aggregate";
  from: SchemaViewRelNode;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNode = SchemaViewScanNode | SchemaViewJoinNode | SchemaViewAggregateNode;

interface SchemaViewRelNodeInputBase<TColumns extends string> {
  readonly __columns__?: TColumns;
}

interface SchemaViewScanNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "scan";
  table:
    | string
    | import("./schema-contracts").SchemaDslTableToken<string>
    | SchemaDataEntityHandle<TColumns>;
}

interface SchemaViewJoinNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "join";
  left: SchemaViewRelNodeInput;
  right: SchemaViewRelNodeInput;
  on: SchemaViewEqExpr;
  type: "inner" | "left" | "right" | "full";
}

interface SchemaViewAggregateNodeInput<
  TColumns extends string = string,
> extends SchemaViewRelNodeInputBase<TColumns> {
  kind: "aggregate";
  from: SchemaViewRelNodeInput;
  groupBy: Record<string, SchemaColRefToken>;
  measures: Record<string, SchemaViewAggregateMetric>;
}

export type SchemaViewRelNodeInput<TColumns extends string = string> =
  | SchemaViewScanNodeInput<TColumns>
  | SchemaViewJoinNodeInput<TColumns>
  | SchemaViewAggregateNodeInput<TColumns>;

export interface SchemaColumnLensDefinition {
  source: string | SchemaColRefToken;
  type?: SqlScalarType;
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
}

export interface SchemaTypedColumnDefinition<TSourceColumn extends string = string> {
  kind: "dsl_typed_column";
  sourceColumn: TSourceColumn;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

export interface SchemaCalculatedColumnDefinition {
  kind: "dsl_calculated_column";
  expr: RelExpr;
  definition: TableColumnDefinition;
  coerce?: SchemaValueCoercion;
}

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

export interface DslTableDefinition<
  TMappedColumns extends string = string,
  TSourceColumns extends string = string,
> {
  kind: "dsl_table";
  tableToken: import("./schema-contracts").SchemaDslTableToken<TMappedColumns>;
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
  tableToken: import("./schema-contracts").SchemaDslTableToken<TColumns>;
  rel: (
    context: TContext,
    helpers: SchemaDslViewRelHelpers,
  ) => SchemaViewRelNodeInput<TRelColumns> | RelNode;
  columns: Record<TColumns, DslViewColumnInput<TRelColumns>>;
  constraints?: TableConstraints;
}

type SchemaDslRelationRef<TColumns extends string> =
  | import("./schema-contracts").SchemaDslTableToken<TColumns>
  | DslTableDefinition<TColumns, string>
  | DslViewDefinition<any, TColumns, string>;

interface SchemaTypedColumnBuilderOptions {
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
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
    (table: string): SchemaViewScanNodeInput<string>;
    (
      table: import("./schema-contracts").SchemaDslTableToken<string>,
    ): SchemaViewScanNodeInput<string>;
    <TColumns extends string>(
      table: import("./schema-contracts").SchemaDslTableToken<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      entity: SchemaDataEntityHandle<TColumns>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslTableDefinition<TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
    <TColumns extends string>(
      table: DslViewDefinition<any, TColumns, string>,
    ): SchemaViewScanNodeInput<TColumns>;
  };
  join: <TLeftColumns extends string, TRightColumns extends string>(input: {
    left: SchemaViewRelNodeInput<TLeftColumns>;
    right: SchemaViewRelNodeInput<TRightColumns>;
    on: SchemaViewEqExpr;
    type?: "inner" | "left" | "right" | "full";
  }) => SchemaViewJoinNodeInput<TLeftColumns | TRightColumns>;
  aggregate: <
    TGroupBy extends Record<string, SchemaColRefToken>,
    TMeasures extends Record<string, SchemaViewAggregateMetric>,
  >(input: {
    from: SchemaViewRelNodeInput<string>;
    groupBy: TGroupBy;
    measures: TMeasures;
  }) => SchemaViewAggregateNodeInput<Extract<keyof TGroupBy | keyof TMeasures, string>>;
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

type SchemaTypedColumnBuilderMethod<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>>,
  TType extends SqlScalarType,
  TOptions extends SchemaTypedColumnBuilderOptions,
> = {
  <TSourceColumn extends CompatibleColumnName<TSourceColumns, TColumnMetadata, TType>>(
    sourceColumn: TSourceColumn,
    options?: TOptions,
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TSourceColumn extends TSourceColumns>(
    sourceColumn: TSourceColumn,
    options: TOptions & { coerce: SchemaValueCoercion },
  ): SchemaTypedColumnDefinition<TSourceColumn>;
  <TRelColumns extends string, TColumn extends TRelColumns>(
    table: SchemaDataEntityHandle<TRelColumns> | SchemaDslRelationRef<TRelColumns>,
    column: TColumn,
    options?: TOptions,
  ): SchemaColumnLensDefinition;
  (
    expr: RelExpr,
    options?: Omit<TOptions, "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap">,
  ): SchemaCalculatedColumnDefinition;
};

export interface SchemaTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> {
  id: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    Omit<
      SchemaTypedColumnBuilderOptions,
      "primaryKey" | "nullable" | "enum" | "enumFrom" | "enumMap"
    >
  >;
  string: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "text",
    SchemaTypedColumnBuilderOptions
  >;
  integer: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "integer",
    SchemaTypedColumnBuilderOptions
  >;
  real: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "real",
    SchemaTypedColumnBuilderOptions
  >;
  blob: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "blob",
    SchemaTypedColumnBuilderOptions
  >;
  boolean: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "boolean",
    SchemaTypedColumnBuilderOptions
  >;
  timestamp: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "timestamp",
    SchemaTypedColumnBuilderOptions
  >;
  date: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "date",
    SchemaTypedColumnBuilderOptions
  >;
  datetime: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "datetime",
    SchemaTypedColumnBuilderOptions
  >;
  json: SchemaTypedColumnBuilderMethod<
    TSourceColumns,
    TColumnMetadata,
    "json",
    SchemaTypedColumnBuilderOptions
  >;
}

type SchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> = SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> & {
  (ref: string): RelExpr;
  <TColumns extends string, TColumn extends TColumns>(
    table: SchemaDslRelationRef<TColumns>,
    column: TColumn,
  ): RelExpr;
};

interface SchemaColumnExprHelpers {
  literal: (value: string | number | boolean | null) => RelExpr;
  eq: (left: RelExpr, right: RelExpr) => RelExpr;
  neq: (left: RelExpr, right: RelExpr) => RelExpr;
  gt: (left: RelExpr, right: RelExpr) => RelExpr;
  gte: (left: RelExpr, right: RelExpr) => RelExpr;
  lt: (left: RelExpr, right: RelExpr) => RelExpr;
  lte: (left: RelExpr, right: RelExpr) => RelExpr;
  add: (left: RelExpr, right: RelExpr) => RelExpr;
  subtract: (left: RelExpr, right: RelExpr) => RelExpr;
  multiply: (left: RelExpr, right: RelExpr) => RelExpr;
  divide: (left: RelExpr, right: RelExpr) => RelExpr;
  and: (...args: RelExpr[]) => RelExpr;
  or: (...args: RelExpr[]) => RelExpr;
  not: (input: RelExpr) => RelExpr;
}

type SchemaBuilderTableMethods = {
  <
    TSourceColumns extends string,
    TMappedColumns extends string,
    TRow extends Partial<Record<TSourceColumns, unknown>> = Record<TSourceColumns, unknown>,
    TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
      DataEntityReadMetadataMap<TSourceColumns, TRow>,
  >(
    name: string,
    from: SchemaDataEntityHandle<TSourceColumns, TRow, TColumnMetadata>,
    input: {
      columns:
        | Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>
        | ((helpers: {
            col: SchemaColumnsColHelper<TSourceColumns, TColumnMetadata>;
            expr: SchemaColumnExprHelpers;
          }) => Record<TMappedColumns, DslTableColumnInput<TSourceColumns>>);
      constraints?: TableConstraints;
    },
  ): DslTableDefinition<TMappedColumns, TSourceColumns>;
};

type SchemaBuilderViewMethods<TContext> = {
  <TRelColumns extends string, TColumns extends string>(
    name: string,
    rel: (
      helpers: SchemaDslViewRelHelpers,
      context: TContext,
    ) => SchemaViewRelNodeInput<TRelColumns> | RelNode,
    input: {
      columns:
        | ((helpers: {
            col: SchemaColumnsColHelper<
              TRelColumns,
              DataEntityReadMetadataMap<TRelColumns, Record<TRelColumns, unknown>>
            >;
            expr: SchemaColumnExprHelpers;
          }) => Record<TColumns, DslViewColumnInput<TRelColumns>>)
        | Record<TColumns, DslViewColumnInput<TRelColumns>>;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, TRelColumns>;
  <TColumns extends string>(
    name: string,
    rel: (context: TContext) => SchemaViewRelNodeInput<string> | RelNode,
    input: {
      columns: Record<TColumns, DslViewColumnInput<string>>;
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, TColumns, string>;
};

export interface SchemaBuilder<TContext> {
  table: SchemaBuilderTableMethods;
  view: SchemaBuilderViewMethods<TContext>;
  build(): import("./schema-contracts").SchemaDefinition;
}
