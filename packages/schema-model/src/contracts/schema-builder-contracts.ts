import type { derive } from "../dsl/derive";
import type { TuplResult } from "@tupl/foundation";
import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
  RelNode,
} from "@tupl/foundation";

import type { SchemaDataEntityHandle, TableConstraints } from "./schema-contracts";
import type { DslTableDefinition, DslViewDefinition } from "./table-definition-contracts";
import type { SchemaDslViewRelHelpers, SchemaViewRelNodeInput } from "./schema-view-contracts";
import type { SchemaTypedColumnBuilder } from "./typed-column-builder-contracts";

/**
 * Schema builder contracts own the public table/view builder call signatures and helper callback types.
 */
type SchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
> = SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> & {
  (ref: string): RelExpr;
  <TColumns extends string, TColumn extends TColumns>(
    table: import("./table-definition-contracts").SchemaDslRelationRef<TColumns>,
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

type ColumnHelpers<
  C extends string,
  M extends Partial<Record<C, DataEntityColumnMetadata<any>>>,
> = {
  derive: typeof derive;
  col: SchemaColumnsColHelper<C, M>;
  expr: SchemaColumnExprHelpers;
};

type SchemaBuilderTableMethods = {
  <
    C extends string,
    D extends Record<string, import("./table-definition-contracts").DslTableColumnInput<C>>,
    Row extends Partial<Record<C, unknown>> = Record<C, unknown>,
    M extends Partial<Record<C, DataEntityColumnMetadata<any>>> = DataEntityReadMetadataMap<C, Row>,
  >(
    name: string,
    from: SchemaDataEntityHandle<C, Row, M>,
    input: { columns: D | ((helpers: ColumnHelpers<C, M>) => D); constraints?: TableConstraints },
  ): DslTableDefinition<Extract<keyof D, string>, C, D>;
};

type SchemaBuilderViewMethods<TContext> = {
  <
    C extends string,
    D extends Record<string, import("./table-definition-contracts").DslViewColumnInput<C>>,
  >(
    name: string,
    rel: (
      helpers: SchemaDslViewRelHelpers,
      context: TContext,
    ) => SchemaViewRelNodeInput<C> | RelNode,
    input: {
      columns:
        | D
        | ((helpers: ColumnHelpers<C, DataEntityReadMetadataMap<C, Record<C, unknown>>>) => D);
      constraints?: TableConstraints;
    },
  ): DslViewDefinition<TContext, Extract<keyof D, string>, C, D>;
  <D extends Record<string, import("./table-definition-contracts").DslViewColumnInput<string>>>(
    name: string,
    rel: (context: TContext) => SchemaViewRelNodeInput<string> | RelNode,
    input: { columns: D; constraints?: TableConstraints },
  ): DslViewDefinition<TContext, Extract<keyof D, string>, string, D>;
};

export interface SchemaBuilder<TContext> {
  table: SchemaBuilderTableMethods;
  view: SchemaBuilderViewMethods<TContext>;
  build(): TuplResult<import("./schema-contracts").SchemaDefinition>;
}
