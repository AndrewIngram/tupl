/**
 * Schema model root owns the logical schema contract: DSL entrypoints, schema/query types,
 * timestamps, and DDL helpers. DSL-token detail, planning hooks, normalization, mapping,
 * definition, enum-link resolution, and validation live behind explicit subpaths.
 */
export { asIso8601Timestamp, type Iso8601TimestampString, type TimestampValue } from "./timestamps";
export type {
  CheckConstraint,
  ColumnDefinition,
  ForeignKeyConstraint,
  PhysicalDialect,
  PrimaryKeyConstraint,
  ReferentialAction,
  SchemaDataEntityHandle,
  SchemaDefinition,
  SchemaValueCoercion,
  SchemaValueCoercionName,
  SqlScalarType,
  TableColumnDefinition,
  TableColumns,
  TableConstraints,
  TableDefinition,
  UniqueConstraint,
} from "./contracts/schema-contracts";
export type { SchemaBuilder } from "./contracts/schema-builder-contracts";
export type {
  AggregateFunction,
  ColumnValue,
  FilterClauseBase,
  NullFilterClause,
  QueryRow,
  ScalarFilterClause,
  ScanFilterClause,
  ScanFilterOperator,
  ScanOrderBy,
  SqlTypeValue,
  TableAggregateMetric,
  TableAggregateRequest,
  TableLookupRequest,
  TableMethods,
  TableMethodsForSchema,
  TableMethodsMap,
  TableRow,
  TableScanRequest,
} from "./contracts/query-contracts";
export { createSchemaBuilder, defineTableMethods, isSchemaBuilder } from "./dsl/builder";
export { toSqlDDL, type SqlDdlOptions } from "./ddl";
