import type {
  DataEntityColumnMetadata,
  DataEntityHandle,
  DataEntityReadMetadataMap,
} from "@tupl/foundation";

/**
 * Schema contracts define logical schema definitions independent of DSL and runtime execution.
 */
export type PhysicalDialect = "postgres" | "sqlite";

export type SqlScalarType =
  | "text"
  | "integer"
  | "real"
  | "blob"
  | "boolean"
  | "timestamp"
  | "date"
  | "datetime"
  | "json";

type ColumnConstraintFlags =
  | {
      primaryKey?: false | undefined;
      unique?: false | undefined;
    }
  | {
      primaryKey: true;
      unique?: false | undefined;
    }
  | {
      primaryKey?: false | undefined;
      unique: true;
    };

interface ColumnDefinitionBase {
  type: SqlScalarType;
  nullable?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: PhysicalDialect;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
}

export type ColumnDefinition = ColumnDefinitionBase & ColumnConstraintFlags;

export type TableColumnDefinition = SqlScalarType | ColumnDefinition;

export type TableColumns = Record<string, TableColumnDefinition>;

export interface PrimaryKeyConstraint {
  columns: string[];
  name?: string;
}

export interface UniqueConstraint {
  columns: string[];
  name?: string;
}

export type ReferentialAction = "NO ACTION" | "RESTRICT" | "CASCADE" | "SET NULL" | "SET DEFAULT";

export interface ColumnForeignKeyReference {
  table: string;
  column: string;
  name?: string;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

export interface ForeignKeyConstraint {
  columns: string[];
  references: {
    table: string;
    columns: string[];
  };
  name?: string;
  onDelete?: ReferentialAction;
  onUpdate?: ReferentialAction;
}

export interface CheckConstraintIn {
  kind: "in";
  column: string;
  values: readonly (string | number | boolean | null)[];
  name?: string;
}

export type CheckConstraint = CheckConstraintIn;

export interface TableConstraints {
  primaryKey?: PrimaryKeyConstraint;
  unique?: UniqueConstraint[];
  foreignKeys?: ForeignKeyConstraint[];
  checks?: CheckConstraint[];
}

export interface TableDefinition {
  provider?: string;
  columns: TableColumns;
  constraints?: TableConstraints;
}

export interface SchemaDefinition {
  tables: Record<string, TableDefinition>;
}

export type SchemaDataEntityHandle<
  TColumns extends string = string,
  TRow extends Partial<Record<TColumns, unknown>> = Record<TColumns, unknown>,
  TColumnMetadata extends Partial<Record<TColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TColumns, TRow>,
> = DataEntityHandle<TColumns, TRow, TColumnMetadata>;

export type SchemaValueCoercionName = "isoTimestamp";
export type SchemaValueCoercion = SchemaValueCoercionName | ((value: unknown) => unknown);

export interface SchemaColRefToken {
  kind: "dsl_col_ref";
  ref?: string;
  table?: SchemaDslTableToken<string>;
  entity?: SchemaDataEntityHandle<string>;
  column?: string;
}

declare const SCHEMA_DSL_TABLE_TOKEN_BRAND: unique symbol;

export interface SchemaDslTableToken<TColumns extends string = string> {
  kind: "dsl_table_token";
  readonly __id: symbol;
  readonly [SCHEMA_DSL_TABLE_TOKEN_BRAND]: TColumns;
}
