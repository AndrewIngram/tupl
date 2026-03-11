import type {
  DataEntityColumnMetadata,
  DataEntityReadMetadataMap,
  RelExpr,
} from "@tupl/foundation";

import type {
  ColumnDefinition,
  ColumnForeignKeyReference,
  SchemaCalculatedColumnDefinition,
  SchemaColRefToken,
  SchemaColumnLensDefinition,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
  SchemaTypedColumnDefinition,
  SchemaTypedColumnBuilder,
  SchemaValueCoercion,
  SqlScalarType,
} from "./types";

interface SchemaTypedColumnBuilderOptions {
  nullable?: boolean;
  primaryKey?: boolean;
  unique?: boolean;
  enum?: readonly string[];
  enumFrom?: SchemaColRefToken | string;
  enumMap?: Record<string, string>;
  physicalType?: string;
  physicalDialect?: string;
  foreignKey?: ColumnForeignKeyReference;
  description?: string;
  coerce?: SchemaValueCoercion;
}

/**
 * Builder helpers own the DSL token shapes, type guards, and low-level column/view helper factories.
 */
export function createSchemaDslTableToken<
  TColumns extends string,
>(): SchemaDslTableToken<TColumns> {
  return {
    kind: "dsl_table_token",
    __id: Symbol("schema_dsl_table"),
  } as SchemaDslTableToken<TColumns>;
}

export function isSchemaDslTableToken(value: unknown): value is SchemaDslTableToken<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table_token" &&
    typeof (value as { __id?: unknown }).__id === "symbol"
  );
}

export function isSchemaDataEntityHandle(value: unknown): value is SchemaDataEntityHandle<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "data_entity" &&
    typeof (value as { entity?: unknown }).entity === "string" &&
    typeof (value as { provider?: unknown }).provider === "string"
  );
}

export function isRelExpr(value: unknown): value is RelExpr {
  if (!value || typeof value !== "object") {
    return false;
  }

  const kind = (value as { kind?: unknown }).kind;
  if (kind === "literal") {
    return true;
  }
  if (kind === "function") {
    return Array.isArray((value as { args?: unknown }).args);
  }
  if (kind === "column") {
    return (
      !!(value as { ref?: unknown }).ref && typeof (value as { ref?: unknown }).ref === "object"
    );
  }
  return kind === "subquery";
}

export function isDslTableDefinition(value: unknown): boolean {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_table" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

export function isDslViewDefinition(value: unknown): boolean {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_view" &&
    isSchemaDslTableToken((value as { tableToken?: unknown }).tableToken)
  );
}

export function isSchemaTypedColumnDefinition(
  value: unknown,
): value is SchemaTypedColumnDefinition<string> {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_typed_column" &&
    typeof (value as { sourceColumn?: unknown }).sourceColumn === "string"
  );
}

export function isSchemaCalculatedColumnDefinition(
  value: unknown,
): value is SchemaCalculatedColumnDefinition {
  return (
    !!value &&
    typeof value === "object" &&
    (value as { kind?: unknown }).kind === "dsl_calculated_column" &&
    isRelExpr((value as { expr?: unknown }).expr)
  );
}

export function isSchemaColRefToken(value: unknown): value is SchemaColRefToken {
  if (!value || typeof value !== "object" || (value as { kind?: unknown }).kind !== "dsl_col_ref") {
    return false;
  }

  const token = value as { ref?: unknown; table?: unknown; entity?: unknown; column?: unknown };
  const hasStringRef = typeof token.ref === "string";
  const hasTableColumnRef = isSchemaDslTableToken(token.table) && typeof token.column === "string";
  const hasEntityColumnRef =
    isSchemaDataEntityHandle(token.entity) && typeof token.column === "string";
  return hasStringRef || hasTableColumnRef || hasEntityColumnRef;
}

export function isColumnLensDefinition(value: unknown): value is SchemaColumnLensDefinition {
  if (!value || typeof value !== "object") {
    return false;
  }

  const source = (value as { source?: unknown }).source;
  return typeof source === "string" || isSchemaColRefToken(source);
}

export function buildTypedColumnBuilder<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>(): SchemaTypedColumnBuilder<TSourceColumns, TColumnMetadata> {
  const buildSourceLensDefinition = (
    source: string | SchemaColRefToken,
    type: SqlScalarType,
    options: SchemaTypedColumnBuilderOptions = {},
  ): SchemaColumnLensDefinition => ({
    source,
    type,
    ...(options.nullable != null ? { nullable: options.nullable } : {}),
    ...(options.primaryKey != null ? { primaryKey: options.primaryKey } : {}),
    ...(options.unique != null ? { unique: options.unique } : {}),
    ...(options.enum ? { enum: options.enum } : {}),
    ...(options.enumFrom ? { enumFrom: options.enumFrom } : {}),
    ...(options.enumMap ? { enumMap: options.enumMap } : {}),
    ...(options.physicalType ? { physicalType: options.physicalType } : {}),
    ...(options.physicalDialect ? { physicalDialect: options.physicalDialect as never } : {}),
    ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
    ...(options.description ? { description: options.description } : {}),
    ...(options.coerce ? { coerce: options.coerce } : {}),
  });

  const buildTypedColumnDefinition = <TSourceColumn extends string>(
    sourceColumn: TSourceColumn,
    type: SqlScalarType,
    options: SchemaTypedColumnBuilderOptions = {},
  ): SchemaTypedColumnDefinition<TSourceColumn> => {
    const definition = { type } as ColumnDefinition;
    if (options.nullable != null) {
      definition.nullable = options.nullable;
    }
    if (options.primaryKey === true) {
      definition.primaryKey = true;
    } else if (options.unique === true) {
      definition.unique = true;
    }
    if (options.enum) {
      definition.enum = options.enum;
    }
    if (options.enumFrom) {
      definition.enumFrom = options.enumFrom;
    }
    if (options.enumMap) {
      definition.enumMap = options.enumMap;
    }
    if (options.physicalType) {
      definition.physicalType = options.physicalType;
    }
    if (options.physicalDialect) {
      definition.physicalDialect = options.physicalDialect as never;
    }
    if (options.foreignKey) {
      definition.foreignKey = options.foreignKey;
    }
    if (options.description) {
      definition.description = options.description;
    }
    return {
      kind: "dsl_typed_column",
      sourceColumn,
      definition,
      ...(options.coerce ? { coerce: options.coerce } : {}),
    };
  };

  const buildCalculatedColumnDefinition = (
    expr: RelExpr,
    type: SqlScalarType,
    options: Omit<
      SchemaTypedColumnBuilderOptions,
      "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
    > = {},
  ): SchemaCalculatedColumnDefinition => {
    const definition = {
      type,
      ...(options.nullable != null ? { nullable: options.nullable } : {}),
      ...(options.physicalType ? { physicalType: options.physicalType } : {}),
      ...(options.physicalDialect ? { physicalDialect: options.physicalDialect as never } : {}),
      ...(options.foreignKey ? { foreignKey: options.foreignKey } : {}),
      ...(options.description ? { description: options.description } : {}),
    } satisfies ColumnDefinition;

    return {
      kind: "dsl_calculated_column",
      expr,
      definition,
      ...(options.coerce ? { coerce: options.coerce } : {}),
    };
  };

  const toSchemaDslTableToken = <TColumns extends string>(
    table: unknown,
  ): SchemaDslTableToken<TColumns> => {
    if (isSchemaDslTableToken(table)) {
      return table as SchemaDslTableToken<TColumns>;
    }
    return (table as { tableToken: SchemaDslTableToken<TColumns> }).tableToken;
  };

  const build = (type: SqlScalarType) =>
    ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      if (isRelExpr(arg1)) {
        return buildCalculatedColumnDefinition(
          arg1,
          type,
          (arg2 as
            | Omit<
                SchemaTypedColumnBuilderOptions,
                "primaryKey" | "unique" | "enum" | "enumFrom" | "enumMap"
              >
            | undefined) ?? {},
        );
      }

      if (
        (isSchemaDslTableToken(arg1) || isDslTableDefinition(arg1) || isDslViewDefinition(arg1)) &&
        typeof arg2 === "string"
      ) {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            table: toSchemaDslTableToken(arg1),
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      if (isSchemaDataEntityHandle(arg1) && typeof arg2 === "string") {
        return buildSourceLensDefinition(
          {
            kind: "dsl_col_ref",
            entity: arg1,
            column: arg2,
          },
          type,
          (arg3 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
        );
      }

      return buildTypedColumnDefinition(
        arg1 as TSourceColumns,
        type,
        (arg2 as SchemaTypedColumnBuilderOptions | undefined) ?? {},
      );
    }) as never;

  return {
    id: ((arg1: unknown, arg2?: unknown, arg3?: unknown) => {
      const options = (
        isRelExpr(arg1)
          ? arg2
          : isSchemaDslTableToken(arg1) ||
              isDslTableDefinition(arg1) ||
              isDslViewDefinition(arg1) ||
              isSchemaDataEntityHandle(arg1)
            ? arg3
            : arg2
      ) as SchemaTypedColumnBuilderOptions | undefined;
      return (build("text") as (...args: unknown[]) => unknown)(arg1, arg2, {
        ...options,
        nullable: false,
        primaryKey: true,
      });
    }) as never,
    string: build("text"),
    integer: build("integer"),
    real: build("real"),
    blob: build("blob"),
    boolean: build("boolean"),
    timestamp: build("timestamp"),
    date: build("date"),
    datetime: build("datetime"),
    json: build("json"),
  };
}

export function buildColumnExprHelpers() {
  const fn = (name: string, ...args: RelExpr[]): RelExpr => ({
    kind: "function",
    name,
    args,
  });

  return {
    literal(value: string | number | boolean | null) {
      return { kind: "literal", value } satisfies RelExpr;
    },
    eq(left: RelExpr, right: RelExpr) {
      return fn("eq", left, right);
    },
    neq(left: RelExpr, right: RelExpr) {
      return fn("neq", left, right);
    },
    gt(left: RelExpr, right: RelExpr) {
      return fn("gt", left, right);
    },
    gte(left: RelExpr, right: RelExpr) {
      return fn("gte", left, right);
    },
    lt(left: RelExpr, right: RelExpr) {
      return fn("lt", left, right);
    },
    lte(left: RelExpr, right: RelExpr) {
      return fn("lte", left, right);
    },
    add(left: RelExpr, right: RelExpr) {
      return fn("add", left, right);
    },
    subtract(left: RelExpr, right: RelExpr) {
      return fn("subtract", left, right);
    },
    multiply(left: RelExpr, right: RelExpr) {
      return fn("multiply", left, right);
    },
    divide(left: RelExpr, right: RelExpr) {
      return fn("divide", left, right);
    },
    and(...args: RelExpr[]) {
      return fn("and", ...args);
    },
    or(...args: RelExpr[]) {
      return fn("or", ...args);
    },
    not(input: RelExpr) {
      return fn("not", input);
    },
  };
}

export function buildSchemaColumnsColHelper<
  TSourceColumns extends string,
  TColumnMetadata extends Partial<Record<TSourceColumns, DataEntityColumnMetadata<any>>> =
    DataEntityReadMetadataMap<TSourceColumns, Record<TSourceColumns, unknown>>,
>() {
  const toSchemaDslTableToken = <TColumns extends string>(
    table: unknown,
  ): SchemaDslTableToken<TColumns> => {
    if (isSchemaDslTableToken(table)) {
      return table as SchemaDslTableToken<TColumns>;
    }
    return (table as { tableToken: SchemaDslTableToken<TColumns> }).tableToken;
  };

  return Object.assign(function col<TColumns extends string, TColumn extends TColumns>(
    tableOrRef: unknown,
    column?: TColumn,
  ): RelExpr {
    if (typeof tableOrRef === "string") {
      if (column != null) {
        throw new Error(
          "Schema DSL column expr col(ref) does not accept a second argument for string refs.",
        );
      }
      return {
        kind: "column",
        ref: { column: tableOrRef },
      } satisfies RelExpr;
    }

    if (column == null) {
      throw new Error("Schema DSL column expr col(table, column) requires a column name.");
    }

    return {
      kind: "column",
      ref: {
        table: toSchemaDslTableToken(tableOrRef) as unknown as string,
        column,
      },
    } satisfies RelExpr;
  }, buildTypedColumnBuilder<TSourceColumns, TColumnMetadata>());
}

export function buildSchemaDslViewRelHelpers() {
  const toSchemaDslTableToken = <TColumns extends string>(
    table: unknown,
  ): SchemaDslTableToken<TColumns> => {
    if (isSchemaDslTableToken(table)) {
      return table as SchemaDslTableToken<TColumns>;
    }
    return (table as { tableToken: SchemaDslTableToken<TColumns> }).tableToken;
  };

  return {
    col<TColumns extends string, TColumn extends TColumns>(
      tableOrEntity: unknown,
      column?: TColumn,
    ): SchemaColRefToken {
      if (typeof tableOrEntity === "string") {
        if (column != null) {
          throw new Error(
            "Schema DSL rel col(ref) does not accept a second argument for string refs.",
          );
        }
        return {
          kind: "dsl_col_ref",
          ref: tableOrEntity,
        };
      }

      if (column == null) {
        throw new Error("Schema DSL rel col(table, column) requires a column name.");
      }

      if (isSchemaDataEntityHandle(tableOrEntity)) {
        return {
          kind: "dsl_col_ref",
          entity: tableOrEntity,
          column,
        };
      }

      return {
        kind: "dsl_col_ref",
        table: toSchemaDslTableToken(tableOrEntity),
        column,
      };
    },
    expr: {
      eq(left: SchemaColRefToken, right: SchemaColRefToken) {
        return {
          kind: "eq",
          left,
          right,
        };
      },
    },
    agg: {
      count() {
        return { kind: "metric", fn: "count" };
      },
      countDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "count", column, distinct: true };
      },
      sum(column: SchemaColRefToken) {
        return { kind: "metric", fn: "sum", column };
      },
      sumDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "sum", column, distinct: true };
      },
      avg(column: SchemaColRefToken) {
        return { kind: "metric", fn: "avg", column };
      },
      avgDistinct(column: SchemaColRefToken) {
        return { kind: "metric", fn: "avg", column, distinct: true };
      },
      min(column: SchemaColRefToken) {
        return { kind: "metric", fn: "min", column };
      },
      max(column: SchemaColRefToken) {
        return { kind: "metric", fn: "max", column };
      },
    },
    scan(table: unknown) {
      return {
        kind: "scan",
        table:
          typeof table === "string" || isSchemaDataEntityHandle(table)
            ? table
            : toSchemaDslTableToken(table),
      };
    },
    join(input: {
      left: unknown;
      right: unknown;
      on: { kind: "eq"; left: SchemaColRefToken; right: SchemaColRefToken };
      type?: "inner" | "left" | "right" | "full";
    }) {
      return {
        kind: "join",
        left: input.left,
        right: input.right,
        on: input.on,
        type: input.type ?? "inner",
      };
    },
    aggregate(input: {
      from: unknown;
      groupBy: Record<string, SchemaColRefToken>;
      measures: Record<
        string,
        { kind: "metric"; fn: string; column?: SchemaColRefToken; distinct?: true }
      >;
    }) {
      return {
        kind: "aggregate",
        from: input.from,
        groupBy: input.groupBy,
        measures: input.measures,
      };
    },
  };
}
