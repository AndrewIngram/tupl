import {
  createSchemaBuilder,
  type SchemaBuilder,
  type SchemaDefinition,
  type TableColumnDefinition,
  type TableConstraints,
} from "../../src";

export function buildSchema<TContext = Record<string, never>>(
  register: (builder: SchemaBuilder<TContext>) => void,
): SchemaDefinition {
  const builder = createSchemaBuilder<TContext>();
  register(builder);
  return builder.build();
}

type StaticTableDefinition<TColumns extends Record<string, TableColumnDefinition>> = {
  provider?: string;
  columns: TColumns;
  constraints?: TableConstraints;
};

type StaticSchemaInput = Record<string, StaticTableDefinition<Record<string, TableColumnDefinition>>>;

type StaticSchemaDefinition<TTables extends StaticSchemaInput> = {
  tables: {
    [TTableName in keyof TTables]: {
      provider?: string;
      columns: TTables[TTableName]["columns"];
      constraints?: TableConstraints;
    };
  };
};

export function buildStaticSchema<const TTables extends StaticSchemaInput>(
  tables: TTables,
): StaticSchemaDefinition<TTables> {
  const builder = createSchemaBuilder<Record<string, never>>();
  for (const [name, table] of Object.entries(tables)) {
    builder.table({
      name,
      ...(table.provider ? { provider: table.provider } : {}),
      columns: table.columns,
      ...(table.constraints ? { constraints: table.constraints } : {}),
    });
  }
  return builder.build() as StaticSchemaDefinition<TTables>;
}
