import { describe, expect, it } from "vitest";

import {
  createSchemaBuilder,
  createDataEntityHandle,
  resolveTableColumnDefinition,
  toSqlDDL,
} from "../../src";

describe("query/schema dsl", () => {
  it("keeps foreignKey metadata on source-based table lens columns", () => {
    const ordersEntity = createDataEntityHandle<"id" | "vendor_id">({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const vendorsEntity = createDataEntityHandle<"id">({
      entity: "vendors_raw",
      provider: "warehouse",
    });

    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("my_vendors", vendorsEntity, {
      columns: () => ({
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
      }),
    });
    builder.table("my_orders", ordersEntity, {
      columns: () => ({
        id: { source: "id", type: "text", nullable: false, primaryKey: true },
        vendor_id: {
          source: "vendor_id",
          type: "text",
          nullable: false,
          foreignKey: {
            table: "my_vendors",
            column: "id",
          },
        },
      }),
    });
    const schema = builder.build();

    const resolved = resolveTableColumnDefinition(schema, "my_orders", "vendor_id");
    expect(resolved.foreignKey).toEqual({
      table: "my_vendors",
      column: "id",
    });

    const ddl = toSqlDDL(schema);
    expect(ddl).toContain('FOREIGN KEY ("vendor_id") REFERENCES "my_vendors" ("id")');
  });
});
