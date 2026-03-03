import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { and, eq } from "drizzle-orm";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createDrizzleProvider, type DrizzleProviderTableConfig } from "@sqlql/drizzle";
import {
  defineProviders,
  defineSchema,
  query,
} from "sqlql";

const vendorsRawTable = sqliteTable("vendors_raw", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  name: text("name").notNull(),
});

const ordersRawTable = sqliteTable("orders_raw", {
  id: text("id").primaryKey().notNull(),
  orgId: text("org_id").notNull(),
  userId: text("user_id").notNull(),
  vendorId: text("vendor_id").notNull(),
  totalCents: integer("total_cents").notNull(),
  createdAt: text("created_at").notNull(),
});

type Context = {
  orgId: string;
  userId: string;
};

async function main(): Promise<void> {
  const sqlite = new Database(":memory:");
  sqlite.exec(`
    CREATE TABLE vendors_raw (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      name TEXT NOT NULL
    );

    CREATE TABLE orders_raw (
      id TEXT PRIMARY KEY NOT NULL,
      org_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      vendor_id TEXT NOT NULL,
      total_cents INTEGER NOT NULL,
      created_at TEXT NOT NULL
    );

    INSERT INTO vendors_raw (id, org_id, name) VALUES
      ('v1', 'org_1', 'Northwind'),
      ('v2', 'org_1', 'Acme Parts'),
      ('v3', 'org_2', 'Other Org Vendor');

    INSERT INTO orders_raw (id, org_id, user_id, vendor_id, total_cents, created_at) VALUES
      ('o1', 'org_1', 'u1', 'v1', 1500, '2026-02-01T00:00:00.000Z'),
      ('o2', 'org_1', 'u1', 'v2', 3200, '2026-02-03T00:00:00.000Z'),
      ('o3', 'org_1', 'u2', 'v1', 7000, '2026-02-04T00:00:00.000Z'),
      ('o4', 'org_2', 'u9', 'v3', 1200, '2026-02-05T00:00:00.000Z');
  `);

  const db = drizzle(sqlite);

  const tableConfigs = {
    orders: {
      table: ordersRawTable,
      scope: (context: Context) =>
        and(
          eq(ordersRawTable.orgId, context.orgId),
          eq(ordersRawTable.userId, context.userId),
        ),
    },
    vendors: {
      table: vendorsRawTable,
      scope: (context: Context) => eq(vendorsRawTable.orgId, context.orgId),
    },
  } satisfies Record<"orders" | "vendors", DrizzleProviderTableConfig<Context, string>>;

  const dbProvider = createDrizzleProvider<Context, typeof tableConfigs>({
    name: "dbProvider",
    db,
    tables: tableConfigs,
  });

  const schema = defineSchema<Context>(({ table, view, col, expr, agg, rel }) => {
    const myOrders = table({
      from: dbProvider.tables.orders,
      columns: {
        id: col(dbProvider.tables.orders, "id"),
        vendorId: col(dbProvider.tables.orders, "vendor_id"),
        totalCents: col(dbProvider.tables.orders, "total_cents"),
        createdAt: col(dbProvider.tables.orders, "created_at"),
      },
    });

    const vendorsForOrg = table({
      from: dbProvider.tables.vendors,
      columns: {
        id: col(dbProvider.tables.vendors, "id"),
        name: col(dbProvider.tables.vendors, "name"),
      },
    });

    return {
      tables: {
        myOrders,
        vendorsForOrg,
        myVendorSpend: view({
          rel: () =>
            rel.aggregate({
              from: rel.join({
                left: rel.scan(myOrders),
                right: rel.scan(vendorsForOrg),
                on: expr.eq(col(myOrders, "vendorId"), col(vendorsForOrg, "id")),
                type: "inner",
              }),
              groupBy: [col(vendorsForOrg, "id"), col(vendorsForOrg, "name")],
              measures: {
                totalSpendCents: agg.sum(col(myOrders, "totalCents")),
                orderCount: agg.count(),
              },
            }),
          columns: {
            vendorId: col("id"),
            vendorName: col("name"),
            totalSpendCents: col("totalSpendCents"),
            orderCount: col("orderCount"),
          },
        }),
      },
    };
  });

  const providers = defineProviders({
    dbProvider,
  });

  const rows = await query({
    schema,
    providers,
    context: {
      orgId: "org_1",
      userId: "u1",
    },
    sql: `
      SELECT vendorName, totalSpendCents, orderCount
      FROM myVendorSpend
      ORDER BY totalSpendCents DESC
    `,
  });

  const orderRows = await query({
    schema,
    providers,
    context: {
      orgId: "org_1",
      userId: "u1",
    },
    sql: `
      SELECT o.id, v.name, o.totalCents
      FROM myOrders o
      JOIN vendorsForOrg v ON o.vendorId = v.id
      WHERE o.totalCents >= 2000
      ORDER BY o.totalCents DESC
    `,
  });

  console.log("myVendorSpend:");
  console.log(rows);
  console.log("joined rows:");
  console.log(orderRows);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
