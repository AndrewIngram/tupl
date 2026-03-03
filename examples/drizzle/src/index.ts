import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { createDrizzleProvider } from "@sqlql/drizzle";
import { defineProviders, defineSchema, query } from "sqlql";

const usersTable = sqliteTable("users", {
  id: text("id").primaryKey().notNull(),
  email: text("email").notNull(),
});

const ordersTable = sqliteTable("orders", {
  id: text("id").primaryKey().notNull(),
  userId: text("user_id").notNull(),
  totalCents: integer("total_cents").notNull(),
});

const schema = defineSchema({
  tables: {
    users: {
      provider: "drizzle",
      columns: {
        id: { type: "text", nullable: false },
        email: { type: "text", nullable: false },
      },
    },
    orders: {
      provider: "drizzle",
      columns: {
        id: { type: "text", nullable: false },
        user_id: { type: "text", nullable: false },
        total_cents: { type: "integer", nullable: false },
      },
    },
  },
});

async function main(): Promise<void> {
  const sqlite = new Database(":memory:");
  sqlite.exec(`
    CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, email TEXT NOT NULL);
    CREATE TABLE orders (id TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL, total_cents INTEGER NOT NULL);
    INSERT INTO users (id, email) VALUES ('u1', 'ada@example.com'), ('u2', 'ben@example.com');
    INSERT INTO orders (id, user_id, total_cents) VALUES
      ('o1', 'u1', 1500),
      ('o2', 'u1', 3000),
      ('o3', 'u2', 700);
  `);

  const db = drizzle(sqlite);

  const providers = defineProviders({
    drizzle: createDrizzleProvider({
      db,
      tables: {
        users: {
          table: usersTable,
          columns: {
            id: usersTable.id,
            email: usersTable.email,
          },
        },
        orders: {
          table: ordersTable,
          columns: {
            id: ordersTable.id,
            user_id: ordersTable.userId,
            total_cents: ordersTable.totalCents,
          },
        },
      },
    }),
  });

  const rows = await query({
    schema,
    providers,
    context: {},
    sql: `
      SELECT o.id, u.email, o.total_cents
      FROM orders o
      JOIN users u ON o.user_id = u.id
      WHERE o.total_cents >= 1000
      ORDER BY o.total_cents DESC
    `,
  });

  console.log(rows);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
