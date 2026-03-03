import Database from "better-sqlite3";
import { describe, expect, it } from "vitest";

import type { ProviderAdapter, ProviderFragment, QueryRow, RelNode } from "../../src";
import { createDrizzleProvider, type DrizzleQueryExecutor } from "../../packages/drizzle/src";
import { createKyselyProvider } from "../../packages/kysely/src";

function seedDatabase(): Database.Database {
  const sqlite = new Database(":memory:");
  sqlite.exec(`
    CREATE TABLE users (
      id TEXT PRIMARY KEY NOT NULL,
      email TEXT NOT NULL
    );
    CREATE TABLE orders (
      id TEXT PRIMARY KEY NOT NULL,
      user_id TEXT NOT NULL,
      total_cents INTEGER NOT NULL
    );
    INSERT INTO users (id, email) VALUES
      ('u1', 'ada@example.com'),
      ('u2', 'ben@example.com');
    INSERT INTO orders (id, user_id, total_cents) VALUES
      ('o1', 'u1', 1500),
      ('o2', 'u1', 3000),
      ('o3', 'u2', 700);
  `);
  return sqlite;
}

function buildRel(): RelNode {
  return {
    id: "project_1",
    kind: "project",
    convention: "provider:warehouse",
    input: {
      id: "sort_1",
      kind: "sort",
      convention: "provider:warehouse",
      input: {
        id: "join_1",
        kind: "join",
        convention: "provider:warehouse",
        joinType: "inner",
        left: {
          id: "orders_scan",
          kind: "scan",
          convention: "provider:warehouse",
          table: "orders",
          alias: "o",
          select: ["id", "user_id", "total_cents"],
          output: [{ name: "o.id" }, { name: "o.user_id" }, { name: "o.total_cents" }],
        },
        right: {
          id: "users_scan",
          kind: "scan",
          convention: "provider:warehouse",
          table: "users",
          alias: "u",
          select: ["id", "email"],
          output: [{ name: "u.id" }, { name: "u.email" }],
        },
        leftKey: { alias: "o", column: "user_id" },
        rightKey: { alias: "u", column: "id" },
        output: [
          { name: "o.id" },
          { name: "o.user_id" },
          { name: "o.total_cents" },
          { name: "u.id" },
          { name: "u.email" },
        ],
      },
      orderBy: [{ source: { alias: "o", column: "total_cents" }, direction: "desc" }],
      output: [
        { name: "o.id" },
        { name: "o.user_id" },
        { name: "o.total_cents" },
        { name: "u.id" },
        { name: "u.email" },
      ],
    },
    columns: [
      { source: { alias: "o", column: "id" }, output: "id" },
      { source: { alias: "u", column: "email" }, output: "email" },
      { source: { alias: "o", column: "total_cents" }, output: "total_cents" },
    ],
    output: [{ name: "id" }, { name: "email" }, { name: "total_cents" }],
  };
}

function createMockDrizzleDb(rowsByTable: Map<object, QueryRow[]>): DrizzleQueryExecutor {
  return {
    select(...args: unknown[]) {
      const selection = (args[0] ?? {}) as Record<string, { name?: string }>;
      let rows: QueryRow[] = [];
      const builder = {
        from(table: object) {
          rows = [...(rowsByTable.get(table) ?? [])];
          return builder;
        },
        where() {
          return builder;
        },
        orderBy() {
          return builder;
        },
        limit(value: number) {
          rows = rows.slice(0, value);
          return builder;
        },
        offset(value: number) {
          rows = rows.slice(value);
          return builder;
        },
        async execute() {
          return rows.map((row) => {
            const out: Record<string, unknown> = {};
            for (const [outputName, column] of Object.entries(selection)) {
              const source = column.name ?? outputName;
              out[outputName] = row[source] ?? null;
            }
            return out;
          });
        },
      };
      return builder;
    },
  };
}

async function runRelFragment(
  providerName: string,
  provider: ProviderAdapter<object>,
): Promise<Array<Record<string, unknown>>> {
  const rel = buildRel();
  const fragment: ProviderFragment = {
    kind: "rel",
    provider: providerName,
    rel,
  };

  const canExecute = await provider.canExecute(fragment, {});
  expect(typeof canExecute === "boolean" ? canExecute : canExecute.supported).toBe(true);

  const compiled = await provider.compile(fragment, {});
  return provider.execute(compiled, {}) as Promise<Array<Record<string, unknown>>>;
}

describe("provider conformance (rel fragments)", () => {
  it("returns equivalent rows for drizzle and kysely providers", async () => {
    const expected = [
      { id: "o2", email: "ada@example.com", total_cents: 3000 },
      { id: "o1", email: "ada@example.com", total_cents: 1500 },
      { id: "o3", email: "ben@example.com", total_cents: 700 },
    ];

    {
      const usersTable = { name: "users" };
      const ordersTable = { name: "orders" };
      const db = createMockDrizzleDb(
        new Map<object, QueryRow[]>([
          [
            usersTable,
            [
              { id: "u1", email: "ada@example.com" },
              { id: "u2", email: "ben@example.com" },
            ],
          ],
          [
            ordersTable,
            [
              { id: "o1", user_id: "u1", total_cents: 1500 },
              { id: "o2", user_id: "u1", total_cents: 3000 },
              { id: "o3", user_id: "u2", total_cents: 700 },
            ],
          ],
        ]),
      );

      const drizzleProvider = createDrizzleProvider({
        db,
        tables: {
          users: {
            table: usersTable,
            columns: {
              id: { name: "id" } as any,
              email: { name: "email" } as any,
            },
          },
          orders: {
            table: ordersTable,
            columns: {
              id: { name: "id" } as any,
              user_id: { name: "user_id" } as any,
              total_cents: { name: "total_cents" } as any,
            },
          },
        },
      });

      const rows = await runRelFragment("drizzle", drizzleProvider);
      expect(rows).toEqual(expected);
    }

    {
      const sqlite = seedDatabase();
      try {
        const kyselyProvider = createKyselyProvider({
          executor: {
            async executeSql(args) {
              return sqlite.prepare(args.sql).all() as Array<Record<string, unknown>>;
            },
          },
        });

        const rows = await runRelFragment("kysely", kyselyProvider);
        expect(rows).toEqual(expected);
      } finally {
        sqlite.close();
      }
    }
  });
});
