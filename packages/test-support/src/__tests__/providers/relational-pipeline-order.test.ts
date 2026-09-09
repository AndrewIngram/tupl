import Database from "better-sqlite3";
import knex from "knex";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { Kysely, SqliteDialect } from "kysely";
import { describe, expect, it } from "vite-plus/test";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createKyselyProvider } from "@tupl/provider-kysely";
import { createObjectionProvider } from "@tupl/provider-objection";
import type { RelNode } from "@tupl/foundation";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

const records = sqliteTable("records", {
  id: integer("id"),
  category: text("category"),
  n: integer("n"),
});
const setup = [
  "CREATE TABLE records (id INTEGER, category TEXT, n INTEGER)",
  "INSERT INTO records VALUES (1, 'a', 9), (2, 'b', 2), (3, 'b', 2), (4, 'b', 2), (5, 'c', 6), (6, 'c', 6)",
];

function limited(input: RelNode): RelNode {
  return {
    id: "limit",
    kind: "limit_offset",
    convention: "local",
    input,
    limit: 1,
    output: input.output,
  };
}
function filtered(input: RelNode, column = "category"): RelNode {
  return {
    id: "filter",
    kind: "filter",
    convention: "local",
    input,
    where: [{ op: "eq", column, value: "b" }],
    output: input.output,
  };
}
function sorted(input: RelNode): RelNode {
  return {
    id: "sort",
    kind: "sort",
    convention: "local",
    input,
    orderBy: [{ source: { column: "n" }, direction: "asc" }],
    output: input.output,
  };
}
function cases(scan: RelNode) {
  const count: RelNode = {
    id: "count",
    kind: "aggregate",
    convention: "local",
    input: limited(scan),
    groupBy: [],
    metrics: [{ fn: "count", as: "cnt" }],
    output: [{ name: "cnt" }],
  };
  const swapped: RelNode = {
    id: "swap",
    kind: "project",
    convention: "local",
    input: scan,
    columns: [
      { kind: "column", source: { alias: "r", column: "category" }, output: "n" },
      { kind: "column", source: { alias: "r", column: "n" }, output: "category" },
    ],
    output: [{ name: "n" }, { name: "category" }],
  };
  const union: RelNode = {
    id: "union",
    kind: "set_op",
    convention: "local",
    op: "union_all",
    left: scan,
    right: { ...scan, id: "second_scan" },
    output: scan.output,
  };
  const ref: RelNode = {
    id: "ref",
    kind: "cte_ref",
    convention: "local",
    name: "c",
    select: ["category", "n"],
    output: scan.output,
  };
  const withBody = (body: RelNode): RelNode => ({
    id: "with",
    kind: "with",
    convention: "local",
    ctes: [
      {
        name: "c",
        query: {
          id: "cte_output",
          kind: "project",
          convention: "local",
          input: scan,
          columns: [
            { kind: "column", source: { alias: "r", column: "category" }, output: "category" },
            { kind: "column", source: { alias: "r", column: "n" }, output: "n" },
          ],
          output: scan.output,
        },
      },
    ],
    body,
    output: body.output,
  });
  const computed: RelNode = {
    id: "computed",
    kind: "project",
    convention: "local",
    input: scan,
    columns: [
      {
        kind: "expr",
        expr: {
          kind: "function",
          name: "add",
          args: [
            { kind: "column", ref: { alias: "r", column: "n" } },
            { kind: "literal", value: 1 },
          ],
        },
        output: "n",
      },
    ],
    output: [{ name: "n" }],
  };
  return [
    {
      name: "computed sort retains its projection",
      rel: sorted(computed),
      expected: [3, 3, 3, 7, 7, 10].map((n) => ({ n })),
      columns: { n: "integer" },
      runtimeOnly: true,
    },
    {
      name: "aggregate after limit",
      rel: count,
      expected: [{ cnt: 1 }],
      columns: { cnt: "integer" },
    },
    {
      name: "filter after limit",
      rel: filtered(limited(scan)),
      expected: [],
      columns: { category: "text", n: "integer" },
    },
    {
      name: "sort after limit",
      rel: sorted(limited(scan)),
      expected: [{ category: "a", n: 9 }],
      columns: { category: "text", n: "integer" },
    },
    {
      name: "filter after swapped projection",
      rel: filtered(swapped, "n"),
      expected: Array.from({ length: 3 }, () => ({ n: "b", category: 2 })),
      columns: { category: "integer", n: "text" },
    },
    {
      name: "set operation sort after limit",
      rel: sorted(limited(union)),
      expected: [{ category: "a", n: 9 }],
      columns: { category: "text", n: "integer" },
    },
    {
      name: "WITH filter after limit",
      rel: withBody(filtered(limited(ref))),
      expected: [],
      columns: { category: "text", n: "integer" },
    },
    {
      name: "WITH sort after limit",
      rel: withBody(sorted(limited(ref))),
      expected: [{ category: "a", n: 9 }],
      columns: { category: "text", n: "integer" },
    },
  ];
}

describe.each(["drizzle", "kysely", "objection"] as const)(
  "%s relational pipeline ordering",
  (backend) => {
    it("keeps unsafe flattening local and preserves operation order", async () => {
      const native = new Database(":memory:");
      native.exec(setup.join(";"));
      const objectionDb = knex({
        client: "better-sqlite3",
        connection: { filename: ":memory:" },
        useNullAsDefault: true,
      });
      const kyselyDb = new Kysely<{ records: { id: number; category: string; n: number } }>({
        dialect: new SqliteDialect({ database: native }),
      });
      try {
        if (backend === "objection")
          for (const statement of setup) await objectionDb.raw(statement);
        const entities = {
          records: { table: "records", shape: { id: "integer", category: "text", n: "integer" } },
        } as const;
        const provider =
          backend === "drizzle"
            ? createDrizzleProvider({
                dialect: "sqlite",
                db: drizzle(native),
                tables: { records: { table: records } },
              })
            : backend === "kysely"
              ? createKyselyProvider({ db: kyselyDb, entities })
              : createObjectionProvider({ knex: objectionDb, entities });
        const scan: RelNode = {
          id: "scan",
          kind: "scan",
          convention: "local",
          table: "records",
          entity: provider.entities.records,
          alias: "r",
          select: ["category", "n"],
          output: [{ name: "category" }, { name: "n" }],
        };
        for (const scenario of cases(scan)) {
          const capability = await provider.canExecute(scenario.rel, {});
          if (!scenario.runtimeOnly)
            expect(
              typeof capability === "boolean" ? capability : capability.supported,
              scenario.name,
            ).toBe(false);
          const builder = createSchemaBuilder();
          builder.view("result", () => scenario.rel, {
            columns: ({ col }) =>
              Object.fromEntries(
                Object.entries(scenario.columns).map(([name, type]) => [
                  name,
                  type === "text" ? col.string(name) : col.integer(name),
                ]),
              ),
          });
          const schema = createExecutableSchema(builder).unwrap();
          if (scenario.runtimeOnly && backend === "drizzle") {
            const explained = (
              await schema.explain({ sql: "SELECT * FROM result", context: {} })
            ).unwrap();
            const remoteSql = explained.providerPlans.flatMap((plan) =>
              (plan.description?.operations ?? []).flatMap((operation) =>
                operation.kind === "sql" && operation.sql ? [operation.sql] : [],
              ),
            );
            expect(remoteSql.some((sql) => /order by/i.test(sql))).toBe(true);
          }
          const result = await schema.query({ sql: "SELECT * FROM result", context: {} });
          expect(result.unwrap(), scenario.name).toEqual(scenario.expected);
        }
      } finally {
        await objectionDb.destroy();
        await kyselyDb.destroy();
        if (native.open) native.close();
      }
    });
  },
);
