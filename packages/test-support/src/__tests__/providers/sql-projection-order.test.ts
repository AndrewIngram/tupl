import Database from "better-sqlite3";
import knex from "knex";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { Kysely, SqliteDialect } from "kysely";
import type { RelNode } from "@tupl/foundation";
import { describe, expect, it } from "vite-plus/test";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createKyselyProvider } from "@tupl/provider-kysely";
import { createObjectionProvider } from "@tupl/provider-objection";

const records = sqliteTable("records", {
  id: integer("id"),
  category: text("category"),
  n: integer("n"),
});
const setup = [
  "CREATE TABLE records (id INTEGER, category TEXT, n INTEGER)",
  "INSERT INTO records VALUES (1, 'a', 9), (2, 'b', 2), (3, 'b', 2), (4, 'b', 2), (5, 'c', 6), (6, 'c', 6)",
];
describe.each(["drizzle", "kysely", "objection"] as const)("%s projection ordering", (backend) => {
  it.each([
    "aggregate-before",
    "aggregate-after",
    "source-before",
    "source-after",
    "source-filter-before",
    "source-qualified-before",
    "source-qualified-after",
    "with-before",
    "with-after",
  ] as const)("%s", async (mode) => {
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
      if (backend === "objection") {
        for (const statement of setup) await objectionDb.raw(statement);
      }
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
        alias: "r",
        select: ["id", "category", "n"],
        output: [{ name: "r.id" }, { name: "r.category" }, { name: "r.n" }],
      };
      const aggregate: RelNode = {
        id: "aggregate",
        kind: "aggregate",
        convention: "local",
        input: scan,
        groupBy: [{ alias: "r", column: "category" }],
        metrics: [
          { fn: "count", as: "cnt" },
          { fn: "sum", column: { alias: "r", column: "n" }, as: "total" },
        ],
        output: [{ name: "category" }, { name: "cnt" }, { name: "total" }],
      };
      const cteRef: RelNode = {
        id: "cte",
        kind: "cte_ref",
        convention: "local",
        name: "groups",
        select: ["category", "cnt", "total"],
        alias: "g",
        output: aggregate.output,
      };
      const qualified = mode.includes("qualified");
      const aggregateMode = !mode.startsWith("source");
      const base = mode.startsWith("with") ? cteRef : aggregateMode ? aggregate : scan;
      const project: Extract<RelNode, { kind: "project" }> = {
        id: "project",
        kind: "project",
        convention: "local",
        input: base,
        columns: aggregateMode
          ? [
              { source: { column: "category" }, output: "category" },
              { source: { column: "total" }, output: "cnt" },
              { source: { column: "cnt" }, output: "total" },
            ]
          : [
              { source: { alias: "r", column: "id" }, output: qualified ? "r.n" : "n" },
              { source: { alias: "r", column: "n" }, output: qualified ? "r.id" : "id" },
            ],
        output: aggregateMode
          ? aggregate.output
          : [{ name: qualified ? "r.n" : "n" }, { name: qualified ? "r.id" : "id" }],
      };
      const before = mode.endsWith("before");
      const sort: Extract<RelNode, { kind: "sort" }> = {
        id: "sort",
        kind: "sort",
        convention: "local",
        input: before ? base : project,
        orderBy: [
          {
            source: { ...(qualified ? { alias: "r" } : {}), column: aggregateMode ? "cnt" : "id" },
            direction: "desc",
          },
        ],
        output: before ? base.output : project.output,
      };
      const body: RelNode =
        mode === "source-filter-before"
          ? {
              ...project,
              input: {
                id: "filter",
                kind: "filter",
                convention: "local",
                input: scan,
                where: [{ column: "id", op: "gt", value: 3 }],
                output: scan.output,
              },
            }
          : before
            ? { ...project, input: sort }
            : sort;
      const rel: RelNode = mode.startsWith("with")
        ? {
            id: "with",
            kind: "with",
            convention: "local",
            ctes: [{ name: "groups", query: aggregate }],
            body,
            output: body.output,
          }
        : body;
      const compiled = (await provider.compile(rel, {})).unwrap();
      const actual = (await provider.execute(compiled, {})).unwrap();
      const expected =
        mode === "source-filter-before"
          ? native.prepare("SELECT id AS n, n AS id FROM records WHERE records.id > 3").all()
          : aggregateMode
            ? native
                .prepare(
                  `SELECT category, SUM(n) AS cnt, COUNT(*) AS total FROM records GROUP BY category ORDER BY ${before ? "COUNT(*)" : "SUM(n)"} DESC`,
                )
                .all()
            : native
                .prepare(
                  `SELECT id AS "${qualified ? "r.n" : "n"}", n AS "${qualified ? "r.id" : "id"}" FROM records ORDER BY records.${before ? "id" : "n"} DESC`,
                )
                .all();
      expect(actual).toEqual(expected);
    } finally {
      await objectionDb.destroy();
      await kyselyDb.destroy();
      if (native.open) native.close();
    }
  });
});
