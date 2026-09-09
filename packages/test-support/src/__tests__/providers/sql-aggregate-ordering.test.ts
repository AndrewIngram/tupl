import Database from "better-sqlite3";
import knex from "knex";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { Kysely, SqliteDialect } from "kysely";
import { describe, expect, it } from "vite-plus/test";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createKyselyProvider } from "@tupl/provider-kysely";
import { createObjectionProvider } from "@tupl/provider-objection";
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
const queries = [
  "WITH d(x) AS (SELECT COUNT(*) AS total FROM records ORDER BY total DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(*) AS total FROM records GROUP BY category ORDER BY total DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(*) AS total FROM records GROUP BY category ORDER BY 2 DESC LIMIT 2 OFFSET 1) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category AS total, COUNT(*) AS total FROM records GROUP BY category ORDER BY 2 DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(*) AS n FROM records GROUP BY category ORDER BY n DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(*) AS total FROM records GROUP BY category ORDER BY total DESC LIMIT 2) SELECT label FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(DISTINCT n) AS total FROM records GROUP BY category ORDER BY total DESC, category) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, SUM(n) AS total FROM records GROUP BY category ORDER BY total DESC, category) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, AVG(n) AS total FROM records GROUP BY category ORDER BY total DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, MIN(n) AS total FROM records GROUP BY category ORDER BY total DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, MAX(n) AS total FROM records GROUP BY category ORDER BY total DESC) SELECT * FROM d",
  "WITH d(label, amount) AS (SELECT category, COUNT(*) AS total FROM records GROUP BY category ORDER BY category DESC) SELECT * FROM d",
];

describe.each(["drizzle", "kysely", "objection"] as const)("%s aggregate ordering", (backend) => {
  it.each(queries)("%s", async (sql) => {
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
      const builder = createSchemaBuilder();
      builder.table("records", provider.entities.records, {
        columns: { id: "integer", category: "text", n: "integer" },
      });
      const schema = createExecutableSchema(builder).unwrap();
      const explanation = (await schema.explain({ sql, context: {} })).unwrap();
      const remoteSql = explanation.providerPlans.flatMap((plan) =>
        (plan.description?.operations ?? []).flatMap((operation) =>
          operation.kind === "sql" && operation.sql ? [operation.sql] : [],
        ),
      );
      expect(remoteSql.some((statement) => /order by/i.test(statement))).toBe(true);
      const expected = native.prepare(sql).all();
      const actual = (await schema.query({ sql, context: {} })).unwrap();
      expect(actual).toEqual(expected);
    } finally {
      await objectionDb.destroy();
      await kyselyDb.destroy();
      if (native.open) native.close();
    }
  });
});
