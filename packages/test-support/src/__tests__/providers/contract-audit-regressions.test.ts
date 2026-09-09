import { mapProviderRowsToLogical } from "@tupl/schema-model/mapping";
import { getNormalizedTableBinding } from "@tupl/schema-model/normalization";
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
import { createExecutableSchemaSession, type QueryStepEvent } from "@tupl/runtime/session";
import type { RelNode } from "@tupl/foundation";

const records = sqliteTable("records", {
  id: integer("id"),
  category: text("category"),
  n: integer("n"),
});
const setup = [
  "CREATE TABLE records (id INTEGER, category TEXT, n INTEGER)",
  "INSERT INTO records VALUES (1,'a',9),(2,'b',2),(3,'b',2),(4,NULL,NULL),(5,'c',-6),(6,'c',0)",
];
const parityCases = [
  "SELECT id,COALESCE(n,0) AS zero,ABS(n) AS magnitude FROM records ORDER BY id",
  "WITH c(a,b) AS (SELECT id,id FROM records WHERE id=1 UNION ALL SELECT id,id FROM records WHERE id=2 ORDER BY 1 DESC) SELECT a,b FROM c ORDER BY a",
  "SELECT category, SUM(n) AS __having_metric_2 FROM records GROUP BY category HAVING COUNT(*)>1 ORDER BY category",

  "SELECT category AS label,SUM(n) AS category FROM records GROUP BY category ORDER BY label",
  "SELECT SUM(n) AS category,category AS label FROM records GROUP BY category HAVING COUNT(*)>1 ORDER BY label",
  "SELECT category AS label,SUM(n) AS category FROM records GROUP BY category HAVING category='b' ORDER BY label",
  "SELECT a.category AS a,b.category AS b,COUNT(*) AS c FROM records a JOIN records b ON a.id=b.id GROUP BY a.category,b.category ORDER BY a,b",
  "SELECT id,n+1 AS n FROM records ORDER BY n,id",
  "SELECT id,n-1 AS a,1-n AS b,n*2 AS c,n/2 AS d,n%2 AS e FROM records WHERE n IS NULL ORDER BY id",
  "SELECT id,n/0 AS a,n%0 AS b FROM records ORDER BY id",
  'SELECT id AS "__proto__" FROM records WHERE id=1',
  'SELECT COUNT(*) AS "__proto__" FROM records',
  'SELECT id AS "constructor",n AS "toString" FROM records ORDER BY id',
  "SELECT id FROM records WHERE id<3 UNION ALL SELECT id FROM records WHERE id<3 ORDER BY id LIMIT 1",
  "SELECT id FROM records WHERE id<3 UNION ALL SELECT id FROM records WHERE id<3 ORDER BY id LIMIT 2 OFFSET 1",
  "SELECT id FROM records WHERE id<3 UNION SELECT id FROM records WHERE id<3 ORDER BY id DESC LIMIT 1",
  "SELECT id FROM records WHERE id<4 EXCEPT SELECT id FROM records WHERE id=2 ORDER BY id DESC LIMIT 1",
  "SELECT id FROM records WHERE id<4 INTERSECT SELECT id FROM records WHERE id>1 ORDER BY id DESC LIMIT 1",
  "SELECT id AS x FROM records WHERE id=1 UNION ALL SELECT id AS y FROM records WHERE id=3 UNION ALL SELECT id AS z FROM records WHERE id=2 ORDER BY z DESC LIMIT 2",
  "WITH c(x) AS (SELECT id FROM records WHERE id<3 UNION ALL SELECT id AS y FROM records WHERE id=3 ORDER BY y DESC LIMIT 2) SELECT x FROM c ORDER BY x",
];

describe.each(["drizzle", "kysely", "objection"] as const)(
  "%s contract audit regressions",
  (backend) => {
    it.each([false, true])("preserves contracts with scan-only=%s", async (local) => {
      const native = new Database(":memory:");
      setup.forEach((s) => native.exec(s));
      const kd = new Kysely<{ records: { id: number; category: string | null; n: number | null } }>(
        { dialect: new SqliteDialect({ database: native }) },
      );
      const odb = knex({
        client: "better-sqlite3",
        connection: { filename: ":memory:" },
        useNullAsDefault: true,
      });
      try {
        for (const s of setup) await odb.raw(s);
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
              ? createKyselyProvider({ db: kd, entities })
              : createObjectionProvider({ knex: odb, entities });
        const can = provider.canExecute.bind(provider);
        if (local)
          provider.canExecute = (rel, ctx) => (rel.kind === "scan" ? can(rel, ctx) : false);
        const builder = createSchemaBuilder();
        let coerces = 0;
        const table = builder.table("records", provider.entities.records, {
          columns: ({ col }) => ({
            id: col.integer("id"),
            category: col.string("category"),
            n: col.integer("n"),
          }),
        });
        builder.table("coerced", provider.entities.records, {
          columns: ({ col }) => ({
            id: col.integer("id"),
            label: col.string("id", {
              coerce: (v) => {
                coerces++;
                return `${String(v)}!`;
              },
            }),
            reverse: col.integer("id", { coerce: (v) => -Number(v) }),
          }),
        });
        builder.view("coerced_view", ({ scan }) => scan(table), {
          columns: ({ col }) => ({
            id: col.integer("id", { coerce: (v) => Number(v) + 10 }),
          }),
        });
        const scan: RelNode = {
          id: "raw",
          kind: "scan",
          convention: "local",
          table: "records",
          entity: provider.entities.records,
          select: ["id"],
          orderBy: [
            { column: "n", direction: "asc" },
            { column: "id", direction: "asc" },
          ],
          limit: 1,
          offset: 1,
          output: [{ name: "id" }],
        };
        builder.view("embedded", () => scan, { columns: ({ col }) => ({ id: col.integer("id") }) });
        const definition = builder.build().unwrap();
        const binding = getNormalizedTableBinding(definition, "coerced");
        if (binding?.kind !== "physical") throw new Error("Expected a physical binding");
        expect(
          mapProviderRowsToLogical([{ id: 7 }], ["label"], binding, definition.tables.coerced),
        ).toEqual([{ label: "7!" }]);
        expect(coerces).toBe(1);
        coerces = 0;
        const schema = createExecutableSchema(builder).unwrap();
        for (const sql of parityCases) {
          const events: QueryStepEvent[] = [];
          const session = createExecutableSchemaSession(schema, {
            sql,
            context: {},
            options: { onEvent: (event) => events.push(event) },
          }).unwrap();
          const actual = await session.runToCompletion();
          while (!("done" in (await session.next()))) {}
          expect(actual, sql).toEqual(native.prepare(sql).all());
          if (sql.includes("__proto__"))
            expect(Object.hasOwn(actual[0]!, "__proto__"), sql).toBe(true);
          if (local)
            expect(
              events.some((event) => event.routeUsed === "provider_fragment"),
              sql,
            ).toBe(false);
        }
        expect(
          (await schema.query({ sql: "SELECT id FROM embedded", context: {} })).unwrap(),
        ).toEqual([{ id: 5 }]);
        expect(
          (
            await schema.query({ sql: "SELECT id FROM coerced ORDER BY id LIMIT 1", context: {} })
          ).unwrap(),
        ).toEqual([{ id: 1 }]);
        expect(coerces).toBe(0);
        expect(
          (
            await schema.query({ sql: "SELECT label FROM coerced WHERE id=2", context: {} })
          ).unwrap(),
        ).toEqual([{ label: "2!" }]);
        expect(coerces).toBe(1);
        expect(
          (
            await schema.query({ sql: "SELECT id FROM coerced WHERE label='3!'", context: {} })
          ).unwrap(),
        ).toEqual([{ id: 3 }]);
        expect(
          (
            await schema.query({
              sql: "SELECT id FROM coerced ORDER BY reverse LIMIT 1",
              context: {},
            })
          ).unwrap(),
        ).toEqual([{ id: 6 }]);
        expect(
          (
            await schema.query({
              sql: "SELECT id FROM coerced_view WHERE id>14 ORDER BY id",
              context: {},
            })
          ).unwrap(),
        ).toEqual([{ id: 15 }, { id: 16 }]);
        for (const sql of [
          "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<3 LIMIT 2) SELECT n FROM c",
          "SELECT category,COUNT(*) AS c,LAG(n) OVER (ORDER BY category) AS p FROM records GROUP BY category",
          "SELECT category,COUNT(*) AS c,LAG(category,1,n+1) OVER (ORDER BY category) AS p FROM records GROUP BY category",
        ]) {
          expect((await schema.query({ sql, context: {} })).isErr(), sql).toBe(true);
        }
        expect(
          (
            await schema.query({
              sql: "SELECT category AS label,COUNT(*) AS category,LAG(label) OVER (ORDER BY label) AS p FROM records GROUP BY category ORDER BY label",
              context: {},
            })
          ).unwrap(),
        ).toEqual([
          { label: null, category: 1, p: null },
          { label: "a", category: 1, p: null },
          { label: "b", category: 2, p: "a" },
          { label: "c", category: 2, p: "b" },
        ]);
      } finally {
        await odb.destroy();
        await kd.destroy();
        if (native.open) native.close();
      }
    });
  },
);
