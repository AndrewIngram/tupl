import { createRequire } from "node:module";
import { writeFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
const root = process.cwd();
const auditOutput =
  process.env.TUPL_AUDIT_OUTPUT_DIR ?? `${root}/docs/audits/2026-09-09-contract-audit`;
const require = createRequire(`${root}/packages/test-support/package.json`);
const Database = require("better-sqlite3");
const knex = require("knex");
const { drizzle } = require("drizzle-orm/better-sqlite3");
const { sqliteTable, integer, text } = require("drizzle-orm/sqlite-core");
const { Kysely, SqliteDialect } = require("kysely");
const { createDrizzleProvider } = await import("@tupl/provider-drizzle");
const { createKyselyProvider } = await import("@tupl/provider-kysely");
const { createObjectionProvider } = await import("@tupl/provider-objection");
const { createExecutableSchema, createSchemaBuilder } = await import("@tupl/schema");
const { createExecutableSchemaSession } = await import("@tupl/runtime/session");
const definitions = { id: integer("id"), category: text("category"), n: integer("n") };
const table = sqliteTable("records", definitions);
const setup = [
  "CREATE TABLE records (id INTEGER, category TEXT, n INTEGER)",
  "INSERT INTO records VALUES (1,'a',9),(2,'b',2),(3,'b',2),(4,NULL,NULL),(5,'c',-6),(6,'c',0)",
];
const queries = [
  "SELECT id,n-1 AS v,n*2 AS w,n/2 AS x,n%2 AS y FROM records WHERE n IS NULL ORDER BY id",
  "SELECT id,n/2 AS v FROM records WHERE id=1",
  "SELECT id,n/0 AS v FROM records WHERE id=1",
  "SELECT category AS label,SUM(n) AS category FROM records GROUP BY category ORDER BY label",
  "SELECT category,COUNT(*) AS c,LAG(n) OVER (ORDER BY category) AS prev FROM records GROUP BY category ORDER BY category",
  "SELECT id FROM records WHERE id < 3 UNION ALL SELECT id FROM records WHERE id < 3 ORDER BY id LIMIT 1",
  "SELECT id FROM records WHERE id < 3 UNION SELECT id FROM records WHERE id < 3 ORDER BY id DESC LIMIT 1",
  "SELECT id FROM records WHERE id < 4 EXCEPT SELECT id FROM records WHERE id=2 ORDER BY id DESC LIMIT 1",
  "SELECT id FROM records WHERE id < 4 INTERSECT SELECT id FROM records WHERE id>1 ORDER BY id DESC LIMIT 1",
  'SELECT id AS "__proto__" FROM records WHERE id=1',
  'SELECT id AS "constructor" FROM records WHERE id=1',

  "SELECT id, n FROM records ORDER BY id",
  "SELECT n AS id, id AS n FROM records ORDER BY id,n",
  "SELECT n AS id, id AS n FROM records WHERE n > 0 ORDER BY id,n",
  "SELECT id, n+1 AS n FROM records ORDER BY n,id",
  "SELECT id FROM records WHERE n IS NULL ORDER BY id",
  "SELECT id FROM records WHERE n NOT IN (2,NULL) ORDER BY id",
  "SELECT id FROM records WHERE n IN (2,NULL) ORDER BY id",
  "SELECT id FROM records WHERE n > 0 OR n IS NULL ORDER BY id LIMIT 2 OFFSET 1",
  "SELECT category,COUNT(*) AS c,SUM(n) AS s FROM records GROUP BY category ORDER BY c,category",
  "SELECT SUM(n) AS category,category AS s,COUNT(*) AS c FROM records GROUP BY category HAVING COUNT(*) > 1 ORDER BY c,s",
  "SELECT COUNT(*) AS c,SUM(n) AS s FROM records WHERE id < 0",
  "SELECT DISTINCT n FROM records ORDER BY n",
  "SELECT id FROM records WHERE id < 3 UNION ALL SELECT id FROM records WHERE id < 3 ORDER BY id",
  "SELECT id FROM records WHERE id < 3 UNION SELECT id FROM records WHERE id < 3 ORDER BY id",
  "SELECT a.id,b.id AS other FROM records a LEFT JOIN records b ON a.n=b.n ORDER BY a.id,b.id",
  "SELECT a.id,b.id AS other FROM records a LEFT JOIN records b ON a.n=b.n WHERE b.n IS NULL ORDER BY a.id,b.id",
  "SELECT a.id FROM records a WHERE EXISTS (SELECT b.id FROM records b WHERE b.n=a.n) ORDER BY a.id",
  "SELECT a.id FROM records a WHERE NOT EXISTS (SELECT b.id FROM records b WHERE b.n=a.n) ORDER BY a.id",
  "WITH c(x,y) AS (SELECT n,id FROM records) SELECT x,y FROM c ORDER BY y",
  "WITH c AS (SELECT id,n FROM records ORDER BY id LIMIT 2) SELECT COUNT(*) AS c FROM c",
  "WITH c AS (SELECT id,n FROM records ORDER BY id LIMIT 2) SELECT id FROM c WHERE n=2 ORDER BY id",
  "SELECT id,ROW_NUMBER() OVER (ORDER BY n,id) AS rn FROM records ORDER BY id",
  "SELECT id,SUM(n) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM records ORDER BY id",
  "SELECT id,LAG(n,1,99) OVER (ORDER BY id) AS p FROM records ORDER BY id",
  "SELECT id FROM records WHERE category = 'x'' OR 1=1 --' ORDER BY id",
];
const report = [];
const plans = [];
const seenPlans = new Set();
const encode = (key, value) =>
  typeof value === "number" && !Number.isFinite(value) ? { nonFiniteNumber: String(value) } : value;
for (const backend of ["drizzle", "kysely", "objection"]) {
  for (const route of ["native", "scan-only", "no-aggregate"]) {
    const native = new Database(":memory:");
    setup.forEach((s) => native.exec(s));
    const kd = new Kysely({ dialect: new SqliteDialect({ database: native }) });
    const odb = knex({
      client: "better-sqlite3",
      connection: { filename: ":memory:" },
      useNullAsDefault: true,
    });
    try {
      for (const s of setup) await odb.raw(s);
      const entities = {
        records: { table: "records", shape: { id: "integer", category: "text", n: "integer" } },
      };
      const provider =
        backend === "drizzle"
          ? createDrizzleProvider({
              dialect: "sqlite",
              db: drizzle(native),
              tables: { records: { table } },
            })
          : backend === "kysely"
            ? createKyselyProvider({ db: kd, entities })
            : createObjectionProvider({ knex: odb, entities });
      const can = provider.canExecute.bind(provider);
      const contains = (rel, kind) =>
        rel?.kind === kind ||
        ["input", "left", "right", "body"].some((k) => rel[k] && contains(rel[k], kind)) ||
        (rel.ctes ?? []).some((c) => contains(c.query, kind));
      provider.canExecute = (rel, context) =>
        route === "scan-only" && rel.kind !== "scan"
          ? false
          : route === "no-aggregate" && contains(rel, "aggregate")
            ? false
            : can(rel, context);
      const calls = [];
      const execute = provider.execute.bind(provider);
      provider.execute = (plan, ctx) => {
        calls.push({ plan, ctx });
        return execute(plan, ctx);
      };
      const builder = createSchemaBuilder();
      builder.table("records", provider.entities.records, {
        columns: ({ col, derive }) => ({
          id: col.integer("id"),
          category: col.string("category"),
          n: col.integer("n"),
          d: col.integer(derive({ n: col.integer("n") }, ({ n }) => (n == null ? null : n * 2))),
        }),
      });
      builder.table("coerced", provider.entities.records, {
        columns: ({ col }) => ({ id: col.string("id", { coerce: (v) => `${v}!` }) }),
      });
      const scan = {
        id: "raw",
        kind: "scan",
        convention: "local",
        table: "records",
        entity: provider.entities.records,
        select: ["id"],
        output: [{ name: "id" }],
        orderBy: [{ column: "id", direction: "asc" }],
        limit: 1,
        offset: 1,
      };
      builder.view("embedded", () => scan, { columns: ({ col }) => ({ id: col.integer("id") }) });
      const schema = createExecutableSchema(builder).unwrap();
      if (route === "native") {
        const rel = {
          id: "mod",
          kind: "project",
          convention: "local",
          input: {
            ...scan,
            orderBy: undefined,
            limit: undefined,
            offset: undefined,
            select: ["n"],
            output: [{ name: "n" }],
          },
          columns: [
            {
              kind: "expr",
              expr: {
                kind: "function",
                name: "mod",
                args: [
                  { kind: "column", ref: { column: "n" } },
                  { kind: "literal", value: 2 },
                ],
              },
              output: "v",
            },
          ],
          output: [{ name: "v" }],
        };
        const supported = await provider.canExecute(rel, {});
        const compiled = await provider.compile(rel, {});
        let result;
        try {
          result = compiled.isErr()
            ? { error: compiled.error.message }
            : await provider.execute(compiled.unwrap(), {});
        } catch (e) {
          result = { error: e.message };
        }
        plans.push({
          backend,
          case: "direct modulo capability",
          supported,
          compileOk: compiled.isOk(),
          result: result?.isErr?.()
            ? { error: result.error.message }
            : result?.isOk?.()
              ? result.unwrap()
              : result,
        });
      }
      const cases = [
        ...queries.map((sql) => ({ sql, expected: native.prepare(sql).all() })),
        {
          sql: "SELECT id,d FROM records WHERE n>0 ORDER BY id LIMIT 2",
          expected: [
            { id: 1, d: 18 },
            { id: 2, d: 4 },
          ],
        },
        { sql: "SELECT id FROM records WHERE d>4 ORDER BY id LIMIT 1", expected: [{ id: 1 }] },
        { sql: "SELECT id FROM embedded", expected: [{ id: 2 }], known: true },
        {
          sql: "SELECT id FROM coerced ORDER BY id",
          expected: [1, 2, 3, 4, 5, 6].map((id) => ({ id: `${id}!` })),
          known: true,
        },
      ];
      for (const c of cases) {
        calls.length = 0;
        const events = [];
        let actual, error;
        try {
          const session = createExecutableSchemaSession(schema, {
            sql: c.sql,
            context: {},
            options: { onEvent: (e) => events.push(e) },
          }).unwrap();
          actual = await session.runToCompletion();
          while (!("done" in (await session.next()))) {}
        } catch (e) {
          error = { tag: e._tag, message: e.message };
        }
        const entry = {
          backend,
          route,
          ...c,
          actual,
          error,
          pass: !error && isDeepStrictEqual(actual, c.expected),
          executions: events.map(({ kind, routeUsed, status }) => ({ kind, routeUsed, status })),
          providerCalls: calls.length,
        };
        report.push(entry);
        if (!entry.pass) {
          try {
            const plan = (await schema.explain({ sql: c.sql, context: {} })).unwrap();
            if (!seenPlans.has(c.sql)) {
              seenPlans.add(c.sql);
              plans.push({
                backend,
                route,
                sql: c.sql,
                plan: JSON.parse(
                  JSON.stringify(plan, (key, value) =>
                    ["entity", "providerInstance"].includes(key) ? undefined : value,
                  ),
                ),
              });
            }
          } catch (e) {
            entry.explainError = e.message;
          }
          console.log(JSON.stringify({ ...entry, plan: undefined }));
        }
      }
    } finally {
      await odb.destroy();
      await kd.destroy();
      if (native.open) native.close();
    }
  }
}
writeFileSync(
  `${auditOutput}/results.ndjson`,
  report.map((entry) => JSON.stringify(entry, encode)).join("\n") + "\n",
);
writeFileSync(
  `${auditOutput}/plans.ndjson`,
  plans.map((entry) => JSON.stringify(entry, encode)).join("\n") + "\n",
);
console.log(
  JSON.stringify({
    total: report.length,
    passed: report.filter((r) => r.pass).length,
    failed: report.filter((r) => !r.pass).length,
  }),
);
