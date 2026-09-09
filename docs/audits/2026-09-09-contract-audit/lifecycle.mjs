import { createRequire } from "node:module";
import { writeFileSync } from "node:fs";
import assert from "node:assert/strict";
const root = process.cwd();
const auditOutput =
  process.env.TUPL_AUDIT_OUTPUT_DIR ?? `${root}/docs/audits/2026-09-09-contract-audit`;
const require = createRequire(root + "/packages/test-support/package.json");
const knex = require("knex");
const { Result } = await import("better-result");
const { createObjectionProvider } = await import("@tupl/provider-objection");
const { createExecutableSchema, createSchemaBuilder } = await import("@tupl/schema");
const db = knex({
  client: "better-sqlite3",
  connection: { filename: ":memory:" },
  useNullAsDefault: true,
});
const results = [];
try {
  await db.schema.createTable("items", (t) => {
    t.integer("id");
    t.text("tenant");
    t.integer("n");
  });
  await db("items").insert([
    { id: 1, tenant: "a", n: 5 },
    { id: 2, tenant: "b", n: 7 },
  ]);
  const provider = createObjectionProvider({
    knex: () => db,
    entities: {
      items: {
        table: "items",
        shape: { id: "integer", n: "integer" },
        base: (ctx) => db("items").where("tenant", ctx.tenant),
      },
    },
  });
  const execute = provider.execute.bind(provider);
  const reached = {};
  const release = {};
  provider.execute = async (plan, ctx) => {
    if (ctx.pause) {
      reached[ctx.tenant]?.();
      await new Promise((resolve) => (release[ctx.tenant] = resolve));
    }
    return ctx.fail ? Result.err(new Error("deliberate failure")) : execute(plan, ctx);
  };
  let count = 0;
  const builder = createSchemaBuilder();
  builder.table("items", provider.entities.items, {
    columns: ({ col, derive }) => ({
      id: col.integer("id"),
      n: col.integer("n"),
      d: col.integer(
        derive({ n: col.integer("n") }, ({ n }) => {
          count++;
          return n * 2;
        }),
      ),
    }),
  });
  const schema = createExecutableSchema(builder).unwrap();
  const query = (ctx) => schema.query({ sql: "SELECT id,d FROM items ORDER BY id", context: ctx });
  const atA = new Promise((r) => (reached.a = r));
  const atB = new Promise((r) => (reached.b = r));
  const a = query({ tenant: "a", pause: true, fail: true });
  await atA;
  const b = query({ tenant: "b", pause: true });
  await atB;
  release.b();
  assert.deepEqual((await b).unwrap(), [{ id: 2, d: 14 }]);
  release.a();
  assert.equal((await a).isErr(), true);
  assert.deepEqual((await query({ tenant: "a" })).unwrap(), [{ id: 1, d: 10 }]);
  assert.deepEqual((await query({ tenant: "b" })).unwrap(), [{ id: 2, d: 14 }]);
  assert.equal(count, 3);
  results.push({
    case: "overlapping scoped contexts, one failure, later reuse, no shared derivations",
    pass: true,
    derivations: count,
  });
  for (const tenant of ["a", "b", "a' OR 1=1 --"]) {
    const expected = tenant === "a" ? 1 : tenant === "b" ? 2 : null;
    for (const sql of [
      "SELECT id FROM items",
      "SELECT MAX(id) AS id FROM items",
      "WITH c AS (SELECT id FROM items) SELECT id FROM c",
      "SELECT a.id FROM items a JOIN items b ON a.id=b.id",
    ]) {
      const rows = (await schema.query({ sql, context: { tenant } })).unwrap();
      assert.deepEqual(
        rows,
        expected === null ? (sql.includes("MAX") ? [{ id: null }] : []) : [{ id: expected }],
      );
      results.push({ case: sql, tenant, pass: true });
    }
  }
  assert.equal(
    (await schema.query({ sql: "SELECT tenant FROM items", context: { tenant: "a" } })).isErr(),
    true,
  );
  results.push({ case: "undeclared tenant field rejected", pass: true });
} finally {
  await db.destroy();
}
writeFileSync(`${auditOutput}/lifecycle-results.json`, JSON.stringify(results, null, 2) + "\n");
console.log(JSON.stringify({ checks: results.length, passed: results.length }));
