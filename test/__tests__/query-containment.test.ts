import { fc, test as propertyTest } from "@fast-check/vitest";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";
import { createContainmentFixture } from "../support/containment";

const predicates = [
  "NOT (1 BETWEEN 2 AND NULL)",
  "NOT (value BETWEEN 2 AND NULL)",
  "NOT (label LIKE 'one')",
  "NOT (value = NULL)",
  "NOT (value <> NULL)",
  "1=1",
  "1=0",
  "NULL",
  "org=2",
  "org<>1",
  "NOT (org=1)",
  "org=2 OR 1=1",
  "org=1 OR org=2",
  "org=1 AND 1=0 OR 1=1",
  "NOT (org=1 AND 1=0)",
  "NOT (org=1 OR 1=0)",
  "(id=3 OR id=99) OR NOT (id=3 OR id=99)",
  "NULL OR 1=1",
  "NULL AND 1=1",
  "NOT NULL",
  "id NOT IN (1,NULL)",
  "id IN (1,2,3,99)",
  "id NOT IN (1,2)",
  "id IN (3,NULL) OR 1=1",
  "id NOT IN (3,NULL) OR 1=1",
  "org IS NULL OR org IS NOT NULL",
  "org IS DISTINCT FROM 1",
  "org=2 OR label='x'' OR 1=1 --'",
];
const atoms = ["org=1", "org=2", "id=3", "NULL"];
for (const left of atoms)
  for (const right of atoms) {
    predicates.push(`(${left}) OR (${right})`, `NOT ((${left}) AND (${right}))`);
  }
const queries = predicates.flatMap((predicate) => [
  `SELECT id, org FROM accounts WHERE ${predicate}`,
  `SELECT COUNT(*) AS n FROM accounts WHERE ${predicate}`,
]);
queries.push(
  "SELECT a.id FROM accounts a WHERE EXISTS (SELECT d.label FROM details d WHERE d.id=a.id)",
  "SELECT a.id FROM accounts a WHERE NOT EXISTS (SELECT d.label FROM details d WHERE d.id=a.id)",
  "SELECT a.id AS a_id, b.id AS b_id FROM accounts a JOIN accounts b ON a.id=b.id",
  "SELECT details.id FROM accounts AS details WHERE details.org=2 OR 1=1",
  "SELECT a.id, a.org FROM accounts a LEFT JOIN details d ON a.id=d.id WHERE a.org=2 OR 1=1",
  "SELECT id, org FROM accounts WHERE org=2 OR 1=1 UNION ALL SELECT id, org FROM details WHERE org<>1 OR 1=1",
  "WITH c AS (SELECT id, org FROM accounts WHERE org=2 OR 1=1) SELECT id, org FROM c WHERE NOT (org=2)",
  "SELECT id, org FROM accounts WHERE EXISTS (SELECT id FROM details WHERE org=2 OR 1=1)",
);
queries.push(
  "SELECT * FROM accounts",
  "SELECT a.* FROM accounts a WHERE EXISTS (SELECT * FROM details d WHERE d.id=a.id)",
  "SELECT a.*, 1 AS extra FROM accounts a",
  "SELECT * FROM (SELECT id, label FROM accounts) a",
  "WITH a AS (SELECT * FROM accounts) SELECT a.* FROM a",
  "SELECT id, org, label, value FROM accounts",
  "SELECT id FROM accounts ORDER BY id DESC LIMIT 1 OFFSET 1",
  "SELECT COUNT(*) AS n, SUM(value) AS total FROM accounts",
  "SELECT label, COUNT(*) AS n FROM accounts GROUP BY label HAVING COUNT(*) > 0",
  "SELECT DISTINCT value FROM accounts",
  ...["INNER", "LEFT", "RIGHT", "FULL"].map(
    (join) =>
      `SELECT a.id AS a_id, d.id AS d_id FROM accounts a ${join} JOIN details d ON a.id=d.id`,
  ),
  "SELECT a.id FROM accounts a LEFT JOIN details d ON a.id=d.id WHERE d.id IS NULL",
  "SELECT a.id FROM accounts a WHERE EXISTS (SELECT d.id FROM details d WHERE d.id=a.id)",
  "SELECT a.id FROM accounts a WHERE NOT EXISTS (SELECT d.id FROM details d WHERE d.id=a.id)",
  "SELECT id FROM accounts WHERE id IN (SELECT id FROM details)",
  "SELECT id FROM accounts WHERE id NOT IN (SELECT id FROM details)",
  "SELECT id FROM accounts WHERE value NOT IN (SELECT value FROM details)",
  "SELECT id FROM accounts WHERE value NOT IN (SELECT value FROM details WHERE 1=0)",
  "SELECT (SELECT COUNT(*) FROM accounts) AS n",
  ...["UNION ALL", "UNION", "INTERSECT", "EXCEPT"].map(
    (op) => `SELECT id FROM accounts ${op} SELECT id FROM details`,
  ),
  "WITH c AS (SELECT id, org FROM accounts), d AS (SELECT id, org FROM c) SELECT id, org FROM d",
  "SELECT id, org FROM (SELECT id, org FROM accounts) a",
  "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM accounts",
);
const rejected = [
  "SELECT a.* FROM accounts a WHERE EXISTS (SELECT * FROM details d WHERE d.secret=a.id)",
  "SELECT * FROM accounts a JOIN details d ON a.id=d.id",
  "SELECT a.*, d.id FROM accounts a JOIN details d ON a.id=d.id",
  "SELECT missing.* FROM accounts",
  "SELECT a.id FROM accounts a WHERE EXISTS (SELECT d.id FROM details d WHERE d.id=a.secret)",
  "SELECT a.id FROM accounts a WHERE EXISTS (SELECT d.id FROM details d WHERE d.secret=a.id)",
  "SELECT (SELECT MAX(secret) FROM details) AS hidden FROM accounts",

  "SELECT id FROM accounts_raw",
  "SELECT id FROM details_raw",
  "SELECT id FROM constructor",
  "SELECT id FROM __proto__",
  "WITH c AS (SELECT id FROM accounts_raw) SELECT id FROM c",
  "SELECT id FROM accounts WHERE EXISTS (SELECT id FROM accounts_raw)",
  "SELECT * FROM vault",
  "SELECT * FROM sqlite_master",
  "SELECT * FROM information_schema.tables",
  ...["secret", "constructor", "__proto__"].flatMap((column) => [
    `SELECT ${column} FROM accounts`,
    `SELECT id FROM accounts WHERE ${column} IS NOT NULL`,
    `SELECT id FROM accounts ORDER BY ${column}`,
    `SELECT COUNT(${column}) FROM accounts`,
    `SELECT id FROM accounts GROUP BY ${column}`,
    `SELECT id FROM accounts HAVING ${column} IS NOT NULL`,
    `SELECT ROW_NUMBER() OVER (ORDER BY ${column}) FROM accounts`,
    `SELECT a.id FROM accounts a JOIN details d ON a.${column}=d.id`,
    `WITH c AS (SELECT ${column} FROM accounts) SELECT * FROM c`,
    `SELECT id FROM accounts UNION SELECT ${column} FROM accounts`,
    `SELECT id FROM accounts WHERE EXISTS (SELECT id FROM details WHERE ${column} IS NOT NULL)`,
    `SELECT id FROM accounts WHERE EXISTS (SELECT ${column} FROM details)`,
  ]),
  "SELECT (SELECT secret FROM vault) FROM accounts",
  "SELECT * FROM accounts; SELECT * FROM vault",
  "DELETE FROM accounts",
  "UPDATE accounts SET value=0",
  "PRAGMA database_list",
  "ATTACH DATABASE ':memory:' AS escaped",
  "SELECT load_extension('escape')",
  "SELECT readfile('/etc/passwd')",
  "SELECT pg_read_file('/etc/passwd')",
];
function normalize(rows: unknown[]) {
  return rows.map((row) => JSON.stringify(row, Object.keys(row as object).sort())).sort();
}
const atomSql = fc.constantFrom(
  "org=1",
  "org=2",
  "id=1",
  "id=99",
  "value=10",
  "value IS NULL",
  "NULL",
  "1=1",
  "1=0",
  "id IN (1, NULL)",
  "id NOT IN (1, NULL)",
);
type Predicate =
  | { kind: "atom"; sql: string }
  | { kind: "not"; input: Predicate }
  | { kind: "binary"; op: "AND" | "OR"; left: Predicate; right: Predicate };
const atom = atomSql.map((sql) => ({ kind: "atom" as const, sql }));
function predicate(depth: number): fc.Arbitrary<Predicate> {
  if (depth === 0) return atom;
  const child = predicate(depth - 1);
  return fc.oneof(
    atom,
    child.map((input) => ({ kind: "not" as const, input })),
    fc
      .tuple(child, fc.constantFrom("AND" as const, "OR" as const), child)
      .map(([left, op, right]) => ({ kind: "binary" as const, op, left, right })),
  );
}
function renderPredicate(expr: Predicate): string {
  switch (expr.kind) {
    case "atom":
      return expr.sql;
    case "not":
      return `NOT (${renderPredicate(expr.input)})`;
    case "binary":
      return `(${renderPredicate(expr.left)}) ${expr.op} (${renderPredicate(expr.right)})`;
  }
}
for (const dialect of ["sqlite", "postgres"] as const)
  for (const provider of ["drizzle", "kysely", "objection"] as const)
    for (const local of [false, true])
      describe(`${dialect} ${provider} ${local ? "local" : "pushdown"} scope containment`, () => {
        let fixture: Awaited<ReturnType<typeof createContainmentFixture>>;
        beforeAll(async () => {
          fixture = await createContainmentFixture(provider, local, dialect);
          await fixture.mutateHidden(1);
        });
        afterAll(async () => {
          await fixture?.close();
        });
        async function assertQuery(sql: string) {
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isOk(), result.isErr() ? result.error.message : sql).toBe(true);
          const actual = normalize(result.unwrap());
          expect(actual, sql).toEqual(normalize(await fixture.reference(sql)));
          return actual;
        }
        it.each(queries)("matches authorized-only SQL: %s", async (sql) => {
          await assertQuery(sql);
        });
        it.each(rejected)("rejects access outside the declared schema: %s", async (sql) => {
          fixture.statements.length = 0;
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isErr(), sql).toBe(true);
          expect(fixture.statements).toEqual([]);
        });
        it("re-resolves scopes for lookup execution and fails closed on scope errors", async () => {
          const request = {
            table: "accounts",
            key: "id",
            keys: [0, 1, 2, 5, 99],
            select: ["id", "org"],
          };
          expect((await fixture.provider.lookupMany(request, { org: 1 })).unwrap()).toEqual([
            { id: 1, org: 1 },
            { id: 2, org: 1 },
            { id: 5, org: 1 },
          ]);
          expect(
            (await fixture.provider.lookupMany({ ...request, keys: [1] }, { org: 2 })).unwrap(),
          ).toEqual([{ id: 1, org: 2 }]);
          expect(
            (
              await fixture.schema.query({
                sql: "SELECT id FROM accounts WHERE org=1 OR 1=1",
                context: { org: 3 },
              })
            ).unwrap(),
          ).toEqual([]);
          fixture.statements.length = 0;
          expect((await fixture.provider.lookupMany(request, { org: 1, deny: true })).isErr()).toBe(
            true,
          );
          expect(
            (
              await fixture.schema.query({
                sql: "SELECT id FROM accounts",
                context: { org: 1, deny: true },
              })
            ).isErr(),
          ).toBe(true);
          expect(fixture.statements).toEqual([]);
        });
        propertyTest.prop(
          {
            predicate: predicate(3),
            variant: fc.integer({ min: 2, max: 50 }),
            shape: fc.constantFrom("scan", "count", "cte", "exists", "union", "window"),
          },
          { numRuns: 100 },
        )(
          "generated Boolean queries ignore hidden-row mutations",
          async ({ predicate, variant, shape }) => {
            const where = renderPredicate(predicate);
            const scan = `SELECT id, org FROM accounts WHERE ${where}`;
            const sql =
              shape === "count"
                ? `SELECT COUNT(*) AS n FROM accounts WHERE ${where}`
                : shape === "cte"
                  ? `WITH c AS (${scan}) SELECT id, org FROM c`
                  : shape === "exists"
                    ? `SELECT id, org FROM accounts WHERE EXISTS (SELECT id FROM details WHERE ${where})`
                    : shape === "union"
                      ? `${scan} UNION ALL SELECT id, org FROM details WHERE ${where}`
                      : shape === "window"
                        ? `SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM accounts WHERE ${where}`
                        : scan;
            await fixture.mutateHidden(0);
            const before = await assertQuery(sql);
            await fixture.mutateHidden(variant);
            expect(await assertQuery(sql)).toEqual(before);
          },
        );
      });
