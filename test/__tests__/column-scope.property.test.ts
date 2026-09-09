import { fc, test as propertyTest } from "@fast-check/vitest";
import { afterAll, beforeAll, describe, expect, it } from "vite-plus/test";
import { createContainmentFixture } from "../support/containment";

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const generatedName = fc
  .array(fc.constantFrom("a", "b", "x", "y"), { minLength: 1, maxLength: 5 })
  .map((parts) => `alias_${parts.join("")}`);
const aliasName = fc.oneof(
  generatedName,
  fc.constantFrom("constructor", "__proto__", "toString", "a.b", 'a"b'),
);
const missingName = fc.oneof(
  generatedName,
  fc.constantFrom("secret", "value", "constructor", "__proto__"),
);

function relation(depth: number, derived: boolean) {
  let source = "SELECT id,org FROM accounts";
  if (derived) {
    for (let level = 0; level < depth; level++) source = `SELECT * FROM (${source}) q${level}`;
    return { prefix: "", from: `(${source}) exposed` };
  }
  const ctes = [`q0 AS (${source})`];
  for (let level = 1; level <= depth; level++)
    ctes.push(`q${level} AS (SELECT * FROM q${level - 1})`);
  return { prefix: `WITH ${ctes.join(",")} `, from: `q${depth} exposed` };
}

function missingColumnQuery(prefix: string, from: string, name: string, clause: number) {
  const ref = `exposed.${quote(name)}`;
  const queries = [
    `SELECT ${ref} FROM ${from}`,
    `SELECT id FROM ${from} WHERE ${ref} IS NULL`,
    `SELECT COUNT(${ref}) AS n FROM ${from}`,
    `SELECT ${ref},COUNT(*) AS n FROM ${from} GROUP BY ${ref}`,
    `SELECT id FROM ${from} ORDER BY ${ref}`,
    `SELECT id,LAG(${ref}) OVER (ORDER BY id) AS previous FROM ${from}`,
    `SELECT id,LAG(id,1,${ref}) OVER (ORDER BY id) AS previous FROM ${from}`,
    `SELECT id FROM accounts WHERE EXISTS (SELECT ${ref} FROM ${from})`,
    `SELECT id FROM ${from} UNION ALL SELECT ${ref} FROM ${from}`,
    `SELECT id,COUNT(*) AS n FROM ${from} GROUP BY id HAVING ${ref} IS NULL`,
    `SELECT exposed.id FROM ${from} JOIN details d ON ${ref}=d.id`,
    `SELECT id,COUNT(*) OVER (PARTITION BY ${ref}) AS n FROM ${from}`,
    `SELECT id,COUNT(*) AS n FROM ${from} GROUP BY id ORDER BY ${ref}`,
    `SELECT id,COUNT(*) AS n FROM ${from} GROUP BY id ORDER BY ${quote(name)}`,
    `SELECT id,COUNT(*) AS n,LAG(${ref}) OVER (ORDER BY id) AS p FROM ${from} GROUP BY id`,
    `SELECT id,COUNT(*) AS ${quote(name)} FROM ${from} GROUP BY id ORDER BY ${ref}`,
    `SELECT id,COUNT(*) AS ${quote(name)} FROM ${from} GROUP BY id HAVING ${ref} IS NULL`,
  ];
  return prefix + queries[clause]!;
}

for (const dialect of ["sqlite", "postgres"] as const)
  for (const provider of ["drizzle", "kysely", "objection"] as const)
    for (const local of [false, true])
      describe(`${dialect} ${provider} scan-only=${local} column scope`, () => {
        let fixture: Awaited<ReturnType<typeof createContainmentFixture>>;
        beforeAll(async () => {
          fixture = await createContainmentFixture(provider, local, dialect);
          await fixture.mutateHidden(9);
        });
        afterAll(async () => {
          await fixture?.close();
        });

        it("rejects ambiguous HAVING inputs before choosing grouped columns", async () => {
          const source = "FROM accounts a JOIN accounts b ON a.id=b.id GROUP BY a.id";
          const sql = `SELECT a.id,COUNT(*) AS n ${source} HAVING id>0 ORDER BY a.id`;
          fixture.statements.length = 0;
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isErr(), sql).toBe(true);
          expect(fixture.statements).toEqual([]);
          const qualified = sql.replace("HAVING id", "HAVING a.id");
          const control = await fixture.schema.query({ sql: qualified, context: { org: 1 } });
          expect(control.unwrap()).toEqual(await fixture.reference(qualified));
          const unique =
            "SELECT a.id,COUNT(*) AS n FROM accounts a JOIN (SELECT id AS other_id FROM accounts) b ON a.id=b.other_id GROUP BY a.id HAVING id>0 ORDER BY a.id";
          const uniqueResult = await fixture.schema.query({ sql: unique, context: { org: 1 } });
          expect(uniqueResult.unwrap()).toEqual(await fixture.reference(unique));
        });

        propertyTest.prop(
          { name: generatedName, threshold: fc.integer({ min: 0, max: 6 }) },
          { numRuns: 30 },
        )("checks HAVING ambiguity across all joined inputs", async ({ name, threshold }) => {
          const column = quote(name);
          const source = `(SELECT id AS ${column} FROM accounts)`;
          const prefix = `SELECT a.${column} AS id,COUNT(*) AS n FROM ${source} a JOIN ${source} b ON a.${column}=b.${column} GROUP BY a.${column}`;
          const sql = `${prefix} HAVING ${column}>${threshold} ORDER BY a.${column}`;
          fixture.statements.length = 0;
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isErr(), sql).toBe(true);
          expect(fixture.statements).toEqual([]);
          const qualified = `${prefix} HAVING a.${column}>${threshold} ORDER BY a.${column}`;
          const control = await fixture.schema.query({ sql: qualified, context: { org: 1 } });
          expect(control.unwrap()).toEqual(await fixture.reference(qualified));
        });

        propertyTest.prop(
          {
            depth: fc.integer({ min: 0, max: 3 }),
            derived: fc.boolean(),
            name: missingName,
            clause: fc.integer({ min: 0, max: 16 }),
          },
          { numRuns: 80 },
        )(
          "rejects missing columns in every consumer before executing SQL",
          async ({ depth, derived, name, clause }) => {
            const { prefix, from } = relation(depth, derived);
            const sql = missingColumnQuery(prefix, from, name, clause);
            fixture.statements.length = 0;
            const result = await fixture.schema.query({ sql, context: { org: 1 } });
            expect(result.isErr(), sql).toBe(true);
            expect(fixture.statements, sql).toEqual([]);
          },
        );

        propertyTest.prop(
          {
            depth: fc.integer({ min: 0, max: 2 }),
            derived: fc.boolean(),
            groupAlias: aliasName,
            metricAlias: aliasName,
            threshold: fc.integer({ min: 0, max: 6 }),
          },
          { numRuns: 60 },
        )(
          "preserves grouped values and HAVING semantics under output renaming",
          async ({ depth, derived, groupAlias, metricAlias, threshold }) => {
            fc.pre(groupAlias !== metricAlias);
            const { prefix, from } = relation(depth, derived);
            const projection = `exposed.id AS ${quote(groupAlias)},COUNT(*) AS ${quote(metricAlias)}`;
            const sql = `${prefix}SELECT ${projection} FROM ${from} GROUP BY exposed.id HAVING ${quote(groupAlias)}>=${threshold} AND ${quote(metricAlias)}>0 ORDER BY ${quote(groupAlias)}`;
            // PostgreSQL does not accept SELECT aliases in HAVING; use equivalent source expressions.
            const oracle = `${prefix}SELECT ${projection} FROM ${from} GROUP BY exposed.id HAVING exposed.id>=${threshold} AND COUNT(*)>0 ORDER BY ${quote(groupAlias)}`;
            const expected = await fixture.reference(oracle);
            const result = await fixture.schema.query({ sql, context: { org: 1 } });
            expect(result.isOk(), result.isErr() ? `${sql}: ${result.error.message}` : sql).toBe(
              true,
            );
            expect(result.unwrap(), sql).toEqual(expected);
          },
        );

        propertyTest.prop(
          { name: aliasName, recursive: fc.boolean(), threshold: fc.integer({ min: 0, max: 5 }) },
          { numRuns: 40 },
        )(
          "preserves values through declared CTE output renaming",
          async ({ name, recursive, threshold }) => {
            const column = quote(name);
            const definition = recursive
              ? `WITH RECURSIVE renamed(${column}) AS (SELECT id FROM accounts WHERE id=1 UNION ALL SELECT ${column}+1 FROM renamed WHERE ${column}<3)`
              : `WITH renamed(${column}) AS (SELECT id FROM accounts), next AS (SELECT ${column} FROM renamed)`;
            const source = recursive ? "renamed" : "next";
            const sql = `${definition} SELECT ${column} AS id FROM ${source} WHERE ${column}>${threshold} ORDER BY ${column}`;
            const expected = await fixture.reference(sql);
            const result = await fixture.schema.query({ sql, context: { org: 1 } });
            expect(result.isOk(), result.isErr() ? `${sql}: ${result.error.message}` : sql).toBe(
              true,
            );
            expect(result.unwrap(), sql).toEqual(expected);
          },
        );

        it.each([
          'SELECT id AS "a""b" FROM accounts ORDER BY "a""b"',
          'SELECT org,COUNT(*) AS "a""b" FROM accounts GROUP BY org ORDER BY "a""b"',
          'SELECT id,ROW_NUMBER() OVER (ORDER BY id) AS "a""b" FROM accounts ORDER BY id',
          'SELECT id AS "a""b" FROM accounts UNION ALL SELECT id FROM details ORDER BY "a""b"',
        ])("preserves quotes at SQL identifier emission: %s", async (sql) => {
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isOk(), result.isErr() ? result.error.message : sql).toBe(true);
          expect(result.unwrap(), sql).toEqual(await fixture.reference(sql));
        });

        it("keeps crafted SQL aliases inside identifiers", async () => {
          const predicate =
            dialect === "sqlite" ? "?=?" : "CAST($2 AS integer)=CAST($3 AS integer)";
          const alias = `x" FROM vault WHERE ${predicate} UNION ALL SELECT secret FROM vault --`;
          const sql = `SELECT 'public' AS ${quote(alias)} FROM accounts`;
          // PostgreSQL truncates long physical aliases; the public result retains the requested name.
          const expected = (await fixture.reference("SELECT 'public' AS result FROM accounts")).map(
            () => Object.fromEntries([[alias, "public"]]),
          );
          for (const variant of [9, 27]) {
            await fixture.mutateHidden(variant);
            const result = await fixture.schema.query({ sql, context: { org: 1 } });
            expect(result.isOk(), result.isErr() ? result.error.message : sql).toBe(true);
            expect(result.unwrap(), sql).toEqual(expected);
          }
        });

        it.each([
          "WITH q0 AS (SELECT id,org FROM accounts) SELECT id,COUNT(*) AS n FROM q0 exposed GROUP BY id ORDER BY exposed.secret",
          "SELECT id,COUNT(*) AS n FROM accounts GROUP BY id ORDER BY secret",
          "SELECT id,COUNT(*) AS secret FROM accounts GROUP BY id ORDER BY accounts.secret",
          "WITH c AS (SELECT id FROM accounts) SELECT secret FROM c",
          "WITH RECURSIVE c(id) AS (SELECT id FROM accounts WHERE id=1 UNION ALL SELECT id+1 FROM c WHERE id<2) SELECT secret FROM c",
          "WITH c AS (SELECT id FROM accounts),d AS (SELECT id FROM c) SELECT id FROM d WHERE EXISTS (SELECT missing FROM c)",
          "SELECT id,COUNT(*) AS org FROM accounts GROUP BY id HAVING org>0",
          "SELECT id AS org,COUNT(*) AS n FROM accounts GROUP BY id HAVING org>0",
          "SELECT id,COUNT(*) AS n FROM accounts GROUP BY id HAVING secret IS NULL",
          "SELECT id,COUNT(*) AS n FROM accounts GROUP BY id HAVING accounts.secret IS NULL",
        ])("rejects audited reference: %s", async (sql) => {
          fixture.statements.length = 0;
          const result = await fixture.schema.query({ sql, context: { org: 1 } });
          expect(result.isErr(), sql).toBe(true);
          expect(fixture.statements).toEqual([]);
        });
      });
