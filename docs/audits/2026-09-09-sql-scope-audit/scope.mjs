import { createContainmentFixture } from "../../../test/support/containment.ts";
import { writeFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
const valid = [
  "WITH c AS (SELECT id FROM accounts WHERE id=1) SELECT id FROM c UNION ALL SELECT id FROM (WITH c AS (SELECT id FROM details WHERE id=4) SELECT id FROM c) x",
  "WITH accounts AS (SELECT id FROM details) SELECT id FROM accounts ORDER BY id",
  "WITH accounts_raw AS (SELECT id FROM accounts) SELECT id FROM accounts_raw ORDER BY id",
  "WITH c AS (SELECT id FROM accounts WHERE id=1) SELECT a.id AS x,b.id AS y FROM c a JOIN (WITH c AS (SELECT id FROM details WHERE id=4) SELECT id FROM c) b ON 1=1",
  "SELECT id AS secret FROM accounts UNION ALL SELECT id AS other FROM details ORDER BY secret LIMIT 2",
  "SELECT id AS secret FROM accounts ORDER BY secret LIMIT 2",
  "SELECT id AS constructor FROM accounts ORDER BY constructor",
  'SELECT id AS "__proto__" FROM accounts UNION ALL SELECT id FROM details ORDER BY "__proto__" LIMIT 2',
  "WITH RECURSIVE c(id) AS (SELECT id FROM accounts WHERE id=1 UNION ALL SELECT c.id+1 FROM c WHERE c.id<3) SELECT c.id,a.org FROM c LEFT JOIN accounts a ON c.id=a.id ORDER BY c.id",
  "SELECT id FROM accounts WHERE EXISTS (WITH c AS (SELECT id FROM details) SELECT id FROM c WHERE c.id=accounts.id)",
  "SELECT id FROM accounts UNION ALL SELECT id FROM (SELECT * FROM details WHERE org=2 OR 1=1) d ORDER BY id LIMIT 2 OFFSET 2",
  "WITH c(id,org) AS (SELECT id,org FROM accounts UNION ALL SELECT id,org FROM details ORDER BY details.id LIMIT 3) SELECT * FROM c ORDER BY id",
  "SELECT a.id AS x,d.id AS y FROM accounts a FULL JOIN details d ON a.id=d.id WHERE a.id IS NULL OR d.id IS NULL ORDER BY x,y",
];
const invalid = [
  "SELECT id AS secret FROM accounts WHERE secret IS NOT NULL",
  "SELECT id AS secret FROM accounts ORDER BY accounts.secret",
  "SELECT id AS secret FROM accounts UNION ALL SELECT id FROM details ORDER BY accounts.secret",
  "WITH c AS (SELECT id FROM accounts) SELECT secret FROM c",
  "WITH accounts AS (SELECT id FROM details) SELECT secret FROM accounts",
  "SELECT COUNT(*) AS secret,LAG(accounts.secret) OVER (ORDER BY accounts.id) FROM accounts GROUP BY accounts.id",
  "SELECT id FROM accounts WHERE EXISTS (SELECT id FROM details WHERE accounts.secret IS NOT NULL)",
  "SELECT id FROM accounts WHERE EXISTS (WITH accounts AS (SELECT id FROM details) SELECT secret FROM accounts)",
  "SELECT id AS secret FROM accounts UNION ALL SELECT secret FROM accounts",
  'SELECT id FROM accounts AS "accounts_raw" WHERE accounts_raw.secret IS NOT NULL',
];
const normalize = (rows) => rows.map((row) => JSON.stringify(row, Object.keys(row).sort())).sort();
const results = [];
for (const dialect of ["sqlite", "postgres"])
  for (const provider of ["drizzle", "kysely", "objection"])
    for (const local of [false, true]) {
      const fixture = await createContainmentFixture(provider, local, dialect);
      try {
        for (const sql of [...valid, ...invalid]) {
          const entry = { dialect, provider, local, sql, invalid: invalid.includes(sql) };
          try {
            let expected;
            if (!entry.invalid) {
              try {
                expected = await fixture.reference(sql);
              } catch (e) {
                entry.referenceError = e.message;
                results.push(entry);
                continue;
              }
            }
            await fixture.mutateHidden(1);
            fixture.statements.length = 0;
            const first = await fixture.schema.query({ sql, context: { org: 1 } });
            const calls = fixture.statements.length;
            await fixture.mutateHidden(27);
            fixture.statements.length = 0;
            const second = await fixture.schema.query({ sql, context: { org: 1 } });
            entry.actual = first.isOk() ? first.value : { error: first.error.message };
            entry.expected = expected;
            entry.calls = calls;
            entry.pass = entry.invalid
              ? first.isErr() && calls === 0 && second.isErr() && fixture.statements.length === 0
              : first.isOk() &&
                second.isOk() &&
                isDeepStrictEqual(normalize(first.value), normalize(expected)) &&
                isDeepStrictEqual(normalize(first.value), normalize(second.value));
          } catch (e) {
            entry.error = e.message;
            entry.pass = false;
          }
          results.push(entry);
          if (!entry.pass) console.log(JSON.stringify(entry));
        }
      } finally {
        await fixture.close();
      }
    }
writeFileSync(new URL("./scope-results.json", import.meta.url), JSON.stringify(results, null, 2));
console.log(
  JSON.stringify({
    total: results.length,
    pass: results.filter((r) => r.pass).length,
    fail: results.filter((r) => r.pass === false).length,
    referenceRejected: results.filter((r) => r.referenceError).length,
  }),
);
