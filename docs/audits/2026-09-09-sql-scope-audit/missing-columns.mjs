import { createContainmentFixture } from "../../../test/support/containment.ts";
import { writeFileSync } from "node:fs";
import { isDeepStrictEqual } from "node:util";
const valid = [];
const invalid = [
  "WITH c AS (SELECT id FROM accounts) SELECT id FROM c WHERE secret IS NULL",
  "WITH c AS (SELECT id FROM accounts) SELECT COUNT(secret) AS n FROM c",
  "WITH c AS (SELECT id FROM accounts) SELECT secret,COUNT(*) AS n FROM c GROUP BY secret",
  "WITH c AS (SELECT id FROM accounts) SELECT id,LAG(secret) OVER (ORDER BY id) AS v FROM c",
  "WITH c AS (SELECT id FROM accounts) SELECT constructor FROM c",
  "SELECT secret FROM (SELECT id FROM accounts) x",
  "WITH RECURSIVE c(id) AS (SELECT id FROM accounts WHERE id=1 UNION ALL SELECT id+1 FROM c WHERE id<2) SELECT secret FROM c",
  "SELECT id,COUNT(*) AS n FROM accounts GROUP BY id HAVING secret IS NULL",
  "SELECT id,COUNT(*) AS n FROM accounts GROUP BY id HAVING accounts.secret IS NULL",
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
writeFileSync(
  new URL("./missing-columns-results.json", import.meta.url),
  JSON.stringify(results, null, 2),
);
console.log(
  JSON.stringify({
    total: results.length,
    pass: results.filter((r) => r.pass).length,
    fail: results.filter((r) => r.pass === false).length,
    referenceRejected: results.filter((r) => r.referenceError).length,
  }),
);
