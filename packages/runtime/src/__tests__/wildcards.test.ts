import { describe, expect, it } from "vite-plus/test";
import { commerceRows, commerceSchema } from "@tupl/test-support/fixtures";
import { withQueryHarness } from "@tupl/test-support/runtime";
import { lowerSqlToRelResult } from "@tupl/planner";

const queries = [
  "SELECT * FROM users ORDER BY id",
  "SELECT u.* FROM users u ORDER BY id",
  "SELECT * FROM users ORDER BY 1",
  "SELECT a.*, b.* FROM (SELECT id AS user_id FROM users) a JOIN (SELECT user_id AS buyer_id FROM orders) b ON a.user_id = b.buyer_id ORDER BY user_id",
  "WITH a AS (SELECT id FROM users) SELECT * FROM (WITH a AS (SELECT email FROM users) SELECT * FROM a) q ORDER BY email",
  "SELECT *, 1 AS extra FROM users ORDER BY id",
  "SELECT 1 AS extra, u.* FROM users u ORDER BY id",
  "SELECT DISTINCT * FROM users ORDER BY id",
  "SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS position FROM users ORDER BY id",
  "SELECT u.* FROM users u WHERE EXISTS (SELECT * FROM orders o WHERE o.user_id = u.id) ORDER BY id",
  "SELECT u.*, o.id AS order_id FROM users u JOIN orders o ON u.id = o.user_id ORDER BY order_id",
  "WITH a AS (SELECT id, email FROM users), b AS (SELECT * FROM a) SELECT b.* FROM b ORDER BY id",
  "SELECT q.* FROM (SELECT id, email FROM users) q ORDER BY id",
  "SELECT * FROM users UNION ALL SELECT * FROM users",
  "SELECT id, email FROM users UNION ALL SELECT q.* FROM (SELECT id, email FROM users) q",
  "WITH RECURSIVE r AS (SELECT id FROM users UNION SELECT r.* FROM r) SELECT * FROM r ORDER BY id",
  "SELECT *, COUNT(*) AS n FROM (SELECT id FROM users) u GROUP BY id ORDER BY id",
];

describe("SELECT wildcards", () => {
  it.each(queries)("executes %s with SQLite parity", async (sql) => {
    await withQueryHarness(
      { schema: commerceSchema, rowsByTable: commerceRows },
      async (harness) => {
        const { actual, expected } = await harness.runAgainstBoth(sql, {});
        expect(actual).toEqual(expected);
        const plan = lowerSqlToRelResult(sql, commerceSchema).unwrap().rel;
        expect(plan.output.map((column) => column.name)).toEqual(Object.keys(expected[0]!));
      },
    );
  });

  it.each([
    ["SELECT missing.* FROM users", "Unknown wildcard qualifier: missing"],
    ["SELECT users.* FROM users u", "Unknown wildcard qualifier: users"],
    ["SELECT * FROM users u JOIN orders o ON u.id = o.user_id", "Duplicate output column: id"],
    ["SELECT *, id FROM users", "Duplicate output column: id"],
    ["SELECT id, id FROM users", "Duplicate output column: id"],
    ["SELECT * FROM users UNION SELECT id FROM users", "same number of columns"],
    ["SELECT id FROM users UNION SELECT * FROM users", "same number of columns"],
    [
      "WITH RECURSIVE r AS (SELECT id FROM users UNION SELECT *, 1 AS extra FROM r) SELECT * FROM r",
      "same number of columns",
    ],
    ["SELECT *", "requires a source relation"],
    ["SELECT * AS renamed FROM users", "cannot have an alias"],
    ["SELECT *, COUNT(*) AS n FROM users", "could not be lowered"],
  ])("rejects %s", async (sql, message) => {
    await withQueryHarness(
      { schema: commerceSchema, rowsByTable: commerceRows },
      async (harness) => {
        await expect(harness.runTupl(sql, {})).rejects.toThrow(message);
      },
    );
  });
});
