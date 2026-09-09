import { test as propertyTest, fc } from "@fast-check/vitest";
import { afterEach, describe, expect, it } from "vite-plus/test";
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { eq, gte } from "drizzle-orm";
import { sqliteTable, integer } from "drizzle-orm/sqlite-core";
import { Kysely, SqliteDialect } from "kysely";
import knexFactory from "knex";
import type { RelNode } from "@tupl/foundation";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createKyselyProvider } from "@tupl/provider-kysely";
import { createObjectionProvider } from "@tupl/provider-objection";

type Context = { org: number; deny?: boolean };
const ddl = [
  "create table a(id integer, org integer)",
  "create table b(id integer, org integer)",
  "insert into a values (0,1),(1,1),(2,1),(3,2)",
  "insert into b values (1,1),(2,2),(4,1)",
];
const convention = "provider:scoped";
function scan(table: string, alias = table): RelNode {
  return {
    id: `scan_${table}`,
    kind: "scan",
    convention,
    table,
    alias,
    select: ["id", "org"],
    output: [{ name: `${alias}.id` }, { name: `${alias}.org` }],
  };
}
function project(
  input: RelNode,
  columns: Extract<RelNode, { kind: "project" }>["columns"],
): RelNode {
  return {
    id: "project",
    kind: "project",
    convention,
    input,
    columns,
    output: columns.map((c) => ({ name: c.output })),
  };
}
function single(table = "a", alias = table) {
  return project(scan(table, alias), [{ source: { alias, column: "id" }, output: "id" }]);
}
function join(joinType: Extract<RelNode, { kind: "join" }>["joinType"]) {
  const semi = joinType === "semi";
  return project(
    {
      id: "join",
      kind: "join",
      convention,
      left: scan("a"),
      right: semi ? single("b") : scan("b"),
      joinType,
      leftKey: { alias: "a", column: "id" },
      rightKey: { alias: "b", column: "id" },
      output: semi ? scan("a").output : [...scan("a").output, ...scan("b").output],
    },
    semi
      ? [{ source: { alias: "a", column: "id" }, output: "id" }]
      : [
          { source: { alias: "a", column: "id" }, output: "a_id" },
          { source: { alias: "b", column: "id" }, output: "b_id" },
        ],
  );
}
const cases: Array<{ name: string; rel: RelNode; rows: Record<string, unknown>[] }> = [
  { name: "scan", rel: single(), rows: [{ id: 1 }, { id: 2 }] },
  { name: "aliased scan", rel: single("a", "visible_a"), rows: [{ id: 1 }, { id: 2 }] },
  {
    name: "aggregate",
    rel: {
      id: "aggregate",
      kind: "aggregate",
      convention,
      input: scan("a"),
      groupBy: [],
      metrics: [{ fn: "count", as: "n" }],
      output: [{ name: "n" }],
    },
    rows: [{ n: 2 }],
  },
  { name: "inner join", rel: join("inner"), rows: [{ a_id: 1, b_id: 1 }] },
  {
    name: "left join",
    rel: join("left"),
    rows: [
      { a_id: 1, b_id: 1 },
      { a_id: 2, b_id: null },
    ],
  },
  {
    name: "right join",
    rel: join("right"),
    rows: [
      { a_id: 1, b_id: 1 },
      { a_id: null, b_id: 4 },
    ],
  },
  {
    name: "full join",
    rel: join("full"),
    rows: [
      { a_id: 1, b_id: 1 },
      { a_id: 2, b_id: null },
      { a_id: null, b_id: 4 },
    ],
  },
  { name: "semi join", rel: join("semi"), rows: [{ id: 1 }] },
  ...(["union_all", "union", "intersect", "except"] as const).map((op) => ({
    name: op,
    rel: {
      id: "set",
      kind: "set_op",
      convention,
      op,
      left: single(),
      right: single("b"),
      output: [{ name: "id" }],
    } satisfies RelNode,
    rows: (op === "union_all"
      ? [1, 2, 1, 4]
      : op === "union"
        ? [1, 2, 4]
        : op === "intersect"
          ? [1]
          : [2]
    ).map((id) => ({ id })),
  })),
  {
    name: "CTE",
    rel: {
      id: "with",
      kind: "with",
      convention,
      ctes: [{ name: "visible", query: single() }],
      body: project(
        {
          id: "ref",
          kind: "cte_ref",
          convention,
          name: "visible",
          alias: "v",
          select: ["id"],
          output: [{ name: "id" }],
        },
        [{ source: { alias: "v", column: "id" }, output: "id" }],
      ),
      output: [{ name: "id" }],
    },
    rows: [{ id: 1 }, { id: 2 }],
  },
  {
    name: "grouped aggregate",
    rel: {
      id: "groups",
      kind: "aggregate",
      convention,
      input: scan("a"),
      groupBy: [{ alias: "a", column: "org" }],
      metrics: [{ fn: "count", as: "n" }],
      output: [{ name: "org" }, { name: "n" }],
    },
    rows: [{ org: 1, n: 2 }],
  },
  {
    name: "CTE window",
    rel: {
      id: "with_window",
      kind: "with",
      convention,
      ctes: [{ name: "visible", query: single() }],
      body: project(
        {
          id: "window",
          kind: "window",
          convention,
          input: {
            id: "ref",
            kind: "cte_ref",
            convention,
            name: "visible",
            alias: "v",
            select: ["id"],
            output: [{ name: "id" }],
          },
          functions: [
            {
              fn: "row_number",
              as: "rank",
              partitionBy: [],
              orderBy: [{ source: { column: "id" }, direction: "asc" }],
            },
          ],
          output: [{ name: "id" }, { name: "rank" }],
        },
        [
          { source: { alias: "v", column: "id" }, output: "id" },
          { source: { column: "rank" }, output: "rank" },
        ],
      ),
      output: [{ name: "id" }, { name: "rank" }],
    },
    rows: [
      { id: 1, rank: 1 },
      { id: 2, rank: 2 },
    ],
  },
  {
    name: "sort and pagination",
    rel: {
      id: "limit",
      kind: "limit_offset",
      convention,
      limit: 1,
      offset: 1,
      input: {
        id: "sort",
        kind: "sort",
        convention,
        input: single(),
        orderBy: [{ source: { column: "id" }, direction: "asc" }],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    },
    rows: [{ id: 2 }],
  },
];
function checkContext(context: Context) {
  if (context.deny) throw new Error("scope denied");
}
for (const name of ["drizzle", "kysely", "objection"] as const)
  describe(`${name} scope isolation`, () => {
    const cleanups: Array<() => unknown> = [];
    afterEach(async () => {
      for (const cleanup of cleanups.splice(0)) await cleanup();
    });
    async function fixture() {
      if (name === "objection") {
        const knex = knexFactory({
          client: "better-sqlite3",
          connection: { filename: ":memory:" },
          useNullAsDefault: true,
        });
        cleanups.push(() => knex.destroy());
        for (const sql of ddl) await knex.raw(sql);
        return createObjectionProvider<Context>({
          name: "scoped",
          knex,
          entities: Object.fromEntries(
            ["a", "b"].map((table) => [
              table,
              {
                table,
                base: (context: Context) => {
                  checkContext(context);
                  return knex(table)
                    .where(`${table}.org`, context.org)
                    .where(`${table}.id`, ">=", 1);
                },
              },
            ]),
          ),
        });
      }
      const sqlite = new Database(":memory:");
      sqlite.exec(ddl.join(";"));
      if (name === "kysely") {
        const db = new Kysely({ dialect: new SqliteDialect({ database: sqlite }) });
        cleanups.push(() => db.destroy());
        return createKyselyProvider<Context>({
          name: "scoped",
          db,
          entities: {
            a: {
              table: "a",
              base: async ({ query, context, alias }) => {
                checkContext(context);
                return query.where(`${alias}.org`, "=", context.org).where(`${alias}.id`, ">=", 1);
              },
            },
            b: {
              table: "b",
              base: async ({ query, context, alias }) => {
                checkContext(context);
                return query.where(`${alias}.org`, "=", context.org).where(`${alias}.id`, ">=", 1);
              },
            },
          },
        });
      }
      cleanups.push(() => sqlite.close());
      return createDrizzleProvider<Context>({
        name: "scoped",
        db: drizzle(sqlite),
        tables: Object.fromEntries(
          ["a", "b"].map((name) => {
            const table = sqliteTable(name, { id: integer("id"), org: integer("org") });
            return [
              name,
              {
                table,
                scope: async (context: Context) => {
                  checkContext(context);
                  return [eq(table.org, context.org), gte(table.id, 1)];
                },
              },
            ];
          }),
        ),
      });
    }
    it.each(cases)("scopes $name before relational operations", async ({ rel, rows }) => {
      const provider = await fixture();
      const context = { org: 1 };
      expect(await provider.canExecute(rel, context)).toBe(true);
      const plan = (await provider.compile(rel, context)).unwrap();
      const actual = (await provider.execute(plan, context)).unwrap();
      expect(actual).toHaveLength(rows.length);
      expect(actual).toEqual(expect.arrayContaining(rows));
    });
    it("uses the execution context when reusing a compiled plan", async () => {
      const provider = await fixture();
      const plan = (await provider.compile(single(), { org: 1 })).unwrap();
      expect((await provider.execute(plan, { org: 2 })).unwrap()).toEqual([{ id: 3 }]);
      expect((await provider.execute(plan, { org: 1 })).unwrap()).toEqual([{ id: 1 }, { id: 2 }]);
    });
    it("scopes lookup keys and additional filters", async () => {
      const provider = await fixture();
      const request = {
        table: "a",
        key: "id",
        keys: [0, 1, 2, 3],
        select: ["id"],
        where: [{ op: "gte", column: "id", value: 2 }] as const,
      };
      expect(
        (await provider.lookupMany({ ...request, where: [...request.where] }, { org: 1 })).unwrap(),
      ).toEqual([{ id: 2 }]);
      expect(
        (await provider.lookupMany({ ...request, where: [...request.where] }, { org: 2 })).unwrap(),
      ).toEqual([{ id: 3 }]);
    });
    propertyTest.prop(
      {
        org: fc.integer({ min: 1, max: 2 }),
        requestedOrg: fc.integer({ min: 1, max: 3 }),
        negate: fc.boolean(),
        keys: fc.array(fc.integer({ min: 0, max: 5 }), { maxLength: 12 }),
      },
      { numRuns: 100 },
    )(
      "keeps generated lookup predicates inside mandatory scope",
      async ({ org, requestedOrg, negate, keys }) => {
        const provider = await fixture();
        const actual = (
          await provider.lookupMany(
            {
              table: "a",
              key: "id",
              keys,
              select: ["id"],
              where: [{ op: negate ? "neq" : "eq", column: "org", value: requestedOrg }],
            },
            { org },
          )
        ).unwrap();
        const userPredicateMatchesScope = negate ? org !== requestedOrg : org === requestedOrg;
        const authorizedIds = org === 1 ? [1, 2] : [3];
        const expected = authorizedIds
          .filter((id) => keys.includes(id) && userPredicateMatchesScope)
          .map((id) => ({ id }));
        expect(actual).toEqual(expected);
      },
    );
    it("fails closed when scope resolution throws", async () => {
      const provider = await fixture();
      const context = { org: 1, deny: true };
      const plan = (await provider.compile(single(), context)).unwrap();
      expect((await provider.execute(plan, context)).isErr()).toBe(true);
      expect(
        (
          await provider.lookupMany({ table: "a", key: "id", keys: [1], select: ["id"] }, context)
        ).isErr(),
      ).toBe(true);
    });
  });
