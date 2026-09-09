import knex from "knex";
import { describe, expect, it, vi } from "vite-plus/test";
import { createExecutableSchemaSession } from "@tupl/runtime/session";
import { Result } from "better-result";
import { createObjectionProvider } from "@tupl/provider-objection";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

function unwrap<T, E>(result: Result<T, E>) {
  if (Result.isError(result)) throw result.error;
  return result.value;
}

async function fixture() {
  const db = knex({
    client: "better-sqlite3",
    connection: { filename: ":memory:" },
    useNullAsDefault: true,
  });
  await db.schema.createTable("raw_documents", (t) => {
    t.integer("id");
    t.text("status");
    t.text("body");
    t.integer("position");
  });
  await db("raw_documents").insert([
    { id: 1, status: "draft", body: "invalid", position: 1 },
    { id: 2, status: "published", body: '{"text":"hello"}', position: 2 },
    { id: 3, status: "published", body: '{"text":"world"}', position: 3 },
    { id: 4, status: "published", body: '{"text":"hello"}', position: 4 },
  ]);
  const sql: Array<{ sql: string; bindings: unknown[] }> = [];
  db.on("query", (q) => sql.push({ sql: q.sql, bindings: q.bindings }));
  const provider = createObjectionProvider({
    knex: db,
    entities: {
      raw: {
        table: "raw_documents",
        shape: { id: "integer", status: "text", body: "text", position: "integer" },
      },
    },
  });
  const builder = createSchemaBuilder();
  let parses = 0;
  let expensiveCalls = 0;
  const documents = builder.table("documents", provider.entities.raw, {
    columns: ({ col, derive }) => {
      const body = col.string("body", { nullable: false });
      const parsed = derive({ body }, ({ body }) => {
        parses++;
        const value: unknown = JSON.parse(body);
        if (
          !value ||
          typeof value !== "object" ||
          !("text" in value) ||
          typeof value.text !== "string"
        )
          throw new Error("Invalid document");
        return value.text;
      });
      return {
        // Declared first deliberately: planner scheduling must follow query dependencies.
        expensive: col.string(
          derive({ body }, ({ body }) => {
            expensiveCalls++;
            return body.repeat(2);
          }),
        ),
        id: col.integer("id", { nullable: false }),
        status: col.string("status", { nullable: false }),
        position: col.integer("position", { nullable: false }),
        text: col.string(parsed, { nullable: false }),
        size: col.integer(
          derive({ parsed }, ({ parsed }) => parsed.length),
          { nullable: false },
        ),
      };
    },
  });
  builder.view("documentView", ({ scan }) => scan(documents), {
    columns: ({ col }) => ({
      id: col.integer(documents, "id"),
      status: col.string(documents, "status"),
      text: col.string(documents, "text"),
    }),
  });
  builder.view("decorated", ({ scan }) => scan(documents), {
    columns: ({ col, derive }) => ({
      id: col.integer(documents, "id"),
      status: col.string(documents, "status"),
      shout: col.string(
        derive({ text: col.string(documents, "text") }, ({ text }) => text?.toUpperCase() ?? ""),
      ),
    }),
  });
  const executable = unwrap(createExecutableSchema(builder));
  return {
    db,
    sql,
    provider,
    builder,
    documents,
    executable,
    parses: () => parses,
    expensiveCalls: () => expensiveCalls,
    async query(sql: string) {
      return unwrap(await executable.query({ sql, context: {} }));
    },
  };
}

describe("derived columns", () => {
  it("keeps unused computation out of native queries and counts", async () => {
    const f = await fixture();
    try {
      expect(await f.query("SELECT id FROM documents ORDER BY id LIMIT 1")).toEqual([{ id: 1 }]);
      expect(await f.query('SELECT COUNT(*) AS "n" FROM documents')).toEqual([{ n: 4 }]);
      expect(f.parses()).toBe(0);
      expect(f.sql.every((q) => !q.sql.includes("body"))).toBe(true);
    } finally {
      await f.db.destroy();
    }
  });

  it("pushes native filters before computation and shares intermediate results", async () => {
    const f = await fixture();
    try {
      expect(
        await f.query(
          "SELECT id, text, size FROM documents WHERE status = 'published' ORDER BY id",
        ),
      ).toEqual([
        { id: 2, text: "hello", size: 5 },
        { id: 3, text: "world", size: 5 },
        { id: 4, text: "hello", size: 5 },
      ]);
      expect(f.parses()).toBe(3);
      expect(f.sql.some((q) => q.bindings.includes("published"))).toBe(true);
    } finally {
      await f.db.destroy();
    }
  });

  it("filters derived values before limiting and delays projection-only computation", async () => {
    const f = await fixture();
    try {
      expect(
        await f.query(
          "SELECT id, text FROM documents WHERE status = 'published' ORDER BY position DESC LIMIT 1",
        ),
      ).toEqual([{ id: 4, text: "hello" }]);
      expect(f.parses()).toBe(1);
      expect(
        await f.query(
          "SELECT id FROM documents WHERE status = 'published' AND text = 'hello' ORDER BY position DESC LIMIT 1",
        ),
      ).toEqual([{ id: 4 }]);
      expect(f.parses()).toBe(4);
    } finally {
      await f.db.destroy();
    }
  });
});

describe("derived relational semantics", () => {
  it.each([
    ["SELECT id FROM documentView ORDER BY id", [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }], 0],
    ["SELECT id FROM decorated ORDER BY id", [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }], 0],
    [
      "SELECT shout FROM decorated WHERE status = 'published' ORDER BY id LIMIT 1",
      [{ shout: "HELLO" }],
      1,
    ],
    [
      "SELECT text, COUNT(*) AS n FROM documents WHERE status = 'published' GROUP BY text ORDER BY text",
      [
        { text: "hello", n: 2 },
        { text: "world", n: 1 },
      ],
      3,
    ],
    [
      "SELECT DISTINCT text FROM documents WHERE status = 'published' ORDER BY text",
      [{ text: "hello" }, { text: "world" }],
      3,
    ],
    [
      "SELECT id FROM documents WHERE status = 'published' AND (id = 3 OR text = 'hello') ORDER BY id",
      [{ id: 2 }, { id: 3 }, { id: 4 }],
      3,
    ],
    [
      "WITH d AS (SELECT id, text FROM documents) SELECT id FROM d ORDER BY id",
      [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }],
      0,
    ],
    [
      "SELECT a.id FROM documents a JOIN documents b ON a.text = b.text WHERE a.status = 'published' AND b.status = 'published' ORDER BY a.id",
      [{ id: 2 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 4 }],
      6,
    ],
  ])("%s", async (sql, expected, calls) => {
    const f = await fixture();
    try {
      expect(await f.query(sql)).toEqual(expected);
      expect(f.parses()).toBe(calls);
    } finally {
      await f.db.destroy();
    }
  });

  it("never exposes private inputs and recomputes across queries", async () => {
    const f = await fixture();
    try {
      const invalid = await f.executable.query({ sql: "SELECT body FROM documents", context: {} });
      expect(Result.isError(invalid)).toBe(true);
      expect(f.sql).toHaveLength(0);
      expect(await f.query("SELECT text FROM documents WHERE id = 2")).toEqual([{ text: "hello" }]);
      await f.db("raw_documents").where({ id: 2 }).update({ body: '{"text":"changed"}' });
      expect(await f.query("SELECT text FROM documents WHERE id = 2")).toEqual([
        { text: "changed" },
      ]);
      expect(f.parses()).toBe(2);
    } finally {
      await f.db.destroy();
    }
  });

  it("explain describes local dependencies without invoking them", async () => {
    const f = await fixture();
    try {
      const plan = unwrap(
        await f.executable.explain({ sql: "SELECT text FROM documents WHERE id = 2", context: {} }),
      );
      expect(JSON.stringify(plan)).toContain('"kind":"local"');
      expect(JSON.stringify(plan)).not.toContain("JSON.parse");
      expect(f.parses()).toBe(0);
      expect(f.sql).toHaveLength(0);
    } finally {
      await f.db.destroy();
    }
  });
});

describe("derived execution contracts", () => {
  it("delays unrelated expensive outputs until after a derived filter", async () => {
    const f = await fixture();
    try {
      const rows = await f.query(
        "SELECT expensive FROM documents WHERE status = 'published' AND text = 'world'",
      );
      expect(rows).toEqual([{ expensive: '{"text":"world"}{"text":"world"}' }]);
      expect(f.parses()).toBe(3);
      expect(f.expensiveCalls()).toBe(1);
    } finally {
      await f.db.destroy();
    }
  });

  it("reports real computation counts without retaining intermediate values", async () => {
    const f = await fixture();
    try {
      const session = unwrap(
        createExecutableSchemaSession(f.executable, {
          sql: "SELECT text, size FROM documents WHERE id = 2",
          context: {},
        }),
      );
      const events = [];
      while (true) {
        const event = await session.next();
        if ("done" in event) break;
        events.push(event);
      }
      const computations = events.flatMap((event) =>
        event.status === "done" ? (event.computations ?? []) : [],
      );
      expect(computations.filter((c) => c.label === "derive").map((c) => c.invocations)).toEqual([
        1, 1,
      ]);
      expect(events.every((event) => !("rows" in event))).toBe(true);
    } finally {
      await f.db.destroy();
    }
  });

  it.each([
    [
      "SELECT id, ROW_NUMBER() OVER (ORDER BY text, id) AS ranking FROM documents WHERE status = 'published' ORDER BY ranking",
      [
        { id: 2, ranking: 1 },
        { id: 4, ranking: 2 },
        { id: 3, ranking: 3 },
      ],
    ],
    [
      "SELECT text FROM documents WHERE id = 2 UNION SELECT text FROM documents WHERE id = 4",
      [{ text: "hello" }],
    ],
    [
      "WITH d AS (SELECT id, text FROM documents WHERE status = 'published') SELECT COUNT(*) AS n FROM d",
      [{ n: 3 }],
    ],
  ])("%s", async (sql, rows) => {
    const f = await fixture();
    try {
      expect(await f.query(sql)).toEqual(rows);
    } finally {
      await f.db.destroy();
    }
  });
});

describe("derived boundary regressions", () => {
  it.each([0, 1, 4])(
    "preserves singleton aggregate cardinality for %i source rows",
    async (count) => {
      const f = await fixture();
      try {
        await f.db("raw_documents").where("id", ">", count).delete();
        f.sql.length = 0;
        for (const sql of [
          "SELECT COUNT(*) AS n FROM (SELECT MAX(text) AS unused FROM documents) d",
          "WITH d AS (SELECT MAX(text) AS unused FROM documents) SELECT COUNT(*) AS n FROM d",
        ])
          expect(await f.query(sql)).toEqual([{ n: 1 }]);
        expect(f.parses()).toBe(0);
        expect(f.sql.every((q) => !q.sql.includes("body"))).toBe(true);
      } finally {
        await f.db.destroy();
      }
    },
  );

  it("reads a bounded native projection for zero-dependency generators", async () => {
    const f = await fixture();
    try {
      f.builder.table("constants", f.provider.entities.raw, {
        columns: ({ col, derive }) => ({ value: col.integer(derive({}, () => 7)) }),
      });
      const schema = unwrap(createExecutableSchema(f.builder));
      expect(
        unwrap(await schema.query({ sql: "SELECT value FROM constants", context: {} })),
      ).toEqual(Array.from({ length: 4 }, () => ({ value: 7 })));
      expect(f.sql.every((q) => !q.sql.includes("body") && !q.sql.includes("select *"))).toBe(true);
    } finally {
      await f.db.destroy();
    }
  });

  it("coerces private inputs once, before validating callback values", async () => {
    const f = await fixture();
    let coercions = 0;
    try {
      f.builder.table("coerced", f.provider.entities.raw, {
        columns: ({ col, derive }) => {
          const id = col.string("id", {
            nullable: false,
            coerce: (value) => {
              coercions++;
              return `${String(value)}!`;
            },
          });
          return {
            value: col.string(
              derive({ id }, ({ id }) => id),
              { nullable: false },
            ),
          };
        },
      });
      const schema = unwrap(createExecutableSchema(f.builder));
      expect(unwrap(await schema.query({ sql: "SELECT value FROM coerced", context: {} }))).toEqual(
        [1, 2, 3, 4].map((id) => ({ value: `${id}!` })),
      );
      expect(coercions).toBe(4);
    } finally {
      await f.db.destroy();
    }
  });

  it("rejects async results without leaking an unhandled rejection", async () => {
    const f = await fixture();
    try {
      f.builder.table("invalidAsync", f.provider.entities.raw, {
        columns: ({ col, derive }) => ({
          value: col.json(derive({}, (): unknown => Promise.reject(new Error("async failure")))),
        }),
      });
      const schema = unwrap(createExecutableSchema(f.builder));
      const result = await schema.query({ sql: "SELECT value FROM invalidAsync", context: {} });
      expect(Result.isError(result)).toBe(true);
      if (Result.isError(result)) expect(result.error.message).toContain("synchronously");
      // The test runner treats an unhandled rejection during this turn as a test failure.
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
    } finally {
      await f.db.destroy();
    }
  });

  it("passes null dependencies to callbacks and validates public output types", async () => {
    const f = await fixture();
    try {
      await f.db("raw_documents").where({ id: 2 }).update({ body: null });
      f.builder.table("nulls", f.provider.entities.raw, {
        columns: ({ col, derive }) => ({
          id: col.integer("id"),
          value: col.string(derive({ body: col.string("body") }, ({ body }) => body ?? "missing")),
          wrong: col.integer(derive({}, (): any => "not a number")),
        }),
      });
      const schema = unwrap(createExecutableSchema(f.builder));
      expect(
        unwrap(await schema.query({ sql: "SELECT value FROM nulls WHERE id = 2", context: {} })),
      ).toEqual([{ value: "missing" }]);
      const wrong = await schema.query({
        sql: "SELECT wrong FROM nulls WHERE id = 2",
        context: {},
      });
      expect(Result.isError(wrong)).toBe(true);
    } finally {
      await f.db.destroy();
    }
  });
});

// A bounded query generator compares the planner against direct transformations of source rows.
// Native predicates avoid the deliberately malformed draft document.
describe("derived reference evaluation", () => {
  const sources = [
    { id: 2, text: "hello" },
    { id: 3, text: "world" },
    { id: 4, text: "hello" },
  ];
  for (const connective of ["AND", "OR"] as const) {
    for (const threshold of [1, 3, 5]) {
      for (const offset of [0, 1]) {
        it(`${connective}, threshold ${threshold}, offset ${offset}`, async () => {
          const f = await fixture();
          try {
            const expected = sources
              .filter((row) =>
                connective === "AND"
                  ? row.id >= threshold && row.text === "hello"
                  : row.id >= threshold || row.text === "hello",
              )
              .slice()
              .reverse()
              .slice(offset, offset + 2);
            expect(
              await f.query(
                `SELECT id, text FROM documents WHERE status = 'published' AND (id >= ${threshold} ${connective} text = 'hello') ORDER BY id DESC LIMIT 2 OFFSET ${offset}`,
              ),
            ).toEqual(expected);
          } finally {
            await f.db.destroy();
          }
        });
      }
    }
  }

  it("exposes public wildcard columns and no private inputs", async () => {
    const f = await fixture();
    try {
      expect(await f.query("SELECT * FROM documentView WHERE id = 2")).toEqual([
        { id: 2, status: "published", text: "hello" },
      ]);
      expect(f.parses()).toBe(1);
    } finally {
      await f.db.destroy();
    }
  });

  it("preserves nullable outer join rows and multiplying joins", async () => {
    const f = await fixture();
    try {
      expect(
        await f.query(
          "SELECT a.id, b.text FROM documents a LEFT JOIN (SELECT position, text FROM documents WHERE status = 'published') b ON a.id = b.position ORDER BY a.id",
        ),
      ).toEqual([
        { id: 1, text: null },
        { id: 2, text: "hello" },
        { id: 3, text: "world" },
        { id: 4, text: "hello" },
      ]);
      expect(
        await f.query(
          "SELECT COUNT(*) AS n FROM documents a JOIN documents b ON a.text = b.text WHERE a.status = 'published' AND b.status = 'published'",
        ),
      ).toEqual([{ n: 5 }]);
    } finally {
      await f.db.destroy();
    }
  });

  it("rejects handles borrowed from another columns declaration", async () => {
    const f = await fixture();
    try {
      let borrowed: import("@tupl/schema-model/dsl").SchemaDerivedValue<string> | undefined;
      f.builder.table("owner", f.provider.entities.raw, {
        columns: ({ col, derive }) => {
          borrowed = derive({}, () => "value");
          return { value: col.string(borrowed) };
        },
      });
      expect(() =>
        f.builder.table("borrower", f.provider.entities.raw, {
          columns: ({ col }) => ({ value: col.string(borrowed!) }),
        }),
      ).toThrow("this columns declaration");
    } finally {
      await f.db.destroy();
    }
  });

  it("accepts relational calculated columns as dependencies", async () => {
    const f = await fixture();
    try {
      f.builder.table("arithmetic", f.provider.entities.raw, {
        columns: ({ col, derive, expr }) => {
          const doubled = col.integer(expr.multiply(col("id"), expr.literal(2)), {
            nullable: false,
          });
          return { value: col.integer(derive({ doubled }, ({ doubled }) => doubled + 1)) };
        },
      });
      const schema = unwrap(createExecutableSchema(f.builder));
      expect(
        unwrap(
          await schema.query({ sql: "SELECT value FROM arithmetic ORDER BY value", context: {} }),
        ),
      ).toEqual([3, 5, 7, 9].map((value) => ({ value })));
    } finally {
      await f.db.destroy();
    }
  });
});

it("stops after a synchronous computation crosses the execution deadline", async () => {
  const f = await fixture();
  const realNow = Date.now.bind(Date);
  let elapsed = 0;
  const clock = vi.spyOn(Date, "now").mockImplementation(() => realNow() + elapsed);
  let calls = 0;
  try {
    f.builder.table("slow", f.provider.entities.raw, {
      columns: ({ col, derive }) => ({
        value: col.integer(
          derive({}, () => {
            calls++;
            elapsed += 10_000;
            return 1;
          }),
        ),
      }),
    });
    const schema = unwrap(createExecutableSchema(f.builder));
    const result = await schema.query({
      sql: "SELECT value FROM slow",
      context: {},
      queryGuardrails: { timeoutMs: 1000 },
    });
    expect(Result.isError(result)).toBe(true);
    if (Result.isError(result)) expect(result.error.message).toContain("timed out");
    expect(calls).toBe(1);
  } finally {
    clock.mockRestore();
    await f.db.destroy();
  }
});

it("prunes computation from cardinality-only and unreferenced CTEs", async () => {
  const f = await fixture();
  try {
    expect(
      await f.query("WITH d AS (SELECT text FROM documents) SELECT COUNT(*) AS n FROM d"),
    ).toEqual([{ n: 4 }]);
    expect(
      await f.query(
        "WITH d AS (SELECT text FROM documents) SELECT id FROM documents ORDER BY id LIMIT 1",
      ),
    ).toEqual([{ id: 1 }]);
    expect(f.parses()).toBe(0);
    expect(f.sql.every((q) => !q.sql.includes("body"))).toBe(true);
  } finally {
    await f.db.destroy();
  }
});

it("evaluates derived values inside scalar subqueries", async () => {
  const f = await fixture();
  try {
    expect(
      await f.query(
        "SELECT (SELECT text FROM documents WHERE status = 'published' ORDER BY id LIMIT 1) AS chosen FROM documents WHERE id = 1",
      ),
    ).toEqual([{ chosen: "hello" }]);
    expect(f.parses()).toBe(1);
  } finally {
    await f.db.destroy();
  }
});

it("preserves derived grouping and HAVING semantics", async () => {
  const f = await fixture();
  try {
    expect(
      await f.query(
        "SELECT text, COUNT(*) AS n FROM documents WHERE status = 'published' GROUP BY text HAVING COUNT(*) > 1 ORDER BY text",
      ),
    ).toEqual([{ text: "hello", n: 2 }]);
    expect(f.parses()).toBe(3);
  } finally {
    await f.db.destroy();
  }
});
