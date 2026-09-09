import knex from "knex";
import { describe, expect, it } from "vite-plus/test";
import { createObjectionProvider } from "@tupl/provider-objection";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";
import { createExecutableSchemaSession, type QueryStepEvent } from "@tupl/runtime/session";

describe("SQL private-entity lookup inspection", () => {
  it.each([false, true])(
    "aligns plans and scoped keyed SQL with derived left key=%s",
    async (derivedKey) => {
      const leftDb = knex({
        client: "better-sqlite3",
        connection: { filename: ":memory:" },
        useNullAsDefault: true,
      });
      const rightDb = knex({
        client: "better-sqlite3",
        connection: { filename: ":memory:" },
        useNullAsDefault: true,
      });
      try {
        await leftDb.schema.createTable("documents", (table) => {
          table.integer("id");
          table.integer("author_id");
          table.string("author_key");
        });
        await rightDb.schema.createTable("profiles", (table) => {
          table.integer("id");
          table.string("name");
          table.string("tenant");
        });
        await leftDb("documents").insert([
          { id: 1, author_id: 7, author_key: "user:7" },
          { id: 2, author_id: 7, author_key: "user:7" },
          { id: 3, author_id: null, author_key: null },
          { id: 4, author_id: 8, author_key: "user:8" },
        ]);
        await rightDb("profiles").insert([
          { id: 7, name: "Visible", tenant: "allowed" },
          { id: 8, name: "Hidden", tenant: "other" },
          { id: 9, name: "Unrequested", tenant: "allowed" },
        ]);
        const left = createObjectionProvider({
          name: "documentsDb",
          knex: (_context: { tenant: string }) => leftDb,
          entities: {
            documents: {
              table: "documents",
              shape: { id: "integer", author_id: "integer", author_key: "text" },
            },
          },
        });
        const right = createObjectionProvider({
          name: "profilesDb",
          knex: (_context: { tenant: string }) => rightDb,
          entities: {
            profiles: {
              table: "profiles",
              shape: { id: "integer", name: "text" },
              base: ({ tenant }) => rightDb("profiles").where("tenant", tenant),
            },
          },
        });
        const builder = createSchemaBuilder<{ tenant: string }>();
        let derivations = 0;
        const documents = builder.table("documents", left.entities.documents, {
          columns: ({ col, derive }) => ({
            id: col.integer("id"),
            author_id: derivedKey
              ? col.integer(
                  derive({ key: col.string("author_key") }, ({ key }) => {
                    derivations += 1;
                    return key == null ? null : Number(key.slice(5));
                  }),
                )
              : col.integer("author_id"),
          }),
        });
        const profiles = right.entities.profiles;
        builder.view(
          "document_profiles",
          ({ scan, join, col, expr }) =>
            join({
              left: scan(documents),
              right: scan(profiles),
              type: "left",
              on: expr.eq(col(documents, "author_id"), col(profiles, "id")),
            }),
          {
            columns: ({ col }) => ({
              id: col.integer(documents, "id"),
              author_name: col.string(profiles, "name"),
            }),
          },
        );
        builder.view(
          "paged_profiles",
          () => ({
            id: "page",
            kind: "limit_offset",
            convention: "local",
            limit: 1,
            offset: 1,
            input: {
              id: "page_order",
              kind: "sort",
              convention: "local",
              orderBy: [{ source: { alias: "p", column: "id" }, direction: "asc" }],
              input: {
                id: "page_scan",
                kind: "scan",
                convention: "local",
                table: "profiles",
                entity: profiles,
                alias: "p",
                select: ["id", "name"],
                output: [{ name: "p.id" }, { name: "p.name" }],
              },
              output: [{ name: "p.id" }, { name: "p.name" }],
            },
            output: [{ name: "p.id" }, { name: "p.name" }],
          }),
          {
            columns: ({ col }) => ({ id: col.integer("p.id"), name: col.string("p.name") }),
          },
        );
        const schema = createExecutableSchema(builder).unwrap();
        const sql = "SELECT id, author_name FROM document_profiles ORDER BY id";
        const context = { tenant: "allowed" };
        const statements: Array<{ sql: string; bindings?: readonly unknown[] }> = [];
        rightDb.on("query", (statement) => statements.push(statement));
        const explained = (await schema.explain({ sql, context })).unwrap();
        expect(statements).toEqual([]);
        expect(explained.physicalPlan.steps.some((step) => step.kind === "lookup_join")).toBe(true);
        expect(explained.physicalPlan.steps.some((step) => step.kind === "local_hash_join")).toBe(
          false,
        );
        expect(derivations).toBe(0);
        const planned = explained.providerPlans
          .filter((plan) => plan.provider === "profilesDb")
          .flatMap((plan) => plan.description?.operations ?? []);
        expect(planned).toContainEqual(
          expect.objectContaining({
            kind: "sql",
            variables: ["allowed"],
          }),
        );
        const events: QueryStepEvent[] = [];
        const session = createExecutableSchemaSession(schema, {
          sql,
          context,
          options: { onEvent: (event) => events.push(event) },
        }).unwrap();
        expect(session.getPlan().steps.some((step) => step.kind === "lookup_join")).toBe(true);
        expect(await session.runToCompletion()).toEqual([
          { id: 1, author_name: "Visible" },
          { id: 2, author_name: "Visible" },
          { id: 3, author_name: null },
          { id: 4, author_name: null },
        ]);
        while (!("done" in (await session.next()))) {
          // Consume the buffered execution observations.
        }
        expect(events).toContainEqual(
          expect.objectContaining({
            kind: "lookup_join",
            routeUsed: "lookup_join",
            status: "done",
          }),
        );
        expect(statements).toHaveLength(1);
        expect(derivations).toBe(derivedKey ? 4 : 0);
        expect(statements[0]).toMatchObject({
          sql: expect.stringContaining("in (?, ?)"),
          bindings: ["allowed", 7, 8],
        });
        statements.length = 0;
        const pagedSql =
          "SELECT d.id, p.name FROM documents d LEFT JOIN paged_profiles p ON d.author_id = p.id ORDER BY d.id";
        const pagedSession = createExecutableSchemaSession(schema, {
          sql: pagedSql,
          context,
        }).unwrap();
        expect(pagedSession.getPlan().steps.some((step) => step.kind === "lookup_join")).toBe(
          false,
        );
        const pagedRows = await pagedSession.runToCompletion();
        expect(statements).toMatchObject([{ sql: expect.stringContaining("limit ? offset ?") }]);
        expect(pagedRows).toEqual([
          { id: 1, name: null },
          { id: 2, name: null },
          { id: 3, name: null },
          { id: 4, name: null },
        ]);
        expect(statements).toHaveLength(1);
        expect(statements[0]).toMatchObject({
          sql: expect.stringContaining("limit ? offset ?"),
          bindings: ["allowed", 1, 1],
        });
        expect((await schema.query({ sql: "SELECT name FROM profiles", context })).isErr()).toBe(
          true,
        );
      } finally {
        await leftDb.destroy();
        await rightDb.destroy();
      }
    },
  );
});
