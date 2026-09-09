import knex from "knex";
import { describe, expect, it } from "vite-plus/test";
import { createExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder } from "@tupl/schema-model";
import { createObjectionProvider } from "@tupl/provider-objection";

describe("Objection native SQL descriptions", () => {
  it("describes scoped SQL and bindings without running the query", async () => {
    const db = knex({
      client: "better-sqlite3",
      connection: { filename: ":memory:" },
      useNullAsDefault: true,
    });
    try {
      await db.schema.createTable("documents", (table) => {
        table.integer("id");
        table.string("tenant");
      });
      await db("documents").insert([
        { id: 1, tenant: "allowed" },
        { id: 2, tenant: "hidden" },
      ]);
      const statements: Array<{ sql: string; bindings?: readonly unknown[] }> = [];
      db.on("query", (query) => statements.push(query));
      const provider = createObjectionProvider({
        knex: (_context: { tenant: string }) => db,
        entities: {
          documents: {
            table: "documents",
            base: ({ tenant }) => db("documents").where("tenant", tenant),
          },
        },
      });
      const builder = createSchemaBuilder<{ tenant: string }>();
      builder.table("documents", provider.entities.documents, { columns: { id: "integer" } });
      const schema = createExecutableSchema(builder).unwrap();
      const input = {
        sql: "SELECT id FROM documents WHERE id = 1",
        context: { tenant: "allowed" },
      };
      const explained = (await schema.explain(input)).unwrap();
      const operation = explained.providerPlans
        .flatMap((plan) => plan.description?.operations ?? [])
        .find((operation) => operation.kind === "sql");
      expect(operation).toMatchObject({
        sql: expect.stringContaining("`tenant` = ?"),
        variables: ["allowed", 1],
      });
      expect(statements).toEqual([]);
      expect((await schema.query(input)).unwrap()).toEqual([{ id: 1 }]);
      expect(statements).toHaveLength(1);
      expect(statements[0]?.sql).toEqual(operation?.sql);
      expect(statements[0]?.bindings).toEqual(operation?.variables);
    } finally {
      await db.destroy();
    }
  });
});
