import { eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/pg-proxy";
import { integer, pgTable, text } from "drizzle-orm/pg-core";
import { describe, expect, it } from "vite-plus/test";
import { createExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder } from "@tupl/schema-model";
import { createDrizzleProvider } from "@tupl/provider-drizzle";

describe("Drizzle native SQL descriptions", () => {
  it("compiles scoped SQL and bindings without executing the proxy", async () => {
    let executions = 0;
    const db = drizzle(async () => {
      executions += 1;
      throw new Error("Explain must not execute queries");
    });
    const documents = pgTable("documents", { id: integer("id"), tenant: text("tenant") });
    const provider = createDrizzleProvider({
      dialect: "postgres",
      db: (_context: { tenant: string }) => db,
      tables: {
        documents: { table: documents, scope: ({ tenant }) => eq(documents.tenant, tenant) },
      },
    });
    const builder = createSchemaBuilder<{ tenant: string }>();
    builder.table("documents", provider.entities.documents, { columns: { id: "integer" } });
    const schema = createExecutableSchema(builder).unwrap();
    const explained = (
      await schema.explain({
        sql: "SELECT id FROM documents WHERE id = 1",
        context: { tenant: "allowed" },
      })
    ).unwrap();
    expect(
      explained.providerPlans.flatMap((plan) => plan.description?.operations ?? []),
    ).toContainEqual(
      expect.objectContaining({
        kind: "sql",
        sql: expect.stringContaining('"tenant" = $1'),
        variables: ["allowed", 1],
      }),
    );
    expect(executions).toBe(0);
  });
});
