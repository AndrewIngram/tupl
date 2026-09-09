import {
  DummyDriver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from "kysely";
import { describe, expect, it } from "vite-plus/test";
import { createExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder } from "@tupl/schema-model";
import { createKyselyProvider } from "@tupl/provider-kysely";
import type { KyselyProviderEntityConfig } from "@tupl/provider-kysely";

describe("Kysely native SQL descriptions", () => {
  it("compiles scoped SQL and positional bindings without connecting", async () => {
    const db = new Kysely<{ documents: { id: number; tenant: string } }>({
      dialect: {
        createAdapter: () => new PostgresAdapter(),
        createDriver: () => new DummyDriver(),
        createIntrospector: (db) => new PostgresIntrospector(db),
        createQueryCompiler: () => new PostgresQueryCompiler(),
      },
    });
    const provider = createKyselyProvider<
      { tenant: string },
      { documents: { id: number; tenant: string } },
      { documents: KyselyProviderEntityConfig<{ tenant: string }> }
    >({
      db: (_context: { tenant: string }) => db,
      entities: {
        documents: {
          table: "documents",
          base: ({ query, context, alias }) => query.where(`${alias}.tenant`, "=", context.tenant),
        },
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
        sql: 'select "documents"."id" as "id" from (select * from "documents" as "documents" where "documents"."tenant" = $1) as "documents" where "documents"."id" = $2',
        variables: ["allowed", 1],
      }),
    );
    await db.destroy();
  });
});
