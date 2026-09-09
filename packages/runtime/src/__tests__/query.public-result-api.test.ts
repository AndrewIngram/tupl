import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import type { RelNode } from "@tupl/foundation";

import { createExecutableSchema, prepareRuntimeSchemaResult } from "@tupl/runtime";
import { createExecutableSchemaSession } from "@tupl/runtime/session";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";
import {
  bindProviderEntities,
  createDataEntityHandle,
  type ProviderAdapter,
} from "@tupl/provider-kit";
import { createSchemaBuilder } from "@tupl/schema-model";
import { resolveTableProvider } from "@tupl/schema-model/normalization";
import { buildEntitySchema, buildSchema } from "@tupl/test-support/schema";

function createRowsProvider<
  TEntities extends Record<string, ReturnType<typeof createDataEntityHandle>>,
>(entities: TEntities, rows: QueryRow[] = [{ id: "u1" }]) {
  const provider = {
    name: "warehouse",
    entities,
    canExecute(rel: RelNode) {
      return rel.kind === "scan";
    },
    async compile(rel: RelNode) {
      return Result.ok({
        provider: "warehouse",
        kind: "rel",
        payload: rel,
      });
    },
    async execute() {
      return Result.ok(rows);
    },
  } satisfies ProviderAdapter<Record<string, never>>;

  return bindProviderEntities(provider);
}

function createWarehouseEntity(entity: string) {
  return createDataEntityHandle({
    entity,
    provider: "warehouse",
  });
}

function unwrapResult<T, E>(result: Result<T, E>): T {
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

describe("public result APIs", () => {
  it("executes provider-owned root rel queries through the canonical executor path", async () => {
    let sawRelCompile = false;

    const provider = bindProviderEntities({
      name: "warehouse",
      entities: {
        orders: createWarehouseEntity("orders"),
        users: createWarehouseEntity("users"),
      },
      canExecute() {
        return true;
      },
      async compile(rel: RelNode) {
        sawRelCompile = true;

        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: rel,
        });
      },
      async execute() {
        return Result.ok([{ id: "o1", email: "a@example.com" }]);
      },
    } satisfies ProviderAdapter<Record<string, never>>);
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("orders", provider.entities.orders, {
      columns: {
        id: "text",
        user_id: "text",
      },
    });
    builder.table("users", provider.entities.users, {
      columns: {
        id: "text",
        email: "text",
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = await executableSchema.query({
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    });

    expect(Result.isOk(result)).toBe(true);
    if (Result.isError(result)) {
      throw new Error("Expected query to succeed.");
    }

    expect(result.value).toEqual([{ id: "o1", email: "a@example.com" }]);
    expect(sawRelCompile).toBe(true);
  });

  it("returns tagged parse errors from query", async () => {
    const provider = createRowsProvider({
      users: createWarehouseEntity("users"),
    });
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("users", provider.entities.users, {
      columns: {
        id: "text",
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = await executableSchema.query({
      context: {},
      sql: "INSERT INTO users VALUES ('u1')",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected query to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplParseError",
    });
    expect(result.error.message).toBe("Only SELECT statements are currently supported.");
  });

  it("returns tagged planning errors from query", async () => {
    const provider = createRowsProvider({
      users: createWarehouseEntity("users"),
    });
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("users", provider.entities.users, {
      columns: {
        id: "text",
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = await executableSchema.query({
      context: {},
      sql: "SELECT id FROM missing_table",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected query to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelLoweringError",
    });
    expect(result.error.message).toBe("Unknown table: missing_table");
  });

  it("returns tagged planning errors from query for invalid enum literals", async () => {
    const provider = createRowsProvider({
      orders: createWarehouseEntity("orders"),
    });
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("orders", provider.entities.orders, {
      columns: {
        id: "text",
        status: { type: "text", enum: ["draft", "paid", "void"] as const },
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = await executableSchema.query({
      context: {},
      sql: "SELECT id FROM orders WHERE status = 'unknown'",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected query to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelLoweringError",
    });
    expect(result.error.message).toBe("Invalid enum value for orders.status");
  });

  it("returns tagged rewrite errors from query for invalid view expansion", async () => {
    const schema = buildSchema((builder) => {
      builder.view("broken_view", ({ scan }) => scan("missing_table"), {
        columns: {
          id: { source: "missing_table.id" },
        },
      });
    });

    const executableSchema = unwrapResult(createExecutableSchema(schema));

    const result = await executableSchema.query({
      context: {},
      sql: "SELECT id FROM broken_view",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected query to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "RelRewriteError",
      operation: "compile planner view rel",
      message: "Unknown table in view rel scan: missing_table",
    });
  });

  it("returns tagged runtime errors from query", async () => {
    const provider = bindProviderEntities({
      name: "warehouse",
      entities: {
        users: createWarehouseEntity("users"),
      },
      canExecute(rel: RelNode) {
        return rel.kind === "scan";
      },
      async compile(rel: RelNode) {
        return Result.ok({
          provider: "warehouse",
          kind: "rel",
          payload: rel,
        });
      },
      async execute() {
        return Result.err(new Error("warehouse exploded"));
      },
    } satisfies ProviderAdapter<Record<string, never>>);
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("users", provider.entities.users, {
      columns: {
        id: "text",
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = await executableSchema.query({
      context: {},
      sql: "SELECT id FROM users",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected query to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplExecutionError",
    });
    expect(result.error.message).toBe("warehouse exploded");
  });

  it("returns tagged runtime errors when executable schema provider bindings are inconsistent", () => {
    const adapter = {
      name: "actual",
      canExecute(rel: RelNode) {
        return rel.kind === "scan";
      },
      async compile(rel: RelNode) {
        return Result.ok({
          provider: "actual",
          kind: "rel",
          payload: rel,
        });
      },
      async execute() {
        return Result.ok([{ id: "u1" }]);
      },
    } satisfies ProviderAdapter;

    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table(
      "users",
      createDataEntityHandle({
        entity: "users",
        provider: "warehouse",
        providerInstance: adapter,
      }),
      {
        columns: {
          id: "text",
        },
      },
    );

    const result = createExecutableSchema(builder);

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createExecutableSchemaResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
    });
    expect(result.error.message).toBe(
      "Table users is bound to provider warehouse, but the attached provider is named actual.",
    );
  });

  it("returns tagged setup errors from createExecutableSchemaSessionResult", () => {
    const provider = createRowsProvider({
      users: createWarehouseEntity("users"),
    });
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table("users", provider.entities.users, {
      columns: {
        id: "text",
      },
    });
    const executableSchema = unwrapResult(createExecutableSchema(builder));

    const result = createExecutableSchemaSession(executableSchema, {
      context: {},
      sql: "INSERT INTO users VALUES ('u1')",
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createSessionResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplParseError",
      message: "Only SELECT statements are currently supported.",
    });
  });

  it("returns tagged errors from createExecutableSchemaResult", () => {
    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table(
      "users",
      createDataEntityHandle({
        entity: "users",
        provider: "warehouse",
      }),
      {
        columns: {
          id: "text",
        },
      },
    );

    const result = createExecutableSchema(builder);
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected createExecutableSchemaResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
      message:
        "Table users must be declared from a provider-owned entity via table(name, provider.entities.someTable, config).",
    });
  });

  it("returns tagged provider binding errors during runtime schema preparation", () => {
    const schema = buildEntitySchema({
      users: {
        provider: "warehouse",
        columns: {
          id: "text",
        },
      },
    });

    const result = prepareRuntimeSchemaResult({
      schema,
      providers: {},
    });

    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected prepareRuntimeSchemaResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Table users is bound to provider warehouse, but no such provider is registered.",
    });
  });
});

describe("resolveTableProvider", () => {
  it("returns a tagged error for views without a direct provider", () => {
    const schema = buildSchema((builder) => {
      builder.view("active_users", ({ scan }) => scan("users"), {
        columns: {
          id: { source: "users.id" },
        },
      });
    });

    const result = resolveTableProvider(schema, "active_users");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "View table active_users does not have a direct provider binding.",
    });
  });

  it("returns a tagged error for unknown tables", () => {
    const schema: SchemaDefinition = {
      tables: {},
    };

    const result = resolveTableProvider(schema, "missing");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Unknown table: missing",
    });
  });

  it("returns a tagged error when a table is missing a provider mapping", () => {
    const schema: SchemaDefinition = {
      tables: {
        users: {
          columns: {
            id: "text",
          },
        },
      },
    };

    const result = resolveTableProvider(schema, "users");
    expect(Result.isError(result)).toBe(true);
    if (Result.isOk(result)) {
      throw new Error("Expected resolveTableProviderResult to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Table users is missing required provider mapping.",
    });
  });
});
