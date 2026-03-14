import { Result } from "better-result";
import { describe, expect, it } from "vitest";

import { createExecutableSchema } from "@tupl/runtime";
import { createExecutableSchemaSession } from "@tupl/runtime/session";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";
import {
  createDataEntityHandle,
  type FragmentProvider,
  type LookupProvider,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { createSchemaBuilder, resolveTableProvider } from "@tupl/schema-model";
import { buildSchema } from "@tupl/test-support/schema";

type TestProvider = FragmentProvider & Partial<Pick<LookupProvider, "lookupMany">>;

function createRowsProvider(rows: QueryRow[] = [{ id: "u1" }]): TestProvider {
  return {
    name: "warehouse",
    canExecute(fragment: ProviderFragment) {
      return fragment.kind === "scan";
    },
    async compile(fragment: ProviderFragment) {
      return Result.ok({
        provider: "warehouse",
        kind: fragment.kind,
        payload: fragment,
      });
    },
    async execute() {
      return Result.ok(rows);
    },
  };
}

function createUsersExecutableSchema(provider: TestProvider) {
  const builder = createSchemaBuilder<Record<string, never>>();
  builder.table(
    "users",
    createDataEntityHandle({
      entity: "users",
      provider: "warehouse",
      providerInstance: provider as FragmentProvider,
    }),
    {
      columns: {
        id: "text",
      },
    },
  );

  const result = createExecutableSchema(builder);
  if (Result.isError(result)) {
    throw result.error;
  }

  return result.value;
}

describe("public result APIs", () => {
  it("returns tagged parse errors from query", async () => {
    const executableSchema = createUsersExecutableSchema(createRowsProvider());

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
      message: "Only SELECT statements are currently supported.",
    });
  });

  it("returns tagged schema-normalization errors from query", async () => {
    const executableSchema = createUsersExecutableSchema(createRowsProvider());

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
      message: "Unknown table: missing_table",
    });
  });

  it("returns tagged runtime errors from query", async () => {
    const executableSchema = createUsersExecutableSchema({
      name: "warehouse",
      canExecute(fragment: ProviderFragment) {
        return fragment.kind === "scan";
      },
      async compile(fragment: ProviderFragment) {
        return Result.ok({
          provider: "warehouse",
          kind: fragment.kind,
          payload: fragment,
        });
      },
      async execute() {
        return Result.err(new Error("warehouse exploded"));
      },
    } satisfies TestProvider);

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
      message: "warehouse exploded",
    });
  });

  it("returns tagged runtime errors when executable schema provider bindings are inconsistent", () => {
    const provider = {
      name: "actual",
      canExecute(fragment: ProviderFragment) {
        return fragment.kind === "scan";
      },
      async compile(fragment: ProviderFragment) {
        return Result.ok({
          provider: "actual",
          kind: fragment.kind,
          payload: fragment,
        });
      },
      async execute() {
        return Result.ok([{ id: "u1" }]);
      },
    } satisfies FragmentProvider;

    const builder = createSchemaBuilder<Record<string, never>>();
    builder.table(
      "users",
      createDataEntityHandle({
        entity: "users",
        provider: "warehouse",
        providerInstance: provider,
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
      throw new Error("Expected createExecutableSchema to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
      message:
        "Table users is bound to provider warehouse, but the attached provider is named actual.",
    });
  });

  it("returns tagged setup errors from createExecutableSchemaSession", () => {
    const executableSchema = createUsersExecutableSchema(createRowsProvider());

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

  it("returns tagged errors from createExecutableSchema", () => {
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
      throw new Error("Expected createExecutableSchema to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplRuntimeError",
      message:
        "Table users must be declared from a provider-owned entity via table(name, provider.entities.someTable, config).",
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
      throw new Error("Expected resolveTableProvider to fail.");
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
      throw new Error("Expected resolveTableProvider to fail.");
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
      throw new Error("Expected resolveTableProvider to fail.");
    }

    expect(result.error).toMatchObject({
      _tag: "TuplProviderBindingError",
      message: "Table users is missing required provider mapping.",
    });
  });
});
