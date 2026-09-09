import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";

import { createValuesRel, type RelNode } from "@tupl/foundation";
import { createDataEntityHandle, type ProviderAdapter } from "@tupl/provider-kit";
import type {
  LookupManyCapableProviderAdapter,
  ProviderLookupManyRequest,
} from "@tupl/provider-kit/shapes";
import { createExecutableSchema, type ExecutableSchema } from "@tupl/runtime";
import { executeRelWithProvidersResult } from "@tupl/runtime/executor";
import { createExecutableSchemaSession } from "@tupl/runtime/session";
import { createSchemaBuilder, type QueryRow, type SchemaDefinition } from "@tupl/schema-model";

const EMPTY_CONTEXT = {} as const;

function expectRowLimitError(result: unknown) {
  expect(result).toMatchObject({
    error: {
      _tag: "TuplGuardrailError",
      guardrail: "maxExecutionRows",
    },
  });
}

function createUsersExecutable(input: { rows: QueryRow[]; canExecute: (rel: RelNode) => boolean }) {
  const provider = {
    name: "warehouse",
    canExecute: input.canExecute,
    async compile(rel: RelNode) {
      return Result.ok({ provider: "warehouse", kind: "rel", payload: rel });
    },
    execute() {
      return Result.ok(input.rows);
    },
  } satisfies ProviderAdapter<typeof EMPTY_CONTEXT>;

  const builder = createSchemaBuilder<typeof EMPTY_CONTEXT>();
  builder.table(
    "users",
    createDataEntityHandle({
      entity: "users",
      provider: provider.name,
      providerInstance: provider,
    }),
    { columns: { id: "text" } },
  );

  const result = createExecutableSchema(builder);
  if (Result.isError(result)) {
    throw result.error;
  }
  return result.value;
}

async function executeLocalRel(rel: RelNode, schema: SchemaDefinition, maxExecutionRows: number) {
  return executeRelWithProvidersResult(rel, schema, {}, EMPTY_CONTEXT, {
    maxExecutionRows,
    maxLookupKeysPerBatch: 10,
    maxLookupBatches: 10,
  });
}

function createLocalSchema() {
  return createUsersExecutable({ rows: [], canExecute: () => false }).schema;
}

function createLookupExecutable() {
  let lookupCalls = 0;
  const ordersProvider = {
    name: "orders_provider",
    canExecute: (rel: RelNode) => rel.kind === "scan",
    async compile(rel: RelNode) {
      return Result.ok({ provider: "orders_provider", kind: "rel", payload: rel });
    },
    execute() {
      return Result.ok([
        { id: "o1", user_id: "u1" },
        { id: "o2", user_id: "u2" },
        { id: "o3", user_id: "u3" },
      ]);
    },
  } satisfies ProviderAdapter<typeof EMPTY_CONTEXT>;
  const usersProvider = {
    name: "users_provider",
    canExecute: (rel: RelNode) => rel.kind === "scan",
    async compile(rel: RelNode) {
      return Result.ok({ provider: "users_provider", kind: "rel", payload: rel });
    },
    execute() {
      return Result.ok([]);
    },
    lookupMany(request: ProviderLookupManyRequest) {
      lookupCalls += 1;
      const id = request.keys[0] ?? "missing";
      const idText = typeof id === "string" ? id : "missing";
      return Result.ok([
        { id, email: `${idText}-a@example.com` },
        { id, email: `${idText}-b@example.com` },
      ]);
    },
  } satisfies ProviderAdapter<typeof EMPTY_CONTEXT> &
    LookupManyCapableProviderAdapter<typeof EMPTY_CONTEXT>;

  const builder = createSchemaBuilder<typeof EMPTY_CONTEXT>();
  builder.table(
    "orders",
    createDataEntityHandle({
      entity: "orders",
      provider: ordersProvider.name,
      providerInstance: ordersProvider,
    }),
    { columns: { id: "text", user_id: "text" } },
  );
  builder.table(
    "users",
    createDataEntityHandle({
      entity: "users",
      provider: usersProvider.name,
      providerInstance: usersProvider,
    }),
    { columns: { id: "text", email: "text" } },
  );

  const result = createExecutableSchema(builder);
  if (Result.isError(result)) {
    throw result.error;
  }
  return { executable: result.value, getLookupCalls: () => lookupCalls };
}

describe("materialization guardrails", () => {
  it("rejects oversized values before LIMIT can shrink them", async () => {
    const values = createValuesRel([[1], [2]], [{ name: "id" }]);
    const rel: RelNode = {
      id: "limit_1",
      kind: "limit_offset",
      convention: "local",
      input: values,
      limit: 1,
      output: values.output,
    };

    expectRowLimitError(await executeLocalRel(rel, createLocalSchema(), 1));
  });

  it("rejects oversized aggregate input before COUNT can shrink it", async () => {
    const values = createValuesRel([[1], [2]], [{ name: "id" }]);
    const rel: RelNode = {
      id: "aggregate_1",
      kind: "aggregate",
      convention: "local",
      input: values,
      groupBy: [],
      metrics: [{ fn: "count", as: "count" }],
      output: [{ name: "count" }],
    };

    expectRowLimitError(await executeLocalRel(rel, createLocalSchema(), 1));
  });

  it("stops many-to-many join growth at the row limit", async () => {
    const left = createValuesRel([[1], [1], [1]], [{ name: "key" }]);
    const right = createValuesRel([[1], [1], [1]], [{ name: "key" }]);
    const rel: RelNode = {
      id: "join_1",
      kind: "join",
      convention: "local",
      joinType: "inner",
      left,
      right,
      leftKey: { column: "key" },
      rightKey: { column: "key" },
      output: [{ name: "key" }],
    };

    const result = await executeLocalRel(rel, createLocalSchema(), 3);
    expectRowLimitError(result);
    expect(result).toMatchObject({ error: { actual: 4, limit: 3 } });
  });

  it("checks UNION DISTINCT while adding unique rows", async () => {
    const left = createValuesRel([[1], [2]], [{ name: "id" }]);
    const right = createValuesRel([[3], [4]], [{ name: "id" }]);
    const rel: RelNode = {
      id: "union_1",
      kind: "set_op",
      convention: "local",
      op: "union",
      left,
      right,
      output: [{ name: "id" }],
    };

    const result = await executeLocalRel(rel, createLocalSchema(), 3);
    expectRowLimitError(result);
    expect(result).toMatchObject({ error: { actual: 4, limit: 3 } });
  });

  it("checks recursive CTE accumulation before growing the retained rows", async () => {
    const seed = createValuesRel([[1]], [{ name: "id" }]);
    const rel: RelNode = {
      id: "repeat_1",
      kind: "repeat_union",
      convention: "local",
      cteName: "numbers",
      mode: "union_all",
      seed,
      iterative: {
        id: "numbers_ref",
        kind: "cte_ref",
        convention: "local",
        name: "numbers",
        select: ["id"],
        output: [{ name: "id" }],
      },
      output: [{ name: "id" }],
    };

    const result = await executeLocalRel(rel, createLocalSchema(), 3);
    expectRowLimitError(result);
    expect(result).toMatchObject({ error: { actual: 4, limit: 3 } });
  });

  it("accepts a materialization exactly at the limit", async () => {
    const values = createValuesRel([[1], [2]], [{ name: "id" }]);

    const result = await executeLocalRel(values, createLocalSchema(), 2);
    expect(result).toEqual(Result.ok([{ id: 1 }, { id: 2 }]));
  });

  it("checks provider-root rows before logical output mapping", async () => {
    let rowReads = 0;
    const firstRow: QueryRow = {
      get id() {
        rowReads += 1;
        return "u1";
      },
    };
    const executable = createUsersExecutable({
      rows: [firstRow, { id: "u2" }],
      canExecute: () => true,
    });

    const result = await executable.query({
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM users",
      queryGuardrails: { maxExecutionRows: 1 },
    });

    expectRowLimitError(result);
    expect(rowReads).toBe(0);
  });

  it("checks nested provider fragments before the local parent runs", async () => {
    const executable = createUsersExecutable({
      rows: [{ id: "u1" }, { id: "u2" }],
      canExecute: (rel) => rel.kind === "scan" || rel.kind === "project",
    });

    const result = await executable.query({
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM users ORDER BY id",
      queryGuardrails: { maxExecutionRows: 1 },
    });

    expectRowLimitError(result);
  });

  it("stops lookup requests when the combined lookup buffer exceeds the limit", async () => {
    const { executable, getLookupCalls } = createLookupExecutable();

    const result = await executable.query({
      context: EMPTY_CONTEXT,
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      queryGuardrails: {
        maxExecutionRows: 3,
        maxLookupKeysPerBatch: 1,
      },
    });

    expectRowLimitError(result);
    expect(result).toMatchObject({ error: { actual: 4, limit: 3 } });
    expect(getLookupCalls()).toBe(2);
  });

  it("rejects invalid row limits at query, session, and executor boundaries", async () => {
    const executable: ExecutableSchema<typeof EMPTY_CONTEXT> = createUsersExecutable({
      rows: [{ id: "u1" }],
      canExecute: () => true,
    });

    for (const maxExecutionRows of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const queryResult = await executable.query({
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users",
        queryGuardrails: { maxExecutionRows },
      });
      expectRowLimitError(queryResult);

      const sessionResult = createExecutableSchemaSession(executable, {
        context: EMPTY_CONTEXT,
        sql: "SELECT id FROM users",
        queryGuardrails: { maxExecutionRows },
      });
      expectRowLimitError(sessionResult);

      const executorResult = await executeLocalRel(
        createValuesRel([[1]], [{ name: "id" }]),
        executable.schema,
        maxExecutionRows,
      );
      expectRowLimitError(executorResult);
    }
  });
});
