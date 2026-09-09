import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";

import type { RelNode } from "@tupl/foundation";
import { createDataEntityHandle, type ProviderAdapter } from "@tupl/provider-kit";
import { createExecutableSchema, type QueryGuardrails } from "@tupl/runtime";
import { executeRelWithProvidersResult } from "@tupl/runtime/executor";
import { createExecutableSchemaSession } from "@tupl/runtime/session";
import { createSchemaBuilder } from "@tupl/schema-model";

const EMPTY_CONTEXT = {} as const;
const INVALID_INTEGER_LIMITS = [
  0,
  -1,
  1.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
  Number.MAX_SAFE_INTEGER + 1,
] as const;
const QUERY_INTEGER_GUARDRAILS = [
  "maxPlannerNodes",
  "maxExecutionRows",
  "maxLookupKeysPerBatch",
  "maxLookupBatches",
] as const satisfies readonly (keyof QueryGuardrails)[];
const EXECUTION_INTEGER_GUARDRAILS = [
  "maxExecutionRows",
  "maxLookupKeysPerBatch",
  "maxLookupBatches",
] as const;

type QueryIntegerGuardrail = (typeof QUERY_INTEGER_GUARDRAILS)[number];
type ExecutionIntegerGuardrail = (typeof EXECUTION_INTEGER_GUARDRAILS)[number];

function createQueryGuardrailOverride(
  guardrail: QueryIntegerGuardrail,
  value: number,
): Partial<QueryGuardrails> {
  switch (guardrail) {
    case "maxPlannerNodes":
      return { maxPlannerNodes: value };
    case "maxExecutionRows":
      return { maxExecutionRows: value };
    case "maxLookupKeysPerBatch":
      return { maxLookupKeysPerBatch: value };
    case "maxLookupBatches":
      return { maxLookupBatches: value };
  }
}

function createRuntimeFixture() {
  let providerCalls = 0;
  const provider = {
    name: "warehouse",
    canExecute() {
      providerCalls += 1;
      return true;
    },
    async compile(rel: RelNode) {
      providerCalls += 1;
      return Result.ok({ provider: "warehouse", kind: "rel", payload: rel });
    },
    execute() {
      providerCalls += 1;
      return Result.ok([{ id: "u1" }]);
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

  const executableResult = createExecutableSchema(builder);
  if (Result.isError(executableResult)) {
    throw executableResult.error;
  }

  const rel = {
    id: "scan_users",
    kind: "scan",
    convention: "local",
    table: "users",
    select: ["id"],
    output: [{ name: "users.id" }],
  } satisfies RelNode;

  return {
    executable: executableResult.value,
    getProviderCalls: () => providerCalls,
    provider,
    rel,
  };
}

function expectGuardrailConfigurationError(result: unknown, guardrail: string) {
  expect(result).toMatchObject({
    error: {
      _tag: "TuplGuardrailError",
      guardrail,
    },
  });
}

describe("guardrail configuration", () => {
  it("rejects invalid integer limits at the query boundary before provider work", async () => {
    for (const guardrail of QUERY_INTEGER_GUARDRAILS) {
      for (const value of INVALID_INTEGER_LIMITS) {
        const fixture = createRuntimeFixture();
        const result = await fixture.executable.query({
          context: EMPTY_CONTEXT,
          sql: "SELECT id FROM users",
          queryGuardrails: createQueryGuardrailOverride(guardrail, value),
        });

        expectGuardrailConfigurationError(result, guardrail);
        expect(fixture.getProviderCalls()).toBe(0);
      }
    }
  });

  it("rejects invalid integer limits at the session boundary before provider work", () => {
    for (const guardrail of QUERY_INTEGER_GUARDRAILS) {
      for (const value of INVALID_INTEGER_LIMITS) {
        const fixture = createRuntimeFixture();
        const result = createExecutableSchemaSession(fixture.executable, {
          context: EMPTY_CONTEXT,
          sql: "SELECT id FROM users",
          queryGuardrails: createQueryGuardrailOverride(guardrail, value),
        });

        expectGuardrailConfigurationError(result, guardrail);
        expect(fixture.getProviderCalls()).toBe(0);
      }
    }
  });

  it("rejects invalid integer limits at the executor boundary before provider work", async () => {
    for (const guardrail of EXECUTION_INTEGER_GUARDRAILS) {
      for (const value of INVALID_INTEGER_LIMITS) {
        const fixture = createRuntimeFixture();
        const guardrails = {
          maxExecutionRows: 1,
          maxLookupKeysPerBatch: 1,
          maxLookupBatches: 1,
          [guardrail]: value,
        } satisfies Record<ExecutionIntegerGuardrail, number>;
        const result = await executeRelWithProvidersResult(
          fixture.rel,
          fixture.executable.schema,
          { [fixture.provider.name]: fixture.provider },
          EMPTY_CONTEXT,
          guardrails,
        );

        expectGuardrailConfigurationError(result, guardrail);
        expect(fixture.getProviderCalls()).toBe(0);
      }
    }
  });

  it("accepts the largest safe integer at query, session, and executor boundaries", async () => {
    const queryFixture = createRuntimeFixture();
    const queryResult = await queryFixture.executable.query({
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM users",
      queryGuardrails: {
        maxPlannerNodes: Number.MAX_SAFE_INTEGER,
        maxExecutionRows: Number.MAX_SAFE_INTEGER,
        maxLookupKeysPerBatch: Number.MAX_SAFE_INTEGER,
        maxLookupBatches: Number.MAX_SAFE_INTEGER,
      },
    });
    expect(queryResult).toEqual(Result.ok([{ id: "u1" }]));
    expect(queryFixture.getProviderCalls()).toBeGreaterThan(0);

    const sessionFixture = createRuntimeFixture();
    const sessionResult = createExecutableSchemaSession(sessionFixture.executable, {
      context: EMPTY_CONTEXT,
      sql: "SELECT id FROM users",
      queryGuardrails: {
        maxPlannerNodes: Number.MAX_SAFE_INTEGER,
        maxExecutionRows: Number.MAX_SAFE_INTEGER,
        maxLookupKeysPerBatch: Number.MAX_SAFE_INTEGER,
        maxLookupBatches: Number.MAX_SAFE_INTEGER,
      },
    });
    if (Result.isError(sessionResult)) {
      throw sessionResult.error;
    }
    await expect(sessionResult.value.runToCompletion()).resolves.toEqual([{ id: "u1" }]);
    expect(sessionFixture.getProviderCalls()).toBeGreaterThan(0);

    const executorFixture = createRuntimeFixture();
    const executorResult = await executeRelWithProvidersResult(
      executorFixture.rel,
      executorFixture.executable.schema,
      { [executorFixture.provider.name]: executorFixture.provider },
      EMPTY_CONTEXT,
      {
        maxExecutionRows: Number.MAX_SAFE_INTEGER,
        maxLookupKeysPerBatch: Number.MAX_SAFE_INTEGER,
        maxLookupBatches: Number.MAX_SAFE_INTEGER,
      },
    );
    expect(Result.isOk(executorResult)).toBe(true);
    expect(executorFixture.getProviderCalls()).toBeGreaterThan(0);
  });
});
