import { Result, type Result as BetterResult } from "better-result";
import { describe, expect, it, vi } from "vite-plus/test";

import type { RelNode } from "@tupl/foundation";
import {
  createDataEntityHandle,
  type ProviderAdapter,
  type ProviderCompiledPlan,
} from "@tupl/provider-kit";
import { createExecutableSchema } from "@tupl/runtime";
import {
  createExecutableSchemaSession,
  type QuerySession,
  type QueryStepEvent,
  type QuerySessionOptions,
} from "@tupl/runtime/session";
import { createSchemaBuilder, type QueryRow } from "@tupl/schema-model";

const EMPTY_CONTEXT = {} as const;

function createProviderSession(input: {
  rows?: QueryRow[];
  canExecute?: ProviderAdapter<typeof EMPTY_CONTEXT>["canExecute"];
  compile?: ProviderAdapter<typeof EMPTY_CONTEXT>["compile"];
  execute?: ProviderAdapter<typeof EMPTY_CONTEXT>["execute"];
  options?: QuerySessionOptions;
  timeoutMs?: number;
  sql?: string;
}) {
  const provider = {
    name: "warehouse",
    canExecute: input.canExecute ?? (() => true),
    compile:
      input.compile ??
      (async (rel: RelNode) => Result.ok({ provider: "warehouse", kind: "rel", payload: rel })),
    execute: input.execute ?? (() => Result.ok(input.rows ?? [{ id: "u1" }])),
  } satisfies ProviderAdapter<typeof EMPTY_CONTEXT>;

  const builder = createSchemaBuilder<typeof EMPTY_CONTEXT>();
  builder.table(
    "users",
    createDataEntityHandle({
      entity: "users",
      provider: provider.name,
      providerInstance: provider,
    }),
    {
      columns: {
        id: "text",
      },
    },
  );

  const executableResult = createExecutableSchema(builder);
  if (Result.isError(executableResult)) {
    throw executableResult.error;
  }

  const sessionResult = createExecutableSchemaSession(executableResult.value, {
    context: EMPTY_CONTEXT,
    sql: input.sql ?? "SELECT id FROM users",
    ...(input.options ? { options: input.options } : {}),
    ...(input.timeoutMs != null ? { queryGuardrails: { timeoutMs: input.timeoutMs } } : {}),
  });
  if (Result.isError(sessionResult)) {
    throw sessionResult.error;
  }

  return sessionResult.value;
}

function createJoinSession(input: {
  execute: ProviderAdapter<typeof EMPTY_CONTEXT>["execute"];
  options?: QuerySessionOptions;
}) {
  const provider = {
    name: "warehouse",
    canExecute: (rel: RelNode) => rel.kind === "scan",
    async compile(rel: RelNode) {
      return Result.ok({ provider: "warehouse", kind: "rel", payload: rel });
    },
    execute: input.execute,
  } satisfies ProviderAdapter<typeof EMPTY_CONTEXT>;

  const builder = createSchemaBuilder<typeof EMPTY_CONTEXT>();
  builder.table(
    "users",
    createDataEntityHandle({
      entity: "users",
      provider: provider.name,
      providerInstance: provider,
    }),
    { columns: { id: "text", team_id: "text" } },
  );
  builder.table(
    "teams",
    createDataEntityHandle({
      entity: "teams",
      provider: provider.name,
      providerInstance: provider,
    }),
    { columns: { id: "text", name: "text" } },
  );

  const executableResult = createExecutableSchema(builder);
  if (Result.isError(executableResult)) {
    throw executableResult.error;
  }
  const sessionResult = createExecutableSchemaSession(executableResult.value, {
    context: EMPTY_CONTEXT,
    sql: "SELECT u.id, t.name FROM users u JOIN teams t ON u.team_id = t.id",
    ...(input.options ? { options: input.options } : {}),
  });
  if (Result.isError(sessionResult)) {
    throw sessionResult.error;
  }
  return sessionResult.value;
}

function createEmptySchemaSession(sql: string) {
  const executableResult = createExecutableSchema(createSchemaBuilder<typeof EMPTY_CONTEXT>());
  if (Result.isError(executableResult)) {
    throw executableResult.error;
  }
  const sessionResult = createExecutableSchemaSession(executableResult.value, {
    context: EMPTY_CONTEXT,
    sql,
  });
  if (Result.isError(sessionResult)) {
    throw sessionResult.error;
  }
  return sessionResult.value;
}

function readCompiledRel(compiled: ProviderCompiledPlan): RelNode {
  if (!compiled.payload || typeof compiled.payload !== "object" || !("kind" in compiled.payload)) {
    throw new Error("Expected a relational node payload.");
  }
  return compiled.payload as RelNode;
}

function createDeferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

async function drainEvents(session: QuerySession): Promise<QueryStepEvent[]> {
  const events: QueryStepEvent[] = [];
  while (true) {
    const next = await session.next();
    if ("done" in next) {
      return events;
    }
    events.push(next);
  }
}

async function collectError(session: QuerySession) {
  try {
    await session.runToCompletion();
  } catch (error) {
    return error;
  }
  throw new Error("Expected session execution to fail.");
}

describe("query session correctness", () => {
  it("shares one provider execution across concurrent completion calls", async () => {
    let executeCalls = 0;
    const rows = [{ id: "u1" }];
    const session = createProviderSession({
      async execute() {
        executeCalls += 1;
        await Promise.resolve();
        return Result.ok(rows);
      },
    });

    const [first, second] = await Promise.all([
      session.runToCompletion(),
      session.runToCompletion(),
    ]);

    expect(first).toEqual(rows);
    expect(second).toEqual(rows);
    expect(executeCalls).toBe(1);
    expect(session.getResult()).toEqual(rows);
  });

  it("shares provider execution between next and completion calls", async () => {
    let executeCalls = 0;
    const rows = [{ id: "u1" }];
    const session = createProviderSession({
      async execute() {
        executeCalls += 1;
        await Promise.resolve();
        return Result.ok(rows);
      },
    });

    const [next, completed] = await Promise.all([session.next(), session.runToCompletion()]);

    expect("done" in next).toBe(false);
    expect(completed).toEqual(rows);
    expect(executeCalls).toBe(1);
  });

  it("retains the original provider failure for every execution request", async () => {
    let executeCalls = 0;
    const session = createProviderSession({
      execute() {
        executeCalls += 1;
        return Result.err(new Error("warehouse unavailable"));
      },
    });

    const first = await collectError(session);
    const second = await collectError(session);

    expect(first).toBe(second);
    expect(first).toMatchObject({
      _tag: "TuplRuntimeError",
      message: "warehouse unavailable",
    });
    expect(executeCalls).toBe(1);
    expect(session.getResult()).toBeNull();
  });

  it("shares one forced-local execution across concurrent completion calls", async () => {
    let executeCalls = 0;
    const rows = [{ id: "u1" }];
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
      async execute() {
        executeCalls += 1;
        await Promise.resolve();
        return Result.ok(rows);
      },
    });

    const [first, second] = await Promise.all([
      session.runToCompletion(),
      session.runToCompletion(),
    ]);

    expect(first).toEqual(rows);
    expect(second).toEqual(rows);
    expect(executeCalls).toBe(1);
  });

  it("retains the original forced-local failure for every execution request", async () => {
    let executeCalls = 0;
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
      execute() {
        executeCalls += 1;
        return Result.err(new Error("local scan unavailable"));
      },
    });

    const first = await collectError(session);
    const second = await collectError(session);

    expect(first).toBe(second);
    expect(first).toMatchObject({
      _tag: "TuplExecutionError",
      message: "local scan unavailable",
    });
    expect(executeCalls).toBe(1);
  });

  it("retains successful empty results", async () => {
    let executeCalls = 0;
    const session = createProviderSession({
      rows: [],
      execute() {
        executeCalls += 1;
        return Result.ok([]);
      },
    });

    expect(session.getResult()).toBeNull();
    expect(await session.runToCompletion()).toEqual([]);
    expect(session.getResult()).toEqual([]);
    expect(await session.runToCompletion()).toEqual([]);
    expect(executeCalls).toBe(1);
  });

  it("reserves concurrent next events exactly once", async () => {
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
    });
    const stepCount = session.getPlan().steps.length;

    const events = await Promise.all(Array.from({ length: stepCount }, async () => session.next()));

    expect(events.every((event) => !("done" in event))).toBe(true);
    const eventIds = new Set(events.map((event) => ("done" in event ? "done" : event.id)));
    expect(eventIds.size).toBe(stepCount);
    expect(events.map((event) => ("done" in event ? 0 : event.executionIndex))).toEqual(
      Array.from({ length: stepCount }, (_, index) => index + 1),
    );
    await expect(session.next()).resolves.toEqual({
      done: true,
      result: [{ id: "u1" }],
    });
  });

  it("does not replay an event when its observer throws", async () => {
    const observerError = new Error("observer failed");
    let callbackCalls = 0;
    const session = createProviderSession({
      rows: [{ id: "u1" }],
      options: {
        onEvent() {
          callbackCalls += 1;
          throw observerError;
        },
      },
    });

    await expect(session.next()).rejects.toBe(observerError);
    expect(session.getResult()).toEqual([{ id: "u1" }]);
    await expect(session.next()).resolves.toEqual({
      done: true,
      result: [{ id: "u1" }],
    });
    await expect(session.runToCompletion()).resolves.toEqual([{ id: "u1" }]);
    expect(callbackCalls).toBe(1);
  });

  it("advances a forced-local event before invoking its observer", async () => {
    const observerError = new Error("observer failed");
    const observedIds: string[] = [];
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
      options: {
        onEvent(event) {
          observedIds.push(event.id);
          if (observedIds.length === 1) {
            throw observerError;
          }
        },
      },
    });

    await expect(session.next()).rejects.toBe(observerError);
    const next = await session.next();
    expect("done" in next).toBe(false);
    if ("done" in next) {
      throw new Error("Expected the next unconsumed event.");
    }
    expect(next.id).not.toBe(observedIds[0]);
    await expect(session.runToCompletion()).resolves.toEqual([{ id: "u1" }]);
  });

  it("publishes actual child completion before a later provider operation finishes", async () => {
    const teams = createDeferred<BetterResult<QueryRow[], never>>();
    let teamsFinished = false;
    let now = 100;
    const nowSpy = vi.spyOn(Date, "now").mockImplementation(() => now);
    try {
      const session = createJoinSession({
        async execute(compiled) {
          const rel = readCompiledRel(compiled);
          if (rel.kind === "scan" && rel.table === "users") {
            return Result.ok([{ id: "u1", team_id: "t1" }]);
          }
          const result = await teams.promise;
          teamsFinished = true;
          return result;
        },
      });

      const first = await session.next();
      expect("done" in first).toBe(false);
      if ("done" in first) {
        throw new Error("Expected a child completion event.");
      }
      expect(first.summary).toContain("users");
      expect(first.status).toBe("done");
      expect(teamsFinished).toBe(false);
      expect(first.stepId).toBeDefined();
      expect(session.getStepState(first.stepId ?? "")?.status).toBe("done");

      const teamsStep = session
        .getPlan()
        .steps.find((step) => step.summary.includes("teams") && step.relNodeId);
      expect(teamsStep).toBeDefined();
      expect(session.getStepState(teamsStep?.id ?? "")?.status).toBe("running");

      now = 175;
      teams.resolve(Result.ok([{ id: "t1", name: "Core" }]));
      await expect(session.runToCompletion()).resolves.toEqual([{ id: "u1", name: "Core" }]);
      const remainingEvents = await drainEvents(session);
      const teamsEvent = remainingEvents.find((event) => event.stepId === teamsStep?.id);
      const joinEvent = remainingEvents.find((event) => event.kind === "join");
      expect(first.durationMs).toBe(0);
      expect(teamsEvent?.durationMs).toBe(75);
      expect(joinEvent?.durationMs).toBe(75);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it("keeps events buffered when runToCompletion finishes first", async () => {
    let callbackCalls = 0;
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
      options: {
        onEvent() {
          callbackCalls += 1;
        },
      },
    });

    await expect(session.runToCompletion()).resolves.toEqual([{ id: "u1" }]);
    expect(callbackCalls).toBe(0);
    const events = await drainEvents(session);
    expect(events.length).toBeGreaterThan(0);
    expect(callbackCalls).toBe(events.length);
  });

  it("records repeated recursive execution as distinct occurrences", async () => {
    const session = createEmptySchemaSession(`
      WITH RECURSIVE numbers AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 1 AS n FROM numbers WHERE n < 3
      )
      SELECT n FROM numbers ORDER BY n ASC
    `);

    await expect(session.runToCompletion()).resolves.toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    const events = await drainEvents(session);
    const occurrencesByNode = new Map<string, number[]>();
    for (const event of events) {
      const occurrences = occurrencesByNode.get(event.relNodeId) ?? [];
      occurrences.push(event.occurrence);
      occurrencesByNode.set(event.relNodeId, occurrences);
    }
    const repeated = [...occurrencesByNode.entries()].find(([, occurrences]) =>
      occurrences.includes(2),
    );
    expect(repeated).toBeDefined();
    expect(new Set(events.map((event) => event.executionId)).size).toBe(events.length);

    const groupingStep = session.getPlan().steps.find((step) => step.kind === "cte");
    expect(groupingStep).toBeDefined();
    expect(session.getStepState(groupingStep?.id ?? "")?.status).toBe("ready");
    const cteReadEvents = events.filter((event) => event.summary.startsWith("Read CTE"));
    expect(cteReadEvents.length).toBeGreaterThan(0);
    expect(cteReadEvents.every((event) => event.routeUsed === "local")).toBe(true);
  });

  it("reports local aggregate execution as local", async () => {
    const session = createProviderSession({
      sql: "SELECT COUNT(*) AS user_count FROM users",
      canExecute: (rel) => rel.kind === "scan",
    });

    const events = await drainEvents(session);
    expect(events.find((event) => event.kind === "aggregate")).toMatchObject({
      summary: "Compute grouped aggregates",
      routeUsed: "local",
    });
  });

  it("reports the local route when runtime capability differs from the static plan", async () => {
    let rootRelNodeId: string | undefined;
    const capabilityChecks = new Map<string, number>();
    const session = createProviderSession({
      sql: "SELECT id FROM users ORDER BY id",
      canExecute(rel) {
        rootRelNodeId ??= rel.id;
        if (rel.id === rootRelNodeId) {
          return false;
        }
        if (rel.kind === "scan") {
          return true;
        }
        const checks = (capabilityChecks.get(rel.id) ?? 0) + 1;
        capabilityChecks.set(rel.id, checks);
        return checks === 1 ? true : Promise.resolve(false);
      },
    });

    const plannedRemote = session
      .getPlan()
      .steps.find((step) => step.kind === "remote_fragment" && step.relNodeId !== rootRelNodeId);
    expect(plannedRemote).toBeDefined();
    const events = await drainEvents(session);
    const measured = events.find((event) => event.stepId === plannedRemote?.id);
    expect(measured).toMatchObject({
      kind: "order",
      routeUsed: "local",
      summary: "Order result rows",
    });
  });

  it("drains successful and failed actual operations before rejecting retained failure", async () => {
    const session = createJoinSession({
      execute(compiled) {
        const rel = readCompiledRel(compiled);
        return rel.kind === "scan" && rel.table === "users"
          ? Result.ok([{ id: "u1", team_id: "t1" }])
          : Result.err(new Error("teams unavailable"));
      },
    });
    const completionError = collectError(session);
    const events: QueryStepEvent[] = [];
    let nextError: unknown;
    while (!nextError) {
      try {
        const next = await session.next();
        if (!("done" in next)) {
          events.push(next);
        }
      } catch (error) {
        nextError = error;
      }
    }
    const retainedError = await completionError;

    expect(events.some((event) => event.status === "done")).toBe(true);
    expect(events.some((event) => event.status === "failed")).toBe(true);
    expect(nextError).toBe(retainedError);
    await expect(session.next()).rejects.toBe(retainedError);
  });

  it("closes observation on timeout and ignores late provider completion", async () => {
    const pending = createDeferred<BetterResult<QueryRow[], never>>();
    const session = createProviderSession({
      timeoutMs: 5,
      execute: () => pending.promise,
    });
    const completionError = collectError(session);

    const failureEvent = await session.next();
    expect("done" in failureEvent).toBe(false);
    if ("done" in failureEvent) {
      throw new Error("Expected a failure event.");
    }
    expect(failureEvent.status).toBe("failed");
    const retainedError = await completionError;
    await expect(session.next()).rejects.toBe(retainedError);

    pending.resolve(Result.ok([{ id: "late" }]));
    await Promise.resolve();
    await expect(session.next()).rejects.toBe(retainedError);
    expect(session.getStepState(failureEvent.stepId ?? "")?.status).toBe("failed");
    expect(session.getResult()).toBeNull();
  });

  it("applies the provider session timeout to compilation", async () => {
    const pendingCompile = createDeferred<BetterResult<ProviderCompiledPlan, never>>();
    let executeCalls = 0;
    const session = createProviderSession({
      timeoutMs: 5,
      compile: () => pendingCompile.promise,
      execute() {
        executeCalls += 1;
        return Result.ok([{ id: "unexpected" }]);
      },
    });
    const completionError = collectError(session);

    const failureEvent = await session.next();
    expect("done" in failureEvent).toBe(false);
    if ("done" in failureEvent) {
      throw new Error("Expected a compile timeout event.");
    }
    expect(failureEvent).toMatchObject({ status: "failed", routeUsed: "provider_fragment" });
    const retainedError = await completionError;
    await expect(session.next()).rejects.toBe(retainedError);
    expect(executeCalls).toBe(0);

    pendingCompile.resolve(
      Result.ok({ provider: "warehouse", kind: "rel", payload: { kind: "late" } }),
    );
    await Promise.resolve();
    await expect(session.next()).rejects.toBe(retainedError);
    expect(session.getStepState(failureEvent.stepId ?? "")?.status).toBe("failed");
  });

  it("captures full rows only for the final root completion", async () => {
    const session = createProviderSession({
      canExecute: (rel) => rel.kind === "scan",
      options: { captureRows: "full" },
    });
    const result = await session.runToCompletion();
    const events = await drainEvents(session);
    const captured = events.filter((event) => event.status === "done" && event.rows);

    expect(captured).toHaveLength(1);
    expect(captured[0]?.rows).toEqual(result);
    expect(captured[0]?.relNodeId).toBe(
      session.getPlan().steps.find((step) => step.id === captured[0]?.stepId)?.relNodeId,
    );
  });
});
