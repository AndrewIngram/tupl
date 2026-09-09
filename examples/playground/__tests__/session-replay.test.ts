import { describe, expect, it } from "vite-plus/test";
import type { QueryExecutionPlan, QueryStepEvent } from "@tupl/runtime/session";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import {
  compilePlaygroundInput,
  createSession,
  replaySession,
  runSessionToCompletion,
} from "../src/session-runtime";
import { createSandboxQuerySession } from "../src/playground-sandbox-session";
import type { ExecutedProviderOperation } from "../src/types";

function isSqlProviderOperation(
  entry: ExecutedProviderOperation,
): entry is Extract<ExecutedProviderOperation, { kind: "sql_query" }> {
  return entry.kind === "sql_query";
}

describe("playground/session-replay", () => {
  it("keeps the latest static-step occurrence and retains failed proxy outcomes", async () => {
    const plan: QueryExecutionPlan = {
      steps: [
        {
          id: "scan_1",
          relNodeId: "rel_scan_1",
          kind: "scan",
          dependsOn: [],
          summary: "Scan orders",
          phase: "fetch",
          operation: { name: "scan" },
        },
      ],
    };
    const events: QueryStepEvent[] = [
      {
        id: "scan_1",
        stepId: "scan_1",
        executionId: "execution_2",
        relNodeId: "rel_scan_1",
        occurrence: 2,
        kind: "scan",
        summary: "Scan orders",
        dependsOn: [],
        executionIndex: 2,
        startedAt: 110,
        endedAt: 114,
        durationMs: 4,
        routeUsed: "scan",
        status: "failed",
        error: "provider exploded",
      },
      {
        id: "scan_1",
        stepId: "scan_1",
        executionId: "execution_1",
        relNodeId: "rel_scan_1",
        occurrence: 1,
        kind: "scan",
        summary: "Scan orders",
        dependsOn: [],
        executionIndex: 1,
        startedAt: 100,
        endedAt: 104,
        durationMs: 4,
        routeUsed: "scan",
        status: "done",
        rowCount: 1,
        outputRowCount: 1,
      },
      {
        id: "execution_3",
        executionId: "execution_3",
        relNodeId: "rel_hidden_1",
        occurrence: 1,
        kind: "filter",
        summary: "Apply hidden filter",
        dependsOn: [],
        executionIndex: 3,
        startedAt: 115,
        endedAt: 116,
        durationMs: 1,
        routeUsed: "local",
        status: "done",
        rowCount: 0,
        outputRowCount: 0,
      },
    ];

    const session = createSandboxQuerySession(
      "failed_session",
      plan,
      events,
      null,
      true,
      "provider exploded",
    );

    expect(session.getStepState("scan_1")).toMatchObject({
      status: "failed",
      executionId: "execution_2",
      occurrence: 2,
      error: "provider exploded",
    });
    expect(session.getStepState("execution_3")).toBeUndefined();
    await expect(session.next()).rejects.toThrow("provider exploded");
  });

  it("replays to a specific step count deterministically", { timeout: 35_000 }, async () => {
    const scenario = SCENARIO_PRESETS[0];
    const query = QUERY_PRESETS[0];
    if (!scenario || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = await compilePlaygroundInput(
      DEFAULT_FACADE_SCHEMA_CODE,
      serializeJson(scenario.rows),
      query.sql,
    );
    if (!compiled.ok) {
      throw new Error(compiled.issues.join("\n"));
    }

    const liveBundle = await createSession(compiled, scenario.context);
    const liveSession = liveBundle.session;
    const first = await liveSession.next();
    if ("done" in first) {
      throw new Error("Expected at least one step event.");
    }

    const replayed = await replaySession(compiled, 1, scenario.context, { reseed: false });
    expect(replayed.events).toHaveLength(1);
    expect(replayed.events[0]?.id).toBe(first.id);
  });

  it(
    "runToCompletion helper matches done state and returns rows",
    { timeout: 15_000 },
    async () => {
      const scenario = SCENARIO_PRESETS[1];
      const query = QUERY_PRESETS[1];
      if (!scenario || !query) {
        throw new Error("Expected example pack with at least one query.");
      }

      const compiled = await compilePlaygroundInput(
        DEFAULT_FACADE_SCHEMA_CODE,
        serializeJson(scenario.rows),
        query.sql,
      );
      if (!compiled.ok) {
        throw new Error(compiled.issues.join("\n"));
      }

      const bundle = await createSession(compiled, scenario.context);
      const snapshot = await runSessionToCompletion(bundle.session, []);

      expect(snapshot.done).toBe(true);
      expect(snapshot.result).not.toBeNull();
      expect((snapshot.result ?? []).length).toBeGreaterThan(0);
      expect(Array.isArray(snapshot.executedOperations)).toBe(true);
      expect(snapshot.executedOperations.filter(isSqlProviderOperation).length).toBeGreaterThan(0);
    },
  );
});
