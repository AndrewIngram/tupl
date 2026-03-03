import { describe, expect, it } from "vitest";

import { FACADE_SCHEMA, QUERY_PRESETS, SCENARIO_PRESETS, serializeJson } from "../src/examples";
import {
  compilePlaygroundInput,
  createSession,
  replaySession,
  runSessionToCompletion,
} from "../src/session-runtime";

describe("playground/session-replay", () => {
  it("replays to a specific step count deterministically", async () => {
    const scenario = SCENARIO_PRESETS[0];
    const query = QUERY_PRESETS[0];
    if (!scenario || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(FACADE_SCHEMA),
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

    const replayed = await replaySession(compiled, 1, scenario.context);
    expect(replayed.events).toHaveLength(1);
    expect(replayed.events[0]?.id).toBe(first.id);
  });

  it("runToCompletion helper matches done state and returns rows", async () => {
    const scenario = SCENARIO_PRESETS[1];
    const query = QUERY_PRESETS[1];
    if (!scenario || !query) {
      throw new Error("Expected example pack with at least one query.");
    }

    const compiled = compilePlaygroundInput(
      serializeJson(FACADE_SCHEMA),
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
  });
});
