import { describe, expect, it } from "vitest";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import { compilePlaygroundInput, createSession, runSessionToCompletion } from "../src/session-runtime";

describe("playground/provider-pushdown", () => {
  it("executes simple same-provider joins and grouped aggregates as a single downstream query", async () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Missing scenario preset.");
    }

    const pushdownPresetIds = [
      "orders_with_vendors",
      "vendor_spend",
      "status_distinct",
      "paid_orders",
    ] as const;

    for (const presetId of pushdownPresetIds) {
      const preset = QUERY_PRESETS.find((query) => query.id === presetId);
      if (!preset) {
        throw new Error(`Missing query preset: ${presetId}`);
      }

      const compiled = await compilePlaygroundInput(
        DEFAULT_FACADE_SCHEMA_CODE,
        serializeJson(scenario.rows),
        preset.sql,
      );

      expect(compiled.ok).toBe(true);
      if (!compiled.ok) {
        continue;
      }

      const bundle = await createSession(compiled, scenario.context);
      const plan = bundle.session.getPlan();

      expect(plan.steps).toHaveLength(1);
      expect(plan.steps[0]?.kind).toBe("remote_fragment");
      expect(plan.steps[0]?.request).toEqual({
        fragment: "rel",
      });

      const snapshot = await runSessionToCompletion(bundle.session, []);
      expect(snapshot.executedOperations).toHaveLength(1);
      expect(snapshot.executedOperations[0]?.kind).toBe("sql_query");
      expect(snapshot.executedOperations[0]?.provider).toBe("dbProvider");
      const sqlText = snapshot.executedOperations[0]?.kind === "sql_query"
        ? snapshot.executedOperations[0].sql.toLowerCase()
        : "";
      if (presetId === "orders_with_vendors" || presetId === "vendor_spend") {
        expect(sqlText).toContain(" join ");
      }
      if (presetId === "vendor_spend") {
        expect(sqlText).toContain("group by");
      }
      if (presetId === "status_distinct") {
        expect(sqlText).toContain("select distinct");
        expect(sqlText).toContain("order by");
      }
    }
  });
});
