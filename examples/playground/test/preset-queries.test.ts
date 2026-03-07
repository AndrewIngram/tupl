import { describe, expect, it } from "vitest";

import {
  DEFAULT_FACADE_SCHEMA_CODE,
  QUERY_PRESETS,
  SCENARIO_PRESETS,
  serializeJson,
} from "../src/examples";
import {
  compilePreparedPlaygroundQuery,
  createSession,
  preparePlaygroundInput,
} from "../src/session-runtime";

describe("playground/preset-queries", () => {
  it("compiles and executes every query preset against every scenario", { timeout: 15_000 }, async () => {
    for (const scenario of SCENARIO_PRESETS) {
      const prepared = await preparePlaygroundInput(
        DEFAULT_FACADE_SCHEMA_CODE,
        serializeJson(scenario.rows),
      );
      expect(prepared.ok, `[${scenario.id}] scenario preparation failed`).toBe(true);
      if (!prepared.ok) {
        continue;
      }

      let reseed = true;
      for (const query of QUERY_PRESETS) {
        const compiled = compilePreparedPlaygroundQuery(prepared, query.sql);

        expect(compiled.ok, `[${scenario.id}] ${query.label} compile mismatch`).toBe(true);
        if (!compiled.ok) {
          continue;
        }

        const bundle = await createSession(compiled, scenario.context, { reseed });
        reseed = false;
        const rows = await bundle.session.runToCompletion();
        expect(
          Array.isArray(rows),
          `[${scenario.id}] ${query.label} should return rows`,
        ).toBe(true);
      }
    }
  });
});
