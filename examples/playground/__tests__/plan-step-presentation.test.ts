import { describe, expect, it } from "vite-plus/test";
import type { QueryExecutionPlanStep, QueryStepState } from "@tupl/runtime/session";

import { presentStep } from "../src/plan-step-presentation";

describe("playground/plan-step-presentation", () => {
  it("summarizes scan steps as compact operator signatures", () => {
    const step: QueryExecutionPlanStep = {
      id: "scan_1",
      kind: "scan",
      dependsOn: [],
      summary: "Scan orders",
      phase: "fetch",
      operation: {
        name: "scan",
        details: {
          table: "orders",
          alias: "o",
        },
      },
      request: {
        select: ["id", "user_id", "total_cents"],
        where: [{ column: "org_id", op: "=", value: "org_1" }],
      },
      outputs: ["id", "user_id", "total_cents"],
      sqlOrigin: "FROM",
    };
    const state: QueryStepState = {
      id: "scan_1",
      relNodeId: "rel_scan_1",
      executionId: "execution_1",
      occurrence: 1,
      kind: "scan",
      status: "done",
      summary: "Scan orders",
      dependsOn: [],
      routeUsed: "scan",
      executionIndex: 1,
      startedAt: 100,
      endedAt: 104,
      durationMs: 4,
      rowCount: 12,
      outputRowCount: 12,
    };

    expect(presentStep(step, state)).toMatchObject({
      operator: "Scan",
      clause: "FROM",
      signature: "orders as o",
      placement: "Remote scan",
      outputsPreview: "id, user_id, total_cents",
    });
  });

  it("summarizes aggregate steps in relational terms", () => {
    const step: QueryExecutionPlanStep = {
      id: "aggregate_1",
      kind: "aggregate",
      dependsOn: ["scan_1"],
      summary: "Compute grouped aggregates",
      phase: "transform",
      operation: {
        name: "aggregate",
        details: {
          groupBy: ["orders.org_id"],
          metrics: [{ fn: "count", as: "order_count" }],
        },
      },
      outputs: ["org_id", "order_count"],
      sqlOrigin: "GROUP BY",
    };

    expect(presentStep(step, null)).toMatchObject({
      operator: "Aggregate",
      signature: "by orders.org_id | count(*) -> order_count",
      placement: "internal op",
    });
  });

  it("surfaces provider fragment placement clearly", () => {
    const step: QueryExecutionPlanStep = {
      id: "remote_fragment_1",
      kind: "remote_fragment",
      dependsOn: [],
      summary: "Execute provider fragment (warehouse)",
      phase: "fetch",
      operation: {
        name: "provider_fragment",
        details: {
          provider: "warehouse",
        },
      },
      request: {
        fragment: "rel",
      },
      outputs: ["id", "email"],
      sqlOrigin: "SELECT",
    };

    expect(presentStep(step, null)).toMatchObject({
      operator: "Remote Fragment",
      signature: "warehouse rel",
      placement: "Remote on warehouse",
    });
  });

  it("shows the measured local route when a statically remote step falls back", () => {
    const step: QueryExecutionPlanStep = {
      id: "remote_fragment_1",
      relNodeId: "rel_remote_fragment_1",
      kind: "remote_fragment",
      dependsOn: [],
      summary: "Execute provider fragment (warehouse)",
      phase: "fetch",
      operation: {
        name: "provider_fragment",
        details: {
          provider: "warehouse",
        },
      },
    };
    const state: QueryStepState = {
      id: step.id,
      relNodeId: "rel_remote_fragment_1",
      executionId: "execution_1",
      occurrence: 1,
      kind: "remote_fragment",
      status: "done",
      summary: step.summary,
      dependsOn: [],
      routeUsed: "local",
      executionIndex: 1,
      startedAt: 100,
      endedAt: 104,
      durationMs: 4,
      rowCount: 2,
      outputRowCount: 2,
    };

    expect(presentStep(step, state)).toMatchObject({
      placement: "Local runtime",
      executionLabel: "local over fetched rows",
    });
  });

  it("treats values materialization as a local logical step", () => {
    const step: QueryExecutionPlanStep = {
      id: "projection_1",
      kind: "projection",
      dependsOn: [],
      summary: "Materialize literal rows",
      phase: "fetch",
      operation: {
        name: "values",
        details: {
          rowCount: 1,
        },
      },
      outputs: [],
      sqlOrigin: "SELECT",
    };

    expect(presentStep(step, null)).toMatchObject({
      operator: "Values",
      signature: "1 literal row",
      placement: "internal op",
      executionLabel: "internal op",
    });
  });
});
