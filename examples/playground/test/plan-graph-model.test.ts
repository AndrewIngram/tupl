import { describe, expect, it } from "vitest";
import type { QueryExecutionPlanScope, QueryExecutionPlanStep } from "sqlql";

import {
  buildPlanGraphLayout,
  buildPlanGraphModel,
  collectDependencies,
} from "../src/plan-graph-model";

const STEPS: QueryExecutionPlanStep[] = [
  {
    id: "scan:users",
    kind: "scan",
    dependsOn: [],
    summary: "scan users",
    phase: "fetch",
    operation: { name: "scan" },
  },
  {
    id: "scan:workouts",
    kind: "scan",
    dependsOn: [],
    summary: "scan workouts",
    phase: "fetch",
    operation: { name: "scan" },
  },
  {
    id: "join:uw",
    kind: "join",
    dependsOn: ["scan:users", "scan:workouts"],
    summary: "join users/workouts",
    phase: "transform",
    operation: { name: "join" },
  },
  {
    id: "order",
    kind: "order",
    dependsOn: ["join:uw"],
    summary: "order results",
    phase: "output",
    operation: { name: "order" },
  },
];

const SCOPES: QueryExecutionPlanScope[] = [
  {
    id: "scope:root",
    kind: "root",
    label: "Root",
  },
  {
    id: "scope:cte",
    kind: "cte",
    label: "CTE recent_workouts",
    parentId: "scope:root",
  },
  {
    id: "scope:subquery",
    kind: "subquery",
    label: "Subquery WHERE #1",
    parentId: "scope:cte",
  },
];

describe("playground/plan-graph-model", () => {
  it("builds deterministic node positions and edge count", () => {
    const first = buildPlanGraphLayout(STEPS);
    const second = buildPlanGraphLayout(STEPS);

    expect(first.edges).toHaveLength(3);
    expect(first.steps.map((step) => step.id)).toEqual(second.steps.map((step) => step.id));

    for (const step of first.steps) {
      expect(first.positionsById.get(step.id)).toEqual(second.positionsById.get(step.id));
    }
  });

  it("collects upstream/downstream dependencies from selected node", () => {
    const deps = collectDependencies(STEPS, "join:uw");

    expect([...deps.upstream].sort()).toEqual(["scan:users", "scan:workouts"]);
    expect([...deps.downstream].sort()).toEqual(["order"]);
  });

  it("marks selected dependency path in graph model", () => {
    const layout = buildPlanGraphLayout(STEPS);
    const model = buildPlanGraphModel(layout, [], {}, "join:uw", null);

    const selectedNode = model.nodes.find(
      (node) => node.id === "join:uw" && node.type === "planStep",
    );
    expect(selectedNode && selectedNode.type === "planStep" ? selectedNode.data.isSelected : false).toBe(
      true,
    );

    const highlightedNodes = model.nodes
      .filter((node) => node.type === "planStep" && node.data.isHighlighted)
      .map((node) => node.id);
    expect(highlightedNodes.sort()).toEqual(["join:uw", "order", "scan:users", "scan:workouts"]);
  });

  it("adds nested scope parent nodes and assigns step parentId", () => {
    const [scanUsers, scanWorkouts, joinUsersWorkouts, orderResults] = STEPS;
    if (!scanUsers || !scanWorkouts || !joinUsersWorkouts || !orderResults) {
      throw new Error("Missing expected test fixture steps.");
    }
    const scopedSteps: QueryExecutionPlanStep[] = [
      {
        ...scanUsers,
        scopeId: "scope:cte",
      },
      {
        ...scanWorkouts,
        scopeId: "scope:subquery",
      },
      {
        ...joinUsersWorkouts,
        scopeId: "scope:root",
      },
      {
        ...orderResults,
        scopeId: "scope:root",
      },
    ];
    const layout = buildPlanGraphLayout(scopedSteps);
    const model = buildPlanGraphModel(layout, SCOPES, {}, null, null);

    const cteScopeNode = model.nodes.find((node) => node.id === "scope:cte");
    expect(cteScopeNode).toBeDefined();
    expect(cteScopeNode?.type).toBe("planScope");

    const subqueryScopeNode = model.nodes.find((node) => node.id === "scope:subquery");
    expect(subqueryScopeNode).toBeDefined();
    expect(subqueryScopeNode?.type).toBe("planScope");
    expect(subqueryScopeNode?.parentId).toBe("scope:cte");

    const usersNode = model.nodes.find((node) => node.id === "scan:users");
    expect(usersNode?.parentId).toBe("scope:cte");

    const workoutsNode = model.nodes.find((node) => node.id === "scan:workouts");
    expect(workoutsNode?.parentId).toBe("scope:subquery");

    const joinNode = model.nodes.find((node) => node.id === "join:uw");
    expect(joinNode?.parentId).toBeUndefined();
  });
});
