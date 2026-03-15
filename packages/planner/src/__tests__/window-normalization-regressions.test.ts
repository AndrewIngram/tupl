import type { RelNode } from "@tupl/foundation";
import { createDataEntityHandle } from "@tupl/provider-kit";
import { describe, expect, it } from "vitest";

import { buildEntitySchema, buildSchema } from "@tupl/test-support/schema";

import { normalizeRelForProvider } from "../planner/provider/provider-rel-normalization";
import { normalizeRelForSnapshot } from "../planner/translation-normalization";
import { rewriteExpandedViewNode } from "../planner/views/view-node-rewriting";

function buildWindowNode(alias: string): Extract<RelNode, { kind: "window" }> {
  return {
    kind: "window",
    id: "window_orders",
    convention: "logical",
    output: [
      { name: "id" },
      { name: "totalCents" },
      { name: "orgId" },
      { name: "createdAt" },
      { name: "runningTotal" },
      { name: "previousTotal" },
    ],
    input: {
      kind: "scan",
      id: "scan_orders",
      convention: "logical",
      table: "orders",
      alias,
      select: ["id", "totalCents", "orgId", "createdAt"],
      output: [
        { name: `${alias}.id` },
        { name: `${alias}.totalCents` },
        { name: `${alias}.orgId` },
        { name: `${alias}.createdAt` },
      ],
    },
    functions: [
      {
        fn: "sum",
        as: "runningTotal",
        column: { alias, column: "totalCents" },
        partitionBy: [{ alias, column: "orgId" }],
        orderBy: [{ source: { alias, column: "createdAt" }, direction: "asc" }],
      },
      {
        fn: "lag",
        as: "previousTotal",
        value: {
          kind: "column",
          ref: { alias, column: "totalCents" },
        },
        offset: 2,
        defaultExpr: {
          kind: "column",
          ref: { alias, column: "orgId" },
        },
        partitionBy: [{ alias, column: "orgId" }],
        orderBy: [{ source: { alias, column: "createdAt" }, direction: "asc" }],
      },
    ],
  };
}

describe("window normalization regressions", () => {
  it("normalizes aggregate and navigation window refs for providers", () => {
    const ordersEntity = createDataEntityHandle({
      entity: "orders_raw",
      provider: "warehouse",
    });
    const schema = buildSchema((builder) => {
      builder.table("orders", ordersEntity, {
        columns: {
          id: { source: "id", type: "text", nullable: false },
          totalCents: { source: "total_cents", type: "integer", nullable: false },
          orgId: { source: "org_id", type: "text", nullable: false },
          createdAt: { source: "created_at", type: "text", nullable: false },
        },
      });
    });

    const normalized = normalizeRelForProvider(buildWindowNode("o"), schema);
    expect(normalized.kind).toBe("window");
    if (normalized.kind !== "window") {
      throw new Error("Expected normalized window node.");
    }

    expect(normalized.functions).toMatchObject([
      {
        fn: "sum",
        column: { alias: "o", column: "total_cents" },
        partitionBy: [{ alias: "o", column: "org_id" }],
        orderBy: [{ source: { alias: "o", column: "created_at" }, direction: "asc" }],
      },
      {
        fn: "lag",
        value: {
          kind: "column",
          ref: { alias: "o", column: "total_cents" },
        },
        offset: 2,
        defaultExpr: {
          kind: "column",
          ref: { alias: "o", column: "org_id" },
        },
        partitionBy: [{ alias: "o", column: "org_id" }],
        orderBy: [{ source: { alias: "o", column: "created_at" }, direction: "asc" }],
      },
    ]);
  });

  it("rewrites navigation window refs during view expansion", () => {
    const rewritten = rewriteExpandedViewNode(
      buildWindowNode("v"),
      buildEntitySchema({}),
      undefined,
      (node) => {
        if (node.kind !== "scan") {
          throw new Error("Expected scan leaf during view rewrite test.");
        }

        return {
          node,
          aliases: new Map([
            [
              "v",
              {
                id: { alias: "o", column: "id" },
                totalCents: { alias: "o", column: "total_cents" },
                orgId: { alias: "o", column: "org_id" },
                createdAt: { alias: "o", column: "created_at" },
              },
            ],
          ]),
        };
      },
    );

    expect(rewritten.node.kind).toBe("window");
    if (rewritten.node.kind !== "window") {
      throw new Error("Expected rewritten window node.");
    }

    expect(rewritten.node.functions).toMatchObject([
      {
        fn: "sum",
        column: { alias: "o", column: "total_cents" },
        partitionBy: [{ alias: "o", column: "org_id" }],
        orderBy: [{ source: { alias: "o", column: "created_at" }, direction: "asc" }],
      },
      {
        fn: "lag",
        value: {
          kind: "column",
          ref: { alias: "o", column: "total_cents" },
        },
        offset: 2,
        defaultExpr: {
          kind: "column",
          ref: { alias: "o", column: "org_id" },
        },
        partitionBy: [{ alias: "o", column: "org_id" }],
        orderBy: [{ source: { alias: "o", column: "created_at" }, direction: "asc" }],
      },
    ]);
  });

  it("preserves navigation window details in snapshot normalization", () => {
    expect(normalizeRelForSnapshot(buildWindowNode("o"))).toMatchObject({
      kind: "window",
      functions: [
        {
          fn: "sum",
          column: { alias: "o", column: "totalCents" },
        },
        {
          fn: "lag",
          value: {
            kind: "column",
            ref: { alias: "o", column: "totalCents" },
          },
          offset: 2,
          defaultExpr: {
            kind: "column",
            ref: { alias: "o", column: "orgId" },
          },
        },
      ],
    });
  });
});
