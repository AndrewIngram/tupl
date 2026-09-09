import { Result } from "better-result";
import { describe, expect, it } from "vite-plus/test";
import type { RelJoinNode } from "@tupl/foundation";
import { createDataEntityHandle, type ProviderAdapter } from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";
import { resolveLookupJoinCandidate } from "../physical/lookup-join-candidate";

const rightProvider = {
  name: "right",
  canExecute: () => true,
  compile: () => Result.ok({ provider: "right", kind: "rel", payload: {} }),
  execute: () => Result.ok([]),
  lookupMany: () => Result.ok([]),
} satisfies ProviderAdapter & LookupManyCapableProviderAdapter;
const entity = createDataEntityHandle({
  provider: "right",
  entity: "rows",
  providerInstance: rightProvider,
});

function join(): RelJoinNode {
  return {
    id: "join",
    kind: "join",
    convention: "local",
    joinType: "left",
    left: {
      id: "values",
      kind: "values",
      convention: "local",
      rows: [[1]],
      output: [{ name: "key" }],
    },
    right: {
      id: "scan",
      kind: "scan",
      convention: "local",
      table: "private",
      alias: "r",
      entity,
      select: ["id"],
      output: [{ name: "r.id" }],
    },
    leftKey: { column: "key" },
    rightKey: { alias: "r", column: "id" },
    output: [{ name: "key" }, { name: "r.id" }],
  };
}

describe("shared lookup eligibility", () => {
  it("accepts a materialized local key and an attached private lookup provider", () => {
    expect(resolveLookupJoinCandidate(join(), { tables: {} }, {})?.description).toMatchObject({
      leftProvider: "local",
      rightProvider: "right",
    });
  });

  it.each([{ limit: 0 }, { limit: 1 }, { offset: 0 }, { offset: 1 }])(
    "preserves a bare right scan page %j",
    (page) => {
      const rel = join();
      if (rel.right.kind !== "scan") throw new Error("Expected scan fixture");
      rel.right = { ...rel.right, ...page };
      expect(resolveLookupJoinCandidate(rel, { tables: {} }, {})).toBeNull();
    },
  );
});
