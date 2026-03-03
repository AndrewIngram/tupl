import { describe, expect, it } from "vitest";

import { defineProviders, defineSchema } from "../../src";
import { lowerSqlToRel, planPhysicalQuery } from "../../src/planning";

describe("query/v1 planning", () => {
  it("lowers simple select/join into relational operators", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders",
          columns: {
            id: "text",
            user_id: "text",
            total_cents: "integer",
          },
        },
        users: {
          provider: "users",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.total_cents > 1000
        ORDER BY o.id ASC
        LIMIT 5
      `,
      schema,
    );

    expect(lowered.rel.kind).toBe("project");

    const project = lowered.rel;
    if (project.kind !== "project") {
      throw new Error("Expected project root.");
    }

    expect(project.columns).toEqual([
      {
        source: { alias: "o", column: "id" },
        output: "id",
      },
      {
        source: { alias: "u", column: "email" },
        output: "email",
      },
    ]);

    const limitNode = project.input;
    expect(limitNode.kind).toBe("limit_offset");
    if (limitNode.kind !== "limit_offset") {
      throw new Error("Expected limit_offset node.");
    }

    expect(limitNode.limit).toBe(5);
  });

  it("plans lookup_join for cross-provider joins with lookupMany", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      orders: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "orders",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
      },
      users: {
        canExecute() {
          return true;
        },
        async compile(fragment) {
          return {
            provider: "users",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          return [];
        },
        async lookupMany() {
          return [];
        },
      },
    });

    const lowered = lowerSqlToRel(
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
      schema,
    );

    const physical = await planPhysicalQuery(
      lowered.rel,
      schema,
      providers,
      {},
      `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
      `,
    );

    expect(physical.steps.some((step) => step.kind === "lookup_join")).toBe(true);
  });
});
