import { describe, expect, it } from "vitest";

import {
  defineSchema,
  defineTableMethods,
  type QueryRow,
  type TableScanRequest,
} from "@sqlql/core";
import { query } from "../src";

function applyScan(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];

  for (const clause of request.where ?? []) {
    switch (clause.op) {
      case "eq":
        out = out.filter((row) => row[clause.column] === clause.value);
        break;
      case "neq":
        out = out.filter((row) => row[clause.column] !== clause.value);
        break;
      case "gt":
        out = out.filter((row) => Number(row[clause.column]) > Number(clause.value));
        break;
      case "gte":
        out = out.filter((row) => Number(row[clause.column]) >= Number(clause.value));
        break;
      case "lt":
        out = out.filter((row) => Number(row[clause.column]) < Number(clause.value));
        break;
      case "lte":
        out = out.filter((row) => Number(row[clause.column]) <= Number(clause.value));
        break;
      case "in": {
        const set = new Set(clause.values);
        out = out.filter((row) => set.has(row[clause.column]));
        break;
      }
    }
  }

  if (request.orderBy) {
    out.sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] as string | number;
        const rightValue = right[term.column] as string | number;
        if (leftValue === rightValue) {
          continue;
        }

        const comparison = leftValue < rightValue ? -1 : 1;
        return term.direction === "asc" ? comparison : -comparison;
      }

      return 0;
    });
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

describe("query", () => {
  it("turns SQL joins into dependent scan calls", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          columns: {
            id: "text",
            org_id: "text",
            user_id: "text",
            status: "text",
            total_cents: "integer",
            created_at: "timestamp",
          },
        },
        users: {
          columns: {
            id: "text",
            team_id: "text",
            email: "text",
          },
        },
        teams: {
          columns: {
            id: "text",
            name: "text",
            tier: "text",
          },
        },
      },
    });

    const data = {
      orders: [
        {
          id: "order_1",
          org_id: "org_1",
          user_id: "user_1",
          status: "paid",
          total_cents: 1200,
          created_at: "2026-02-01T00:00:00Z",
        },
        {
          id: "order_2",
          org_id: "org_1",
          user_id: "user_1",
          status: "paid",
          total_cents: 1900,
          created_at: "2026-02-03T00:00:00Z",
        },
        {
          id: "order_3",
          org_id: "org_1",
          user_id: "user_2",
          status: "paid",
          total_cents: 2500,
          created_at: "2026-02-04T00:00:00Z",
        },
      ],
      users: [
        { id: "user_1", team_id: "team_enterprise", email: "a@example.com" },
        { id: "user_2", team_id: "team_smb", email: "b@example.com" },
      ],
      teams: [
        { id: "team_enterprise", name: "Enterprise", tier: "enterprise" },
        { id: "team_smb", name: "SMB", tier: "smb" },
      ],
    } as const;

    const calls: Array<{ table: string; request: TableScanRequest }> = [];

    const methods = defineTableMethods({
      orders: {
        async scan(request) {
          calls.push({ table: "orders", request });
          return applyScan(data.orders as unknown as QueryRow[], request);
        },
      },
      users: {
        async scan(request) {
          calls.push({ table: "users", request });
          return applyScan(data.users as unknown as QueryRow[], request);
        },
      },
      teams: {
        async scan(request) {
          calls.push({ table: "teams", request });
          return applyScan(data.teams as unknown as QueryRow[], request);
        },
      },
    });

    const rows = await query({
      schema,
      methods,
      context: {},
      sql: `
        SELECT o.id, o.total_cents, u.email, t.name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN teams t ON u.team_id = t.id
        WHERE o.org_id = 'org_1' AND o.status = 'paid' AND t.tier = 'enterprise'
        ORDER BY o.created_at DESC
        LIMIT 2
      `,
    });

    expect(rows).toEqual([
      {
        id: "order_2",
        total_cents: 1900,
        email: "a@example.com",
        name: "Enterprise",
      },
      {
        id: "order_1",
        total_cents: 1200,
        email: "a@example.com",
        name: "Enterprise",
      },
    ]);

    const usersCall = calls.find((call) => call.table === "users");
    const usersInFilter = usersCall?.request.where?.find((clause) => clause.op === "in");
    expect(usersInFilter).toEqual({
      op: "in",
      column: "id",
      values: ["user_1", "user_2"],
    });

    const teamsCall = calls.find((call) => call.table === "teams");
    const teamsInFilter = teamsCall?.request.where?.find((clause) => clause.op === "in");
    expect(teamsInFilter).toEqual({
      op: "in",
      column: "id",
      values: ["team_enterprise", "team_smb"],
    });
  });
});
