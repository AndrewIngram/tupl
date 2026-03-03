import { describe, expect, it } from "vitest";

import {
  createQuerySession,
  defineProviders,
  defineSchema,
  query,
  type ProviderAdapter,
  type ProviderFragment,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "../../src";

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = rows.filter((row) => applyFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] ?? null;
        const rightValue = right[term.column] ?? null;
        if (leftValue === rightValue) {
          continue;
        }

        const direction = term.direction === "asc" ? 1 : -1;
        if (leftValue == null) {
          return -1 * direction;
        }
        if (rightValue == null) {
          return 1 * direction;
        }

        if (leftValue < rightValue) {
          return -1 * direction;
        }
        if (leftValue > rightValue) {
          return 1 * direction;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
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

function applyFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];
    switch (clause.op) {
      case "eq":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || Number(value) <= Number(clause.value)) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || Number(value) < Number(clause.value)) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || Number(value) >= Number(clause.value)) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || Number(value) > Number(clause.value)) {
          return false;
        }
        break;
      case "in":
        if (!clause.values.includes(value)) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }

  return true;
}

describe("query/v1 provider runtime", () => {
  it("pushes same-provider query fragments via sql_query", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
            org_id: "text",
            total_cents: "integer",
          },
        },
      },
    });

    let canExecuteCalls = 0;
    let executeCalls = 0;

    const providers = defineProviders({
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          canExecuteCalls += 1;
          return fragment.kind === "sql_query";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute() {
          executeCalls += 1;
          return [{ id: "o2" }];
        },
      } satisfies ProviderAdapter,
    });

    const rows = await query({
      schema,
      providers,
      context: {},
      sql: `
        SELECT id
        FROM orders
        WHERE org_id = 'org_1'
        ORDER BY total_cents DESC
        LIMIT 1
      `,
    });

    expect(rows).toEqual([{ id: "o2" }]);
    expect(canExecuteCalls).toBeGreaterThan(0);
    expect(executeCalls).toBe(1);
  });

  it("uses lookupMany for cross-provider lookup join paths", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u2" },
    ];
    const usersRows = [
      { id: "u1", email: "ada@example.com" },
      { id: "u2", email: "ben@example.com" },
    ];

    let lookupCalls = 0;

    const providers = defineProviders({
      orders_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies ProviderAdapter,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          lookupCalls += 1;
          const keys = new Set(request.keys);
          return usersRows
            .filter((row) => keys.has(row[request.key]))
            .map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            });
        },
      } satisfies ProviderAdapter,
    });

    const rows = await query({
      schema,
      providers,
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        ORDER BY o.id ASC
      `,
    });

    expect(rows).toEqual([
      { id: "o1", email: "ada@example.com" },
      { id: "o2", email: "ben@example.com" },
    ]);
    expect(lookupCalls).toBeGreaterThan(0);
  });

  it("enforces lookup batching guardrails", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u2" },
      { id: "o3", user_id: "u3" },
    ];

    const usersRows = [
      { id: "u1", email: "a@example.com" },
      { id: "u2", email: "b@example.com" },
      { id: "u3", email: "c@example.com" },
    ];

    const providers = defineProviders({
      orders_provider: {
        canExecute() {
          return true;
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies ProviderAdapter,
      users_provider: {
        canExecute() {
          return true;
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return usersRows.filter((row) => keys.has(row.id));
        },
      } satisfies ProviderAdapter,
    });

    await expect(
      query({
        schema,
        providers,
        context: {},
        sql: `
          SELECT o.id, u.email
          FROM orders o
          JOIN users u ON o.user_id = u.id
          ORDER BY o.id ASC
        `,
        queryGuardrails: {
          maxLookupKeysPerBatch: 1,
          maxLookupBatches: 1,
        },
      }),
    ).rejects.toThrow("maxLookupBatches guardrail");
  });

  it("preserves LEFT JOIN null semantics on lookup joins", async () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "orders_provider",
          columns: {
            id: "text",
            user_id: "text",
          },
        },
        users: {
          provider: "users_provider",
          columns: {
            id: "text",
            email: "text",
          },
        },
      },
    });

    const ordersRows = [
      { id: "o1", user_id: "u1" },
      { id: "o2", user_id: "u_missing" },
    ];
    const usersRows = [{ id: "u1", email: "ada@example.com" }];

    const providers = defineProviders({
      orders_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "orders_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(ordersRows, fragment.request);
        },
      } satisfies ProviderAdapter,
      users_provider: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "users_provider",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(usersRows, fragment.request);
        },
        async lookupMany(request) {
          const keys = new Set(request.keys);
          return usersRows
            .filter((row) => keys.has(row[request.key]))
            .map((row) => {
              const out: QueryRow = {};
              for (const column of request.select) {
                out[column] = row[column] ?? null;
              }
              return out;
            });
        },
      } satisfies ProviderAdapter,
    });

    const rows = await query({
      schema,
      providers,
      context: {},
      sql: `
        SELECT o.id, u.email
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.id
        ORDER BY o.id ASC
      `,
    });

    expect(rows).toEqual([
      { id: "o1", email: "ada@example.com" },
      { id: "o2", email: null },
    ]);
  });

  it("maps session plan scan stages to remote_fragment kind", () => {
    const schema = defineSchema({
      tables: {
        orders: {
          provider: "warehouse",
          columns: {
            id: "text",
          },
        },
      },
    });

    const providers = defineProviders({
      warehouse: {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: "warehouse",
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows([{ id: "o1" }], fragment.request);
        },
      } satisfies ProviderAdapter,
    });

    const session = createQuerySession({
      schema,
      providers,
      context: {},
      sql: "SELECT id FROM orders",
    });

    const plan = session.getPlan();
    expect(plan.steps.length).toBeGreaterThan(0);
    expect(plan.steps.some((step) => step.kind === "remote_fragment")).toBe(true);
  });
});
