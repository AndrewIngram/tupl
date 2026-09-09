# tupl

**Warning**: Feel free to play around with this, but don't rely on it in real code. The API for defining your abstracted database is converging on something I'm happy with, but the approach to building custom providers (e.g Drizzle, Objection etc) is very unstable and prone to significant rewrites.

`tupl` lets you expose a controlled SQL facade over one or more underlying data systems.

## What

`tupl` is a provider-first query runtime:

- You define one or more providers (Drizzle/Kysely/Objection/custom).
- Providers expose normalized entities that represent physical sources.
- You build an executable logical SQL-facing schema from those entities.
- `tupl` plans query fragments across providers and local logical operators.

The facade stays relational (`SELECT` over tables/views), while providers can be relational or non-relational.

Tables and views can also expose derived columns computed by your TypeScript
functions. Queries can select, filter, join, group, and sort these values alongside
native columns. Computation runs locally only when the query needs it.

Terminology used in this repo:

- `provider`: the runtime object registered under a name and asked to `canExecute`, `compile`, `execute`, and optionally `describeCompiledPlan`
- `adapter`: the authoring layer or helper that builds a provider
- `backend`: the wrapped system or query builder, such as Drizzle, Kysely, Objection, or Redis

Package guidance:

- application authors should usually stay on `@tupl/schema`
- adapter authors should usually stay on `@tupl/provider-kit`, `@tupl/provider-kit/shapes`, and `@tupl/provider-kit/testing`
- planner/runtime packages are for advanced tooling, debugging, and lower-level integrations

## Why

Typical reasons to use `tupl`:

- enforce a safer query boundary than direct DB access
- expose only an allowlisted, user-facing data model
- centralize tenancy/scope logic in provider integration
- keep SQL ergonomics for developers and agents while supporting mixed backends

## Examples

### Example A (Primary): Executable Schema + Scoped Drizzle Provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

type QueryContext = { orgId: string; userId: string; db: typeof db };

const dbProvider = createDrizzleProvider<QueryContext>({
  name: "dbProvider",
  db: (ctx) => ctx.db,
  tables: {
    orders: {
      table: tables.orders,
      scope: (ctx) =>
        and(eq(tables.orders.org_id, ctx.orgId), eq(tables.orders.user_id, ctx.userId)),
    },
    vendors: {
      table: tables.vendors,
      scope: (ctx) => eq(tables.vendors.org_id, ctx.orgId),
    },
  },
});

const builder = createSchemaBuilder<QueryContext>();

const myOrders = builder.table("myOrders", dbProvider.entities.orders, {
  columns: ({ col, expr }) => ({
    id: col.id("id"),
    vendorId: col.string("vendor_id"),
    totalCents: col.integer("total_cents"),
    createdAt: col.timestamp("created_at"),
    totalDollars: col.real(expr.divide(col("totalCents"), expr.literal(100)), {
      nullable: false,
    }),
    isLargeOrder: col.boolean(expr.gte(col("totalCents"), expr.literal(3000)), {
      nullable: false,
    }),
  }),
});

const myOrderFacts = builder.view(
  "myOrderFacts",
  ({ scan, join, col, expr }) =>
    join({
      left: scan(myOrders),
      right: scan(dbProvider.entities.vendors),
      on: expr.eq(col(myOrders, "vendorId"), col(dbProvider.entities.vendors, "id")),
      type: "inner",
    }),
  {
    columns: ({ col }) => ({
      orderId: col.id(myOrders, "id"),
      vendorId: col.string(myOrders, "vendorId", { nullable: false }),
      vendorName: col.string(dbProvider.entities.vendors, "name", { nullable: false }),
      totalCents: col.integer(myOrders, "totalCents", { nullable: false }),
      totalDollars: col.real(myOrders, "totalDollars", { nullable: false }),
      isLargeOrder: col.boolean(myOrders, "isLargeOrder", { nullable: false }),
    }),
  },
);

builder.view(
  "myVendorSpend",
  ({ scan, aggregate, col, agg }) =>
    aggregate({
      from: scan(myOrderFacts),
      groupBy: {
        vendorId: col(myOrderFacts, "vendorId"),
        vendorName: col(myOrderFacts, "vendorName"),
      },
      measures: {
        totalSpendCents: agg.sum(col(myOrderFacts, "totalCents")),
        orderCount: agg.count(),
      },
    }),
  {
    columns: ({ col }) => ({
      vendorId: col.id("vendorId"),
      vendorName: col.string("vendorName"),
      totalSpendCents: col.integer("totalSpendCents"),
      orderCount: col.integer("orderCount"),
    }),
  },
);

const executableSchema = createExecutableSchema(builder).unwrap();

const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT vendorName, totalSpendCents, orderCount
    FROM myVendorSpend
    ORDER BY totalSpendCents DESC
  `,
});

const highValueOrders = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT orderId, vendorName, totalDollars, isLargeOrder
    FROM myOrderFacts
    WHERE totalDollars >= 20
    ORDER BY totalDollars DESC
  `,
});
```

If your runtime handle is static, `db` can still be passed directly instead of using a context callback.

`createExecutableSchema` returns a `Result`; `.unwrap()` above stops setup if the
schema is invalid. Query calls return a promise of a `Result` containing rows.
Handle `isErr()`/`isOk()` in application code, or unwrap when failure should throw.

### Example B: Non-Relational Mapping Pattern

```ts
import { createIoredisProvider, type RedisLike } from "@tupl/provider-ioredis";
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

type QueryContext = {
  userId: string;
  redis: RedisLike;
};

const redisProvider = createIoredisProvider<QueryContext>({
  name: "redisProvider",
  redis: (ctx) => ctx.redis,
  entities: {
    product_view_counts: {
      entity: "product_view_counts",
      lookupKey: "product_id",
      columns: ["product_id", "view_count"] as const,
      buildRedisKey({ key, context }) {
        return `product_view_counts:${context.userId}:${String(key)}`;
      },
      decodeRow({ hash }) {
        if (typeof hash.product_id !== "string" || typeof hash.view_count !== "string") {
          return null;
        }
        return {
          product_id: hash.product_id,
          view_count: Number(hash.view_count),
        };
      },
    },
  },
});

const builder = createSchemaBuilder<QueryContext>();

builder.table("productViewCounts", redisProvider.entities.product_view_counts, {
  columns: ({ col }) => ({
    productId: col.string("product_id"),
    viewCount: col.integer("view_count"),
  }),
});

const executableSchema = createExecutableSchema(builder).unwrap();
```

### Derived columns in TypeScript

Use `derive` in a table or view's `columns` callback. It accepts named column
dependencies and a pure, synchronous function. Derived results can depend on
other derived results; expose them with the usual typed column builders.

```ts
const orderSummaries = builder.table("orderSummaries", dbProvider.entities.orders, {
  columns: ({ col, derive }) => {
    const totalCents = col.integer("total_cents", { nullable: false });
    const dollars = derive({ totalCents }, ({ totalCents }) => totalCents / 100);
    const formatted = derive({ dollars }, ({ dollars }) => `$${dollars.toFixed(2)}`);

    return {
      id: col.id("id"),
      totalCents,
      formattedTotal: col.string(formatted, { nullable: false }),
    };
  },
});
```

Only returned columns become public. Here `dollars` and `formatted` are private
intermediates. Shared dependencies are evaluated once per row at their computation
stage. Callbacks must use only their declared inputs and have no side effects;
asynchronous callbacks are not supported.

```sql
-- No derived computation is needed.
SELECT id, totalCents FROM orderSummaries;

-- Native filtering and pagination can reduce the rows formatted locally.
SELECT id, formattedTotal FROM orderSummaries
WHERE totalCents >= 3000 ORDER BY totalCents DESC LIMIT 20;
```

Filters or joins that use a derived value run after its local computation. The
planner pushes independent native work to providers where supported and safe.
`SELECT *` includes public derived columns; `COUNT(*)` skips unused computations.
Expression-based calculated columns using `expr` remain eligible for provider
execution. See [the derived-column guide](./docs/building-a-schema.md#derived-columns-in-typescript)
for shared parsing/rendering, view dependencies, nullability, and execution limits.

## Limitations

Current limitations/non-goals:

- write statements are not supported (`INSERT`, `UPDATE`, `DELETE`)
- provider pushdown breadth is still adapter-specific
- provider planning is greedy rather than cost-based
- keyed helper surfaces such as `lookupMany` remain optional optimizations, not part of the primary provider contract

Execution behavior notes:

- unsupported provider pushdown shapes can fall back to local logical execution
- providers can explicitly reject shapes (`canExecute`) for deterministic behavior
- cross-provider joins can use keyed helper surfaces when available, but those helpers are optional optimization layers
- ordinary SQL-like adapters should start with `createSqlRelationalProviderAdapter(...)`, which keeps provider roots close to the manual provider lifecycle (`resolveRuntime`, `canExecute`, query backend, execute) and reserves `advanced` for real backend exceptions

## Adapter support matrix

| Adapter                    | rel-first pushdown | optional keyed helper | advanced rel pushdown (`WITH`/set-op/window) | local fallback when unsupported | explicit shape rejection |
| -------------------------- | ------------------ | --------------------- | -------------------------------------------- | ------------------------------- | ------------------------ |
| `@tupl/provider-drizzle`   | Yes                | Yes                   | Partial                                      | Yes                             | Yes                      |
| `@tupl/provider-kysely`    | Yes                | Yes                   | Partial                                      | Yes                             | Yes                      |
| `@tupl/provider-objection` | Yes                | Yes                   | Partial                                      | Yes                             | Yes                      |
| `@tupl/provider-ioredis`   | Narrow             | Yes                   | No                                           | Yes                             | Yes                      |
| Custom non-relational      | Custom             | Custom                | Custom                                       | Yes                             | Yes                      |

## Guides

- [Hosted playground](https://tupl-playground.andrewingram.workers.dev/)
- [Building a schema (executable schema, Drizzle example)](./docs/building-a-schema.md)
- [Creating a new adapter (progressive path)](./docs/creating-an-adapter.md)
- [Maintainer bug map (where to fix what)](./docs/maintainer-bug-map.md)

## Verification

- `pnpm typecheck` runs the canonical workspace typecheck across all packages and examples.
- `pnpm typecheck:root` runs only the root `tsconfig.json` check.
- `pnpm verify` runs the standard local verification set: lint, workspace typecheck, full test suite, and format.
- `pnpm verify:ci` runs the local CI verification set: lint, workspace typecheck, the test suite, and `fmt --check`.
- [Building a non-relational adapter (Redis-style)](./docs/building-a-non-relational-adapter.md)
- [Package architecture and allowed dependency directions](./docs/package-architecture.md)
