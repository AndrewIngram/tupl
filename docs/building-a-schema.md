# Building a Schema (Executable Schema, Drizzle Example)

This guide is DSL-first.

You will build:

1. provider-backed physical entities exposed by an adapter
2. scoped provider configuration (tenancy/user scope)
3. an executable SQL facade built with `table(...)` and `view(...)`

## Mental model

- Provider config describes physical access, normalization, and scope.
- The executable schema describes the user-facing facade (renames/transforms/views).
- SQL queries run against the facade, not your physical tables.

## End-to-end example

### 1) Define downstream tables and Drizzle DB

```ts
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

const sqlite = new Database(":memory:");

const ordersRaw = sqliteTable("orders_raw", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id").notNull(),
  user_id: text("user_id").notNull(),
  vendor_id: text("vendor_id").notNull(),
  status: text("status").notNull(),
  total_cents: integer("total_cents").notNull(),
  created_at: text("created_at").notNull(),
});

const vendorsRaw = sqliteTable("vendors_raw", {
  id: text("id").primaryKey().notNull(),
  org_id: text("org_id").notNull(),
  name: text("name").notNull(),
  tier: text("tier").notNull(),
});

const db = drizzle(sqlite);
```

### 2) Create scoped Drizzle provider

```ts
import { and, eq } from "drizzle-orm";
import { createDrizzleProvider } from "@tupl/provider-drizzle";

type QueryContext = { orgId: string; userId: string; db: typeof db };

const dbProvider = createDrizzleProvider<QueryContext>({
  name: "dbProvider",
  db: (ctx) => ctx.db,
  tables: {
    orders: {
      table: ordersRaw,
      scope: (ctx) => and(eq(ordersRaw.org_id, ctx.orgId), eq(ordersRaw.user_id, ctx.userId)),
    },
    vendors: {
      table: vendorsRaw,
      scope: (ctx) => eq(vendorsRaw.org_id, ctx.orgId),
    },
  },
});
```

`dbProvider.entities.orders` and `dbProvider.entities.vendors` are now typed, provider-owned entities that the schema can bind to directly.

### 3) Build facade schema with `createSchemaBuilder(...)`

```ts
import { createExecutableSchema, createSchemaBuilder } from "@tupl/schema";

const builder = createSchemaBuilder<QueryContext>();

const myOrders = builder.table("myOrders", dbProvider.entities.orders, {
  columns: ({ col, expr }) => ({
    id: col.id("id"),
    vendorId: col.string("vendor_id"),
    status: col.string("status", {
      enum: ["pending", "paid", "shipped"] as const,
    }),
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
        spendCents: agg.sum(col(myOrderFacts, "totalCents")),
        orderCount: agg.count(),
      },
    }),
  {
    columns: ({ col }) => ({
      vendorId: col.id("vendorId"),
      vendorName: col.string("vendorName"),
      spendCents: col.integer("spendCents"),
      orderCount: col.integer("orderCount"),
    }),
  },
);

const executableSchema = createExecutableSchema(builder).unwrap();
```

`createExecutableSchema(...)` accepts either a built schema object or a `SchemaBuilder`
and returns a `Result`. The example unwraps it during setup; applications can
instead handle the error explicitly. Query methods return a promise of a `Result`
containing rows. Schema creation finalizes the schema, materializes linked enums,
and validates provider bindings once, before queries run.

When a view only needs a provider entity as a private source, `scan(...)` can read the `DataEntityHandle` directly. You only need `table(...)` when you want that source to be part of the public facade.

### 4) Use calculated columns on a base table

Calculated columns can be declared directly in a table's `columns` mapping. They behave like any other logical column in `SELECT`, `WHERE`, and `ORDER BY`.

```ts
const highValueOrders = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT id, totalDollars, isLargeOrder
    FROM myOrders
    WHERE totalDollars >= 20
    ORDER BY totalDollars DESC
  `,
});
```

### 5) Query composed and aggregate views

```ts
const rows = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT vendorName, spendCents, orderCount
    FROM myVendorSpend
    ORDER BY spendCents DESC
  `,
});

const facts = await executableSchema.query({
  context: { orgId: "org_1", userId: "u1", db },
  sql: `
    SELECT orderId, vendorName, totalDollars
    FROM myOrderFacts
    ORDER BY totalDollars DESC
  `,
});
```

If your database handle is already static for the lifetime of the provider, you can still pass `db` directly instead of `db: (ctx) => ctx.db`.

## Why this pattern stays clean

- Physical concerns stay in provider config (`table`, `scope`, backend APIs).
- Facade concerns stay in the executable schema (`table`, `view`, logical names).
- Scoped typed builders (`columns: ({ col }) => ...`) reduce ref/column drift.

## Troubleshooting checklist

- provider `tables` keys match the entities you bind in `table("logicalName", provider.entities.someTable, ...)`
- scoped columns exist on physical tables
- facade FK references target facade table/column names
- supported SQL can run across provider fragments and local operators; syntax or
  relational shapes the planner cannot represent return an error

## Derived columns in TypeScript

Use `derive` inside a table or view's `columns` callback when an existing TypeScript
function computes a value. Give it named column dependencies and a synchronous
callback, then expose its result with the usual column builders.

```ts
const documents = builder.table("documents", provider.entities.documents, {
  columns: ({ col, derive }) => {
    const body = col.string("body", { nullable: false });
    const parsed = derive({ body }, ({ body }) => parseDocument(body));
    const rendered = derive({ parsed }, ({ parsed }) => renderDocument(parsed));
    return {
      id: col.id("id"),
      status: col.string("status"),
      markdown: col.string(
        derive({ rendered }, ({ rendered }) => rendered.markdown),
        { nullable: false },
      ),
      plainText: col.string(
        derive({ rendered }, ({ rendered }) => rendered.plainText),
        { nullable: false },
      ),
    };
  },
});
```

Here `parseDocument` and `renderDocument` are application functions. Only returned
entries become public columns. `body`, `parsed`, and `rendered` remain private.
Both outputs share parsing and rendering once per row occurrence in that stage of
the query. A later query computes fresh values. Intermediate values can be objects
or Maps; public values must satisfy the declared SQL type and nullability.

A dependency can be a source column definition, an expression-based calculated
column, or another derive result from the same `columns` declaration. In views,
use qualified definitions such as `col.string(documents, "plainText")` as inputs.
Keep handles within their declaration; reference another table through `col`.
JSON inputs without authoritative provider read types are `unknown`: validate
before reading their properties. Nullable inputs reach the callback as `null`.

```sql
-- No parsing, rendering, or body fetch is needed.
SELECT id FROM documents WHERE status = 'published';

-- When supported by the provider, filtering and pagination happen before rendering.
SELECT id, markdown, plainText
FROM documents WHERE status = 'published' ORDER BY id LIMIT 20;

-- The native status restriction reduces candidates; text filtering happens locally.
SELECT id FROM documents
WHERE status = 'published' AND plainText LIKE '%welcome%' LIMIT 20;
```

References anywhere in a query matter, including filters, joins, grouping,
ordering, and windows. `COUNT(*)` does not require unused derived outputs;
`SELECT *` includes all public derived columns. Existing `expr` calculations can
still execute in a supporting provider. JavaScript computations execute locally.
Tupl moves safe native restrictions and projections below that boundary. It does
not push a limit below a derived filter or remove a join without preserving its
row multiplicity. Local filtering currently materializes the candidate rows;
incremental fetching is future work.

Callbacks must be pure, synchronous functions of their declared inputs. Do not
fetch data, mutate dependencies or shared state, or depend on time, randomness,
or invocation counts. Unused computations never execute. A computation can also
be skipped for rows eliminated earlier, so predicate text order does not specify
callback evaluation or failure order. Thrown errors become tagged execution
failures; Promise-like results are rejected. Async callbacks are outside the
initial API contract.

Explain shows local expressions with operation identifiers, their arguments,
and the provider boundaries without invoking callbacks. Sessions report actual
computation invocation and input/output row counts. Enriched explain includes SQL
and bindings for supported first-party SQL fragments; data-dependent lookup
statements are only known during execution. Sessions retain intermediate metadata,
not intermediate objects, and capture final rows only when requested.

Materialization limits still apply. Synchronous callbacks cannot be interrupted
mid-call; execution checks the deadline after each computed expression. Row limits
do not bound the size of objects a trusted callback allocates.

Wrapping an inferred object result with `col.json(derive(...))` preserves its
TypeScript shape for subsequent derived dependencies, including qualified view
references. If column options supply or may supply a `coerce` callback, the JSON
shape becomes `unknown`: a coercer can replace the object. Narrow that value
before reading its fields. This also applies to native columns and view references.
Unvalidated provider JSON remains unknown.

`EXISTS` and `NOT EXISTS` skip derived values used only in the inner SELECT list.
Inner predicates and operations that affect existence still demand their inputs.
Explicit CTE column lists, such as `WITH d(documentId, text) AS (...)`, rename the
query's outputs by position and must contain one unique name per output column.
SELECT-local aliases in `HAVING` and `ORDER BY` resolve before this renaming.
