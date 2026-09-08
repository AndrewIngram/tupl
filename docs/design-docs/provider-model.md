# Provider Model

Providers are rel-subtree compilers.

## Primary contract

The canonical provider surface is:

- `canExecute(rel, context)`
- `compile(rel, context)`
- `describeCompiledPlan?(plan, context)`
- `execute(plan, context)`

Compiled plans are provider-specific payloads. `tupl` does not assume SQL text is available or executable.

## Design intent

- Providers do not define semantics.
- Providers do not need planner-internal knowledge beyond canonical rel shapes.
- Provider authoring should prefer rel-shape and field-policy helpers over secondary capability vocabularies.
- Optional helper layers may exist for narrow patterns like keyed scans or lookup optimizations, but they are not the main semantic contract.

## Non-relational sources

- Non-relational backends should still present a rel-first surface.
- Redis-like providers may support only narrow scan shapes, but they should still answer support in terms of rel subtrees.
- Helper APIs should make field-sensitive `canExecute(...)` easy to author without forcing providers to hand-walk trees.

## Compile vs execute

- `compile(...)` lowers canonical rel into a provider-owned compiled plan.
- `execute(...)` runs that compiled plan.
- This separation exists to support explainability, plan descriptions, and future caching/replay opportunities without exposing provider internals upstream.

## Explain descriptions

- Runtime owns two explain modes internally:
  - basic fragment descriptions that never compile provider plans
  - enriched provider descriptions that may compile supported provider fragments
- Providers do not need a separate public explain-only method in the current model.

## SQL-like adapter path

- Ordinary SQL-like adapters should start with `createSqlRelationalProviderAdapter(...)`.
- The helper should feel like manual provider authoring:
  - top-level lifecycle/config fields
  - one nested `queryBackend` for backend-specific query translation
  - optional `advanced` overrides only for real backend exceptions
- `createRelationalProviderAdapter(...)` remains the lower-level escape hatch for unusual adapters.

## Mandatory entity scope

A provider's `scope` or `base` callback restricts the entity's rows for the current execution context. Apply it to every source before joins, aggregation, set operations, CTEs, windows, sorting, and pagination. Lookup paths must apply the same restrictions. Resolve callbacks at execution time, including when a compiled plan is reused with another context, and propagate callback failures.

Drizzle and Kysely use scoped derived tables for relational reads. Objection uses its scoped base queries as derived sources. Applying mandatory scope as a final `WHERE` predicate on an outer join is incorrect: it removes unmatched rows instead of joining the restricted inputs. Unscoped sources can use their physical tables directly.

Query translation hooks may be awaited. Adapters with thenable query builders must carry those builders inside a non-thenable object throughout translation, including set-operation branches and CTEs. Objection uses `{ builder }`; only `executeQuery` consumes the thenable.

The database-backed operation matrix in `test/__tests__/provider-scope.test.ts` checks these contracts against SQLite for all three SQL providers. Redis shares context-aware key construction and decoding between its keyed scan and lookup paths.

## Containment assurance

The result invariant is `query(scoped tables)`: user predicates can restrict the authorized inputs but cannot redefine them. Each SQL provider shares one physical-source constructor between relational reads and lookup reads. Scope predicates live inside derived sources, including on lookup paths; user predicates apply outside. This does not require materialized CTEs. Scope callbacks and provider implementations remain trusted application code.

Public SQL is validated against the declared schema before execution. Validation traverses expression subqueries as well as ordinary relational children, and column membership uses own properties. Physical table names and undeclared columns must not reach backend execution through aliases, nested expressions, or prototype properties.

`test/__tests__/query-containment.test.ts` exercises Drizzle, Kysely, and Objection against SQLite and PGlite, with normal pushdown and scan-only fallback. Its reference databases are populated independently with only authorized rows and declared columns. Generated Boolean expression trees are rendered into scans, aggregates, CTEs, EXISTS subqueries, unions, and windows. Each generated query runs before and after changing hidden rows and hidden fields, and both results must match the reference. Fixed cases also cover joins, self-joins, set operations, pagination, null semantics, scope errors, and rejection before backend dispatch. Redis's keyed reads are covered separately in `test/__tests__/redis-containment.test.ts`.

Provider capability overrides are authoritative: an explicit unsupported result must not fall through to generic SQL compilation. Fallback execution must retain the same scoped inputs and SQL three-valued Boolean semantics. Scan translation must honor the requested output names so fallback mapping does not turn qualified values into nulls.

These tests establish evidence for result containment over the covered grammar; they are not a proof for arbitrary SQL. Unsupported syntax is rejected. Error-message and timing noninterference, multi-connection PostgreSQL behavior, and correctness of application-supplied scope callbacks are outside this suite. PGlite exercises PostgreSQL SQL semantics through its Drizzle, Kysely, and Knex adapters; it does not replace production-server concurrency or network-driver testing.
