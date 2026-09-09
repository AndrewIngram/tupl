# Incremental SQL Standards Roadmap

This roadmap defines how `tupl` expands SQL support while keeping the user-facing API stable:

1. Define providers.
2. Build an executable schema from provider-owned entities.
3. Execute SQL with `executableSchema.query(...)`.

Planning remains internal; users write SQL, not plans.

## Baseline

Implemented:

- `SELECT ... FROM ...`
- `SELECT *`, `alias.*`, and mixed wildcard/expression projections over public tables, views, joins, CTEs, and derived tables
- `INNER JOIN ... ON a = b` (equality joins)
- `LEFT JOIN ... ON a = b`
- `RIGHT JOIN ... ON a = b`
- `FULL JOIN ... ON a = b`
- `WHERE` with boolean predicate trees (`AND`, `OR`, `NOT`)
- Operators: `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `BETWEEN`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
- `ORDER BY` column refs and ordinals
- `LIMIT`, `OFFSET`
- `GROUP BY` + aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `GROUP BY` ordinals (including computed select-list expressions via local fallback)
- `COUNT(DISTINCT col)`
- `HAVING` with aggregate expressions, grouped source columns, and SELECT aliases;
  ambiguous unqualified input names are rejected
- `SELECT DISTINCT`
- Set operations: `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`
- Subqueries in predicates: `IN (SELECT ...)`, `EXISTS (SELECT ...)`
- Scalar subqueries in `WHERE` and `SELECT`
- Non-recursive and recursive `WITH` CTEs, including declared output column lists
- Derived tables in `FROM` and correlated subqueries in the decorrelatable subset
- First local scalar-expression layer for read queries:
  - arithmetic `+`, `-`, `*`, `/`, `%`
  - string concat
  - `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTR`
  - `COALESCE`, `NULLIF`
  - `ABS`, `ROUND`
  - `CAST`
  - searched `CASE`
- Core window functions:
  - ranking: `ROW_NUMBER`, `RANK`, `DENSE_RANK`
  - aggregate windows: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
  - value/navigation: `LAG`, `LEAD`, `FIRST_VALUE`
  - `PARTITION BY` + `ORDER BY`, named windows, and explicit `ROWS` frames
- Synchronous TypeScript-derived table/view columns with shared dependencies and
  demand-driven local computation; see [the schema guide](building-a-schema.md#derived-columns-in-typescript)
- Provider capability helper vocabulary and route-family diagnostics for pushdown/rejection decisions
- Structured fallback diagnostics with SQLSTATE-like classes
- Query/runtime fallback policy controls for unsupported or expensive provider pushdown
- Rel-first provider compilation with local fallback when a provider rejects a subtree
- Optional keyed lookup helpers for targeted execution optimizations
- Opt-in query sessions via `createExecutableSchemaSession(...)` from `@tupl/runtime/session`
  - Pull-based observations of actual execution in completion order
  - One retained execution result or failure per session
  - No session concurrency scheduler; local branches execute in executor order
- Schema constraint metadata: `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`
- Structured `CHECK` metadata (`kind: "in"`) and enum-derived checks
- Optional query-time constraint validation modes: `off`, `warn`, `error`
  - runtime checks: `NOT NULL`, primary-key uniqueness, unique-key uniqueness, enum/CHECK validation
  - foreign-key runtime checks are not implemented

Unsupported:

- Computed-expression pushdown is still partial and adapter-specific
- `RANGE`/`GROUPS` window frames and `LAST_VALUE`/`NTH_VALUE`
- Cost-based physical planning
- Some provider-specific advanced rel pushdown shapes
- Writes (`INSERT`, `UPDATE`, `DELETE`)

Target direction:

- SQL standards compliance for read queries over time.
- Keep parser/planner/executor behavior converging with SQLite parity for supported subsets.
- Use a single in-house parser targeting SQLite SQL, with no parser fallbacks/workarounds.
- Treat schema constraints as communication-first metadata and optional runtime checks (not at-rest guarantees).
- Defer index metadata and index-driven planning until constraint semantics are fully settled.
- Continue expanding feature support milestone by milestone.
- Keep performance pragmatic: semi-optimal pushdown and batching where possible, without pursuing full database-style optimization.
- Use runtime fallback policy and materialization limits as explicit execution
  controls. Provider `canExecute` decisions define the supported pushdown shapes.

## Wildcard projections and output names

Projection wildcards expand in place before aggregate and window analysis. Bare `*` uses FROM/JOIN order, then each relation's declared public column order. `alias.*` uses the visible relation alias. Public calculated columns participate; private physical columns do not. CTEs and derived tables use their SELECT output order, and recursive references use the seed's output shape.

Rows are objects keyed by output name. Duplicate output names are rejected for wildcard and explicit projections before provider execution. For example, when both joined relations expose `id`, use `SELECT u.*, o.id AS order_id` instead of `SELECT *`. No automatic qualified names are generated. Unaliased expressions use the existing default name, so multiple expressions may need explicit aliases too.

`COUNT(*)` keeps its aggregate meaning. Expanded projections must satisfy normal GROUP BY rules. Set-operation branches must have equal column counts after expansion; subsequent branches use the first branch's output names by position.

## Milestones

### M1: Predicate Richness

Goal: support richer filters with controlled planning complexity.

Status: complete for runtime semantics; planner normalization is out of scope for this milestone.

Execution contract impact:

- No new public table methods.
- `scan(...)` receives richer normalized predicate structures.

### M2: Post-Aggregation Filtering

Goal: unlock common analytical SQL patterns.

Status: complete for `HAVING`; aggregate-route optimizations can be expanded.

Execution contract impact:

- No new public methods.
- Planner inserts post-aggregate filter steps when needed.

### M3: Set Operations

Goal: enable report composition and unionable result pipelines.

Status: complete for `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT`.

Execution contract impact:

- Introduce internal set-op steps over row sets.
- No resolver API changes required.

### M4: Distinct and Join Expansion

Goal: close major SQL gaps for read-only workflows.

Status: complete for target scope (`SELECT DISTINCT`, `LEFT/RIGHT/FULL JOIN`).

Execution contract impact:

- No new public methods.
- Planner/executor adds dedicated distinct/join strategy steps.

### M5: Subqueries

Goal: improve expressiveness while keeping execution safe.

Status: complete for the current read-query target, including correlated forms in the supported decorrelatable subset and derived tables in `FROM`.

Execution contract impact:

- No new public methods.
- Planner lowers supported subqueries into canonical rel plus decorrelation rewrites.

## Writes (Explicit Non-Goal)

Writes remain explicitly unsupported.

Design reservation only:

- Keep IR and capability surfaces open so keyed writes can be introduced later.
- Do not imply write semantics/transactions in API behavior.
- Continue rejecting write SQL statements with clear errors.

## Performance Positioning

Performance is important but not the primary goal.

- `tupl` should avoid obvious inefficiencies and over-fetching.
- It should exploit available provider subtree pushdown and optional helper-level optimizations for practical efficiency.
- It is not intended to compete with database engines on optimizer sophistication.
- If a workload needs deep cost-based optimization, push computation to the backing store or specialized engine.

## Security Boundary

`tupl` is a query/planning/runtime layer, not an authorization system.

- The underlying domain/storage methods are responsible for enforcing security guarantees.
- Tenant scoping, row/column access control, and sensitive-data restrictions must be implemented in domain logic.
- `tupl` should not be treated as the source of truth for authorization correctness.

## Compatibility Matrix

| Feature                                  | Parser              | Planner | Executor | Provider contract impact    |
| ---------------------------------------- | ------------------- | ------- | -------- | --------------------------- |
| Basic select/join/filter                 | done                | done    | done     | rel subtree compile/execute |
| Aggregates/group by/having               | done                | done    | done     | rel subtree compile/execute |
| Non-recursive and recursive CTE          | done                | done    | done     | none new                    |
| Set ops (`UNION`/`INTERSECT`/`EXCEPT`)   | done                | done    | done     | none new                    |
| Derived tables and correlated subqueries | done                | done    | done     | none new                    |
| Window functions (current supported set) | done                | done    | done     | none new                    |
| Query execution observation API          | n/a                 | done    | done     | none new                    |
| Constraint runtime validation            | n/a                 | n/a     | done     | none new                    |
| Writes (`INSERT/UPDATE/DELETE`)          | explicit no-support | n/a     | n/a      | none                        |

## Release Gate for Each Milestone

Each milestone is complete only when all are true:

- Parser acceptance tests for supported syntax and clear unsupported errors.
- Planner tests showing step graph and pushdown decisions.
- Dual-engine integration parity tests (`tupl` vs SQLite) for supported shapes.
- `explain(...)` output updated to reflect new plan decisions.

Compliance test locations:

- `packages/runtime/src/__tests__/compliance/*-parity.test.ts`: SQLite parity scenarios split by capability.
- `test/__tests__/column-scope.property.test.ts`: generated name-resolution checks across providers, dialects, and native/local execution.
- `docs/parser-known-issues.md`: in-house parser behavior notes and known gaps.
