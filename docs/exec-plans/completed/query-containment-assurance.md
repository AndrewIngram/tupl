# Query containment assurance

## Invariant

Every successful public query must have the same results as execution over only the rows authorized by each table scope. Changing unauthorized rows must not change those results. Unsupported queries must fail closed. Scope callbacks are trusted application code; user SQL is untrusted.

## Work

1. Build shared SQLite/PostgreSQL (PGlite) fixtures with real provider execution, authorized-only reference databases, and scan-only fallback mode.
2. Add explicit adversarial SQL regressions and shrinking fast-check Boolean AST generators, plus hidden-row mutation properties.
3. Reproduce and fix projection/predicate/schema-boundary failures exposed by the harness.
4. Audit physical-source access in all providers and centralize scoped construction across relational reads and lookups where needed.
5. Run lint, uncached workspace typechecks, all tests, and formatting; document findings and remaining error/timing limitations.

## Progress

- Started after completing initial provider fixes and toolchain upgrade.
- Completed the public-query harness, provider source audit, regression fixes, and workspace verification.

## Confirmed findings and remediation

### High: expression subqueries bypassed schema validation

- Vulnerability: schema validation visited relational children but skipped subquery plans embedded in filter and projection expressions.
- Exploit path: a public query could reference a schema-undeclared physical column inside `EXISTS`, for example `EXISTS (SELECT id FROM details WHERE secret IS NOT NULL)`.
- Impact: undeclared column values could influence visible query results. No new cross-tenant row-return escape was established in this pass.
- Evidence: `packages/schema-model/src/rel-schema-validation.ts`; the public SQL rejection matrix failed before recursive expression validation was added.
- Remediation: traverse expression subqueries before execution and validate their scans against the declared schema. Retain the inner correlation key when rewriting EXISTS so it is selected and validated before decorrelation.
- Required tests: nested filter/projection/CTE/set-operation references to hidden tables and fields must fail before any backend statement is dispatched. Added across all six SQL provider/dialect combinations and both execution modes.

### Medium: inherited object properties counted as declared columns

- Vulnerability: column validation used JavaScript's `in` operator, which includes prototype properties.
- Exploit path: references to `constructor` and `__proto__` could pass schema validation and reach execution.
- Impact: the declared-column boundary was not fail-closed; some malformed queries succeeded or failed only after backend dispatch.
- Evidence: `packages/schema-model/src/rel-schema-validation.ts`; the rejection matrix covers these names in projections, predicates, ordering, joins, and subqueries.
- Remediation: use own-property checks for table and column membership.
- Required tests: reject inherited names in both ordinary queries and nested expressions without dispatch. Added.

## Correctness fixes supporting the containment oracle

- Honor scan output names in SQL translation and request unqualified physical output names at the runtime scan boundary. This fixes null-valued fallback projections and incorrect predicates caused by missing row values.
- Preserve SQL unknown/null in comparisons, Boolean operations, membership, LIKE, and BETWEEN. Retain NULL inside Drizzle IN/NOT IN lists.
- Make NOT IN subquery lowering null-aware, including the empty-right-side case. The null/emptiness checks can require additional scoped subquery execution; no performance benchmark is claimed.
- Preserve independent conjuncts next to OR expressions so keyed providers retain their required key restriction.
- Bind Objection's DISTINCT FROM predicates through `whereRaw` identifier/value placeholders because Knex rejects that operator through ordinary `where`.
- Preserve explicit unsupported strategy decisions instead of replacing them with generic compilation. Drizzle self-joins and unsupported projected joins then use scoped local execution.
- Reject multi-column semi-join compilation through single-column SQL IN subqueries and use scoped local execution instead.
- Share scoped source construction across reads and lookups in each SQL provider. Redis already funnels both paths through context-aware key construction and decoding.

## Test operation

The default suite runs 100 generated cases per SQL provider/dialect/mode combination (1,200 total), plus 100 Redis cases. Fast-check reports a seed, shrink path, and minimized expression on failure; copy the seed/path into the failing property's options to replay it, and retain the minimized case as a fixed regression. Existing provider-level generated lookup coverage remains in place.

PGlite is pinned to 0.5.8 in the test workspace and playground to keep Drizzle's private SQL types consistent across consumers. The harness uses the documented PGlite adapters for Kysely and Knex. Unrelated floating Redis and TypeScript versions were retained; a fresh frozen-lockfile installation was verified.

## Assumptions and limits

- Scope callbacks, entity configuration, and custom provider code are trusted. The invariant is preservation of the configured scope, not validation of the application's authorization policy.
- The oracle compares row multisets, preserving duplicates. Explicit pagination and window outputs are checked, but unspecified row order is not treated as meaningful.
- The generated grammar is bounded. Unsupported syntax, including current wildcard projection shapes, must fail closed rather than pass vacuously as an empty result.
- No timing/error-channel proof, network-driver coverage, production PostgreSQL concurrency coverage, or materialization benchmark is claimed.

## Verification

- `vp lint` passed.
- Uncached workspace typechecks passed across all 18 packages.
- `vp test` passed: 3,161 tests across 81 files, including 1,300 new generated cases per run and the existing 300 generated lookup cases.
- Uncached workspace builds passed across all 16 build tasks.
- `vp fmt` passed.
- A fresh `pnpm install --frozen-lockfile` succeeded.
- Reviewed the two updated planner snapshots: correlated subqueries now retain and expose the correlation key required for validation and execution.
