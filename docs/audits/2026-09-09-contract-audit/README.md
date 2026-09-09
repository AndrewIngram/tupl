# Immediate contract audit, 9 September 2026

Completed against `841528b7636cce40d0d0254fd6ad8330e0ee62ba`. Six new defects and three previously recorded defects were reproduced. This is not a clean assessment. No production fixes were made.

The nine findings were subsequently fixed. See [remediation and verification](./remediation.md). The results below preserve the original audit baseline.

## New findings

### F1. P1: Packed application imports fail

The packed `@tupl/schema` facade imports lower-layer packages whose exports still point at `src/*.ts`. A plain Node consumer cannot load those files under `node_modules`. Both ESM and CommonJS fail with `ERR_UNSUPPORTED_NODE_MODULES_TYPE_STRIPPING`; a NodeNext TypeScript consumer also fails on extensionless imports in the exported source.

Evidence: `main`, `module`, `types`, and `exports` in [provider-kit/package.json](../../../packages/provider-kit/package.json), [schema-model/package.json](../../../packages/schema-model/package.json), [runtime/package.json](../../../packages/runtime/package.json), and the other core packages. [consumer-results.json](./consumer-results.json) records the actual errors.

The test built the workspace, packed seven packages, and extracted the actual tarballs into a temporary consumer outside the workspace. Every tupl dependency came from a tarball. Only external `better-result` reused an installed copy. An ordinary offline installation was attempted first, but the package-manager metadata cache was missing; tarball extraction avoided network dependence without changing package manifests or export resolution.

Remediation: export compiled JavaScript and declarations for every supported entry point, and configure the corresponding package build entries. Verify both import styles and NodeNext types from tarballs. The provider-backed query and derived inference consumer examples remain blocked at import/type resolution.

### F2. P1: Aggregate output names overwrite grouped values

For this query, `label` contains the aggregate total instead of the category:

```sql
SELECT category AS label, SUM(n) AS category
FROM records
GROUP BY category
ORDER BY label
```

With category `a` and value `9`, the expected row is `{label: 'a', category: 9}`. The actual row is `{label: 9, category: 9}`. Both local and provider aggregate execution reproduce this on all three SQL adapters. HAVING and ordering can consequently use the wrong values too.

[select-project.ts](../../../packages/planner/src/select/select-project.ts) builds the aggregate output by concatenating bare grouping names and public metric aliases. Those names can collide even when final SELECT outputs are unique. The stored plan has duplicate aggregate fields named `category`; later projection cannot recover the overwritten value.

Remediation: assign collision-free internal aggregate identities, and bind grouping, HAVING, windows, sorting, and final projections to those identities. Add regression cases for both SELECT orders, aliases shadowing grouping fields, multiple qualified grouping fields with the same name, and enclosing CTE names.

### F3. P1: Compound query ordering and pagination apply to the final branch locally

```sql
SELECT id FROM records WHERE id < 3
UNION ALL
SELECT id FROM records WHERE id < 3
ORDER BY id LIMIT 1
```

Expected `[{id: 1}]`; scan-only execution returns `[{id: 1}, {id: 2}, {id: 1}]`. Without LIMIT, the ordered result is `[1,2,1,2]` instead of `[1,1,2,2]`. Related probes reproduce incorrect pagination for UNION, EXCEPT, and INTERSECT. All three adapters exhibit the local failure; their native set-operation routes pass these cases.

[structured-select-lowering.ts](../../../packages/planner/src/structured-select-lowering.ts) lowers the final branch's AST, including its trailing clauses, before constructing the `set_op`. The initial canonical plan therefore places sort/limit inside the right branch. Native SQL compilation happens to produce the expected compound SQL here, masking the incorrect logical placement.

Remediation: distinguish compound-level clauses during parsing/lowering and wrap the complete set operation. Test SQL and explicit equivalent RelNodes with local, mixed, and native routes, including three branches and offset.

### F4. P1: Local arithmetic loses NULL semantics and can return Infinity

`SELECT id, n + 1 AS n FROM records ORDER BY n,id` turns a null input into `1` locally; Drizzle's native expression produces null. Subtraction, multiplication, division, and modulo also convert null operands to zero in the adjacent probe. `n / 0` produces JavaScript `Infinity` locally while native Drizzle/SQLite produces null.

[evaluateNumericBinaryResult](../../../packages/runtime/src/runtime/execution/expression-scalar-functions.ts) converts operands with `Number(...)` before checking nulls and returns the arithmetic result without validating finiteness. These differences affect projected values, filters, and ordering whenever placement changes.

Remediation: define and implement null propagation and zero-divisor behavior at the scalar expression boundary. Cover each binary numeric operator, both null operand positions, zero divisors, and predicate/sort use through local and native routes. The evidence encoder preserves non-finite numbers explicitly; it does not serialize Infinity as an indistinguishable JSON null.

### F5. P2: Drizzle advertises modulo support but cannot execute it

A direct `project(scan)` RelNode computing `mod(n, 2)` gets `canExecute: true` and successful `compile`, then execution fails with `Unsupported computed projection function "mod" in Drizzle single-query pushdown.` A supported local operation therefore fails instead of falling back. Kysely and Objection decline the same direct fragment correctly.

[resolveDrizzleRelCompileStrategy](../../../packages/provider-drizzle/src/planning/rel-strategy.ts) validates the pipeline/base structure without checking all projected expression functions. `buildSqlExpressionFromRelExpr` in that file rejects `mod` later. The direct capability probe is recorded in [plans.ndjson](./plans.ndjson).

Remediation: make capability discovery validate the expression grammar that execution actually supports, or implement modulo translation. Add accepted-fragment execution tests and declined-fragment fallback tests for every exposed expression function.

### F6. P2: The valid output alias `__proto__` is lost

```sql
SELECT id AS "__proto__" FROM records WHERE id=1
```

Expected an own property named `__proto__` containing `1`. Local execution and Kysely/Objection return `{}`. Native Drizzle emits an empty projection and fails with a SQL syntax error. The adjacent alias `constructor` passes.

Name-keyed output objects use assignment into ordinary `{}` objects, including [rel-output-mapping.ts](../../../packages/schema-model/src/mapping/rel-output-mapping.ts) and [row-ops.ts](../../../packages/runtime/src/runtime/execution/row-ops.ts). Assigning `__proto__` invokes the inherited setter rather than creating the requested property. This is a reproduced output-correctness issue; the probes did not establish prototype pollution of shared objects or unauthorized data access.

Remediation: preserve arbitrary supported SQL names as own data properties throughout output construction and backend projection maps. Test reserved JavaScript property names on local and provider routes, using own-property assertions rather than JSON text alone.

## Reproduced known defects

| ID  | Recorded issue                         | Observed behavior and required remediation                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| --- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| K1  | Embedded scan modifiers                | A view returning a raw scan with `orderBy id`, `limit: 1`, `offset: 1` returns all six rows, rather than ID 2. Reproduces for all three SQL adapters, including scan-only routing. Canonicalize or reject embedded modifiers before SQL compilation; keep explicit wrapper equivalence tests.                                                                                                                                                                                        |
| K2  | Public source coercion                 | `col.string('id', {coerce: v => String(v) + '!'})` fails native projection with `Column id must be a string.` Scan-only mapping correctly returns suffixed strings. Make coercion a planner-visible value transformation and preserve standalone mapping.                                                                                                                                                                                                                            |
| K3  | Aggregate navigation window validation | `SELECT category,COUNT(*) AS c,LAG(n) OVER (ORDER BY category) AS prev FROM records GROUP BY category ORDER BY category` succeeds with all-null `prev`, despite `n` being absent from aggregate output. Reject unresolved navigation `value`/`defaultExpr` references during lowering. SQLite permits bare aggregate inputs in this query; its particular choice of a row is not proposed as tupl's contract. The defect is accepting an unresolved reference and fabricating nulls. |

K1/K2 remain in the [debt tracker](../../exec-plans/tech-debt-tracker.md). K3 was recorded there as a validation gap; this audit establishes that it can silently return nulls, rather than merely failing later.

## Coverage and limitations

The bounded audit is complete. Cells below have either executed evidence or an explicit limitation. Passing checks establish behavior for these cases, not for the entire grammar.

| Planned pass               | Evidence                                                                                                                                                                                       | Assessment                                                                                                                                                                                                                                                                                                                               |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Execution placement        | 40 cases × 3 SQL providers × 3 capability profiles = 360 comparisons. Runtime observations record actual routes and provider call counts.                                                      | 262 agree with the reference; 98 differ. Differences include repeated manifestations of the findings and the integer-division contract question below. Profiles permit different routes; they do not claim every case ran remotely.                                                                                                      |
| Rewrite correctness        | Source inspection plus passing pipeline-order, projection-order, aggregate-ordering, and derived-column tests; canonical plans retained for failing SQL.                                       | Existing order/scope fixes hold in their tested forms. F2/F3 occur earlier in logical lowering. Rewrite inventory below records inspected preconditions and blockers.                                                                                                                                                                    |
| Authoring/value contracts  | SQL matrix, raw-scan view, direct modulo RelNode, existing explicit-wrapper views, wildcard/CTE/derived tests, uncached type checks including derive typing assertions.                        | F2/F4/F6 and K1/K2/K3. Direct RelNode equivalence is targeted, not exhaustive across every generated SQL case.                                                                                                                                                                                                                           |
| Provider capability/scope  | Direct capability probe; baseline database scope/containment suites; 14 additional lifecycle/scope checks.                                                                                     | F5. No new scope leakage reproduced. Existing suites include SQLite and PGlite across SQL adapters and Redis containment fixtures. New semantic comparisons use SQLite only.                                                                                                                                                             |
| Lifecycle/resources        | Passing session-correctness and materialization-guardrail suites; inspected row-growth/recursive limits and synchronous callback deadline behavior.                                            | Existing tests cover provider failures, repeated observation, timeout during compile/execute, late completion, joins, aggregate inputs, lookup buffers, recursive accumulation, and exact row limits. No new defect reproduced. Provider allocation, CPU within one callback, and examined rows remain outside a materialized-row bound. |
| Reuse/isolation            | Controlled overlapping queries on one schema, tenant B completes before failed A, both tenants queried again, three expected derivations; scoped aggregate/join/CTE and hostile tenant probes. | 14 checks pass. Caller mutation of prepared artifacts and multi-connection database cancellation remain unresolved coverage cells; no contract or result is inferred for them.                                                                                                                                                           |
| Consumer/public boundaries | Real tarballs, ESM/CommonJS imports, NodeNext types; hostile SQL literal, unusual aliases, undeclared tenant field, baseline containment tests.                                                | F1/F6. Runtime consumer examples are blocked by F1. Supported Node/TypeScript version matrix beyond the local versions was not run.                                                                                                                                                                                                      |

Additional boundaries:

- Existing async-result and type tests reject Promise-like derivations. Non-ROWS frame rejection passes in the baseline suite. Collection dependencies and row expansion have no supported API to exercise.
- Ownership inspection confirms `values`, `cte_ref`, `correlate`, and `repeat_union` block whole-subtree provider ownership. Existing recursive/correlated cases pass. Provider-normalization branches that assume those barriers were respected were not fuzzed with malformed internal plans; their recorded consistency question remains open.
- The security pass found no confirmed SQL injection, private-field disclosure, or cross-context data exposure in tested paths. Trusted provider/scope/callback code, error/timing noninterference, and arbitrary unbounded SQL are outside this evidence.
- No exhaustive optimizer equivalence proof, server PostgreSQL/MySQL matrix, or new real Redis-server test was attempted. Baseline PGlite and Redis-fixture coverage must not be described as those tests.

## Inspected rewrite preconditions

| Rewrite                                | Required precondition                                                                                    | Positive evidence and case that must block motion                                                                                                                                                                                       |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Provider pipeline flattening           | Preserve input scope and operator order.                                                                 | Existing wrapper tests pass safe native plans and reject aggregate/filter/sort above an earlier LIMIT, including WITH/set wrappers. Owner: `provider-kit/.../relational-core.ts`, `canFlattenPipeline`.                                 |
| Projection composition/hoisting        | Rebind every crossed filter/sort; retain computed ordering dependencies.                                 | Projection-order and pipeline tests pass renamed source filters/sorts and retained computed sort. Computed keys without a source mapping or retained output must keep the boundary. Owner: `planner/.../provider-rel-normalization.ts`. |
| Filter movement through derivation     | Only independently movable conjunctions, and only through a cardinality-preserving projection.           | Derived-column AND/OR matrix passes. A predicate depending on a derived result remains after computation; OR cannot be partly pushed. Owner: `local-computation-planning.ts`, `move`.                                                   |
| Sort/LIMIT movement through projection | LIMIT crosses cardinality-preserving projection; sort additionally needs source references for all keys. | Projection-only callbacks run after legal native pagination; derived-filter pagination tests retain compute/filter before limit. Joins/aggregation are not crossed by this rewrite.                                                     |
| Unused output pruning                  | Preserve comparison/key inputs and singleton aggregate cardinality.                                      | Empty aggregate, EXISTS, and distinct/set/derived tests pass. Multiplying joins and distinct comparison inputs remain even when their public values are unused. Owner: `local-computation-planning.ts`, `prune`.                        |
| Shared computation staging             | Share only an operation at its owning stage and row occurrence within one execution.                     | Existing invocation-count/self-join tests and new concurrent-context checks pass. Separate executions and duplicate row occurrences must not reuse cached values.                                                                       |
| CTE demand pruning                     | Propagate demanded columns through referenced definitions; preserve cardinality-affecting work.          | Unused/EXISTS/correlated derived CTE tests pass. Referenced filters, grouping, distinctness, and pagination cannot disappear with a selected value.                                                                                     |

## Contract question: integer division

`SELECT n/2 AS v FROM records WHERE id=1` returns `4.5` for `n=9` across the tested tupl routes, while literal SQL in SQLite returns integer `4`. Unlike F4, this case did not change with execution placement in this matrix. Decide whether tupl promises SQLite integer division or floating-point arithmetic before selecting a regression expectation. Do not conceal this difference with numeric rounding in the comparator.

## Verification record

- HEAD: `841528b7636cce40d0d0254fd6ad8330e0ee62ba`; initial tree contained only the two untracked audit plans.
- Local Node `24.20.0`, pnpm `10.0.0`, better-result `2.7.0`, better-sqlite3 `12.6.2`, Drizzle `0.45.1`, Kysely `0.28.11`, Knex `3.1.0`.
- `vp lint`: pass, including the audit scripts.
- `vp run -r typecheck`: pass, 18 cached workspace tasks. `pnpm -r exec tsgo --noEmit -p tsconfig.json`: uncached pass.
- `vp test`: 91 files, 3,452 tests pass. Focused contract rerun: 16 files, 240 tests pass.
- `vp run -r build`: pass. The playground emits a bundle-size warning.
- Formatting and whitespace checks run before finalizing these artifacts.
- Additional observations: 360 semantic comparisons, 3 direct provider-capability probes, 14 lifecycle/scope checks, and 3 failing consumer checks.

The reference database is populated independently and runs the original SQL directly. Tupl results use deep value comparison, including row multiplicity and ordered rows. Pagination queries use an explicit ordering; simple set-order probes have identical tied rows. No coercion, numeric rounding, or stringification is applied to make results agree.

## Reproduction and artifacts

From the repository root:

```sh
node docs/audits/2026-09-09-contract-audit/run.mjs
vp run -r build
python3 docs/audits/2026-09-09-contract-audit/consumer.py
```

The first command uses the normal workspace Vite loader without a listening server. It runs the semantic and lifecycle probes and rewrites the evidence files. These are audit probes that record disagreements, so their process exit status alone is not a passing audit assertion.

- [probe.mjs](./probe.mjs): full fixtures and SQL cases, direct capability checks, expected/actual comparisons, execution observations.
- [results.ndjson](./results.ndjson): one semantic comparison per line; non-finite numbers have explicit markers.
- [plans.ndjson](./plans.ndjson): first failing plan per SQL text and direct capability outcomes. Plans describe intended work; execution observations establish routes actually used.
- [lifecycle.mjs](./lifecycle.mjs) and [lifecycle-results.json](./lifecycle-results.json): controlled concurrency and scope probes.
- [consumer.py](./consumer.py) and [consumer-results.json](./consumer-results.json): package reproduction and diagnostics. The temporary directory is recorded in the results.

## Remediation and automation handoff

Fix F1 before publishing. Prioritize F2/F3/F4 and known K1/K2 for wrong values, row counts, and placement-dependent behavior; then F5/F6 and K3. Each finding above includes the owning boundary and required regression tests. Fixes should preserve these reproducers while reducing them into the owning package's normal tests.

Seed the automation suite with the alias collision, compound clause placement, numeric null/zero cases, capability mismatch, reserved output names, embedded scan/wrapper equivalence, and coercion route equivalence. Keep packed-consumer checks independent of workspace aliases. Add the 14 lifecycle cases and existing materialization/containment cases to the contract inventory before building a broader generator.
