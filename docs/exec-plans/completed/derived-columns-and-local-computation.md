# Derived columns and local computation

Status: completed.

Date: 2026-09-09.

## Outcome

Application authors can expose values computed by existing synchronous TypeScript functions through ordinary tables and views. Tupl owns dependency selection, computation scheduling, and subsequent relational execution. Applications do not need a custom provider to describe these values.

Queries that do not reference derived values anywhere must remain eligible for the same provider execution as before. Declaring a derived column must not make the table local. Queries that do reference derived values should fetch the smallest safely obtainable input and compute only the dependency chains they need.

The integration report motivating this work used published 0.7.0 and reported 43 integration tests and six snapshots. Those external tests are not in this repository and have not been reproduced. Source assessment at 19f0d95 found relational calculated columns, batched lookup execution, and view composition, but no declared TypeScript computation graph or callback expansion operator. Recheck current code before implementation; wildcard support is being implemented independently in the shared checkout.

## Agreed interface direction

Add `derive` to the existing table and view `columns` callbacks. It accepts a named dependency object whose values are column definitions or other derive handles, and a pure synchronous callback. Its result is a typed deferred computation handle, not a public SQL column definition. Existing column builders expose the result.

Sync-only callbacks are agreed initial scope, not a fundamental constraint of derived computation. There is no current async transformation use case. Do not implement async scheduling now; future support would need bounded concurrency, shared in-flight results, and cancellation/failure semantics. Declared dependencies and no hidden data fetching remain separate requirements regardless of whether callbacks are synchronous or asynchronous.

```ts
const documents = builder.table("documents", provider.entities.documents, {
  columns: ({ col, derive }) => {
    const title = col.string("title", { nullable: false });
    const body = col.json("body_json");
    const parsed = derive({ body }, ({ body }) => parseDocument(body));
    const rendered = derive({ document: parsed }, ({ document }) => renderDocument(document));

    return {
      id: col.id("id"),
      title,
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

Only returned entries become public SQL columns. The body source, parsed document, and rendered object can remain private. Column definitions have two roles: describing a public column when returned and providing a typed dependency when referenced. Reusing a derive handle explicitly shares computation; identical callback text does not imply sharing.

Views use the same helper and existing qualified inputs such as `col.string(users, "firstName")`. Existing expression-based calculated columns remain supported. Support calculated column definitions as dependencies too, with one canonical dependency-resolution path rather than competing evaluators.

## Semantic contract

- Dependency analysis includes SELECT, WHERE, JOIN, GROUP BY, HAVING, ORDER BY, windows, DISTINCT, and enclosing relations. An unselected value can still be required elsewhere.
- An unused computation is never invoked and its exclusive source columns are not fetched. COUNT(*) alone does not require cardinality-preserving computed outputs. SELECT * includes every public derived column, but no private intermediate.
- A shared computation runs at most once per row occurrence at its owning computation stage in one execution. Do not cache by primary key, value equality, or across queries. Self-joins and repeated relation invocations have distinct ownership; sharing a declaration does not conflate their rows.
- Preserve SQL row multiplicity, null semantics, outer-join behavior, aggregate inputs, ordering, and pagination. JavaScript callbacks compute values; tupl evaluates SQL predicates over those values.
- Callbacks receive nullable inputs as null where appropriate and explicitly handle them. Do not implicitly skip callbacks on null. Normalize source inputs consistently with existing column reads.
- Private intermediates may be structured objects or Maps. Public results must satisfy their declared SQL type and nullability. Document intermediates as immutable inputs to pure computations.
- Reject Promise-like callback results at the type boundary and fail with a tagged runtime error if untyped code returns one. Wrap thrown application errors with useful computation/relation context; never silently replace a failure with null or an empty result.
- Purity is an author contract, not something the type system proves. Callbacks must not perform database calls, mutate shared state, or depend on invocation count, wall-clock time, or randomness.
- Evaluation is demand-driven. A callback for a row eliminated before its computation may never execute and therefore may never throw. SQL predicate text order does not promise JavaScript evaluation order. Define legal motion of potentially throwing computations before enabling reorderings.
- Explain does not invoke application computations. Execution observations report actual computations and routes, including runtime lookup or provider fallback.

## Implementation sequence

### 1. Establish typed handles and graph ownership

- [x] Extend typed column definitions to retain value type and nullability. Preserve provider read metadata through table declarations and view references rather than widening values to unknown or using casts to recover them later.
- [x] Keep unvalidated JSON unknown when no authoritative stronger type exists. Infer arbitrary intermediate return types and named callback input types.
- [x] Define the accepted dependency variants and ownership in types. Reject ordinary values, async callbacks, incompatible public output types, and references outside the declaration's valid relation scope.
- [x] Specify how existing calculated expressions and source coercions become typed dependencies. Preserve existing table/view authoring syntax and keep provider physical naming separate from logical naming.
- [x] Introduce stable computation identities and graph validation during schema preparation. Reused handles share identity; separate declarations remain separate. Detect invalid references/cycles in runtime-authored schemas without adding string-based forward references just to permit cycles.
- [x] Keep trusted callbacks in an owned schema/runtime registry. Plans carry inspectable descriptors and dependency identities; do not send executable callbacks or private object values to providers or expose them in serialized explain output.
- [x] Record the representation choice in the relational pipeline and package architecture docs. Respect foundation → provider-kit → schema-model → planner → runtime → schema layering.

### 2. Deliver a complete local computation path

- [x] Add a canonical representation for cardinality-preserving local computation that retains its inputs, outputs, dependencies, and stage identity. Determine whether this is a dedicated relational node or an explicit local expression form before implementation.
- [x] Add the representation to traversal, validation, rewriting, alias handling, output inference, provider ownership, execution, and diagnostics. Providers may own supported descendants but never the JavaScript computation itself.
- [x] Execute the required graph in dependency order with per-execution, per-stage row sharing. Release intermediates when their consumers finish; do not retain document objects in session event history.
- [x] Support table and view outputs, private provider entity inputs, existing calculated-column dependencies, joins, aggregates, windows, DISTINCT, CTEs, and set operations wherever the existing relational semantics support them. Explicitly reject any unsupported shape rather than guessing.
- [x] Preserve configured provider scopes on both scans and lookup paths. Private declared dependencies are library-internal reads, not newly queryable public SQL fields.
- [x] Integrate cancellation, timeout checks, row/materialization limits, and callback errors with current Result and session behavior. Document that synchronous callbacks cannot be preempted mid-call and row limits do not bound the size of arbitrary intermediate objects.

### 3. Make demand pruning and safe reduction release requirements

- [x] Propagate required outputs backward across expanded views and calculation graphs, including references from predicates, join keys, windows, aggregates, sorting, and distinctness. Remove unused computation nodes and source columns before provider fragment assignment.
- [x] Preserve operators that affect row count even when their projected values are unused. In particular, pruning columns must not remove a multiplying join or change COUNT(*).
- [x] Split conjunctions into independently movable native and derived predicates. Push supported native predicates toward provider scans only with valid relation/alias/null semantics. Do not split OR or push predicates through outer joins, limits, grouping, windows, or set operations without a proven equivalence.
- [x] Delay computations used solely for final projection until after native filtering, ordering, and pagination when safe. Keep computation before any filter, join, grouping, sorting, or distinct operation that consumes its value.
- [x] Order independent filters to reduce expensive computation using conservative deterministic rules. Do not claim cost-based optimization without estimates, and respect the agreed error/evaluation contract.
- [x] Retain native provider joins and aggregates where legal; do not force a whole table or whole query local because one output is derived. Avoid moving computations across joins merely because the callback's read columns are known.
- [x] Preserve eligible targeted lookup execution using locally computed keys. Validate lineage instead of assuming a derived key maps to a physical left scan column. Deduplicate non-null keys and honor lookup batch/row limits.

These are observable requirements, not optional later tuning. The first implementation is complete only when native-only queries avoid new local work and representative mixed queries demonstrably reduce fetched rows and columns.

### 4. Make plans and provider work reviewable

- [x] Explain computation dependencies, shared intermediates, native/local boundaries, and operator ordering using stable identifiers and public names where available. Give private intermediates useful generated identifiers without requiring users to name every handle.
- [x] Align static strategy selection and runtime execution for supported lookup cases. If a route is conditional, describe the condition rather than presenting one speculative strategy as executed work. Reconcile the existing lookup-plan tech-debt entry with actual current code.
- [x] Report actual computation invocation counts, rows entering/leaving relevant stages, and chosen routes through existing session observations. Do not report unexecuted work as completed.
- [x] Expose generated SQL and bindings through first-party SQL provider descriptions where available, preserving basic explain's no-compilation behavior. Distinguish planned fragments from actual runtime statements; data-dependent lookup bindings require execution-time observation.
- [x] Verify Objection with real Knex queries, not only fake query builders. Include a public view joining a private entity in a second provider and assert the actual keyed SQL/bindings alongside the observed route.

### 5. Validate and document the feature

- [x] Add compile-time tests for input inference, intermediate types, nullability, output mismatch, async rejection, and typed view dependencies. Test expected type failures as well as successful inference.
- [x] Use a real database-backed fixture plus a straightforward reference evaluator for TypeScript transformations. Compare row multisets, explicit ordering, null behavior, and multiplicity. Assert generated SQL, dependency sets, invocation counts, plans, and execution observations separately.
- [x] Add bounded generated cases for predicate combinations, duplicate join keys, nulls, empty inputs, and pagination. Include source fixtures with invalid JSON to pin down demand-driven failure behavior.
- [x] Run a security audit after the significant refactor, concentrating on public/private column containment, provider scopes, callback trust, and resource ownership.
- [x] Update building-a-schema, relational-pipeline, provider-model, planner-invariants, relevant public package docs, and the tech-debt tracker in the implementation branch. Include a small document-rendering example showing shared intermediates and a native-only query.
- [x] Run `vp lint`, `vp run -r typecheck`, `vp test`, and `vp fmt`. Review the final diff and record actual outcomes. Move this plan to completed only when release requirements are met and any deferred work is explicitly tracked.

## Required acceptance cases

| Query or condition                                         | Required evidence                                                                                                                   |
| ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Native-only projection/filter/order                        | Same provider eligibility as without derived declarations; zero computation invocations; no exclusive derived input fields fetched. |
| COUNT(*) over a table with derived columns                 | No derived computation; correct count including duplicates.                                                                         |
| One derived output                                         | Only its transitive dependencies fetched and evaluated.                                                                             |
| Two outputs sharing a renderer                             | Parse/render each once per relevant row occurrence; both results correct.                                                           |
| Repeat the query after changing source data                | Fresh computation; no stale cross-query cache.                                                                                      |
| Native AND derived filter                                  | Native restriction executes in the provider when safe; local derived filter follows computation and precedes pagination.            |
| Native OR derived filter                                   | No unsafe partial pushdown that drops valid rows.                                                                                   |
| Native ORDER BY and LIMIT, derived output only             | Provider limits candidates first when safe; render only returned candidates.                                                        |
| Derived predicate with LIMIT                               | Limit counts matching rows; no premature provider limit.                                                                            |
| Derived grouping, ordering, HAVING, DISTINCT, window input | Correct relational semantics and visible required computation placement.                                                            |
| Join using a derived key                                   | Correct local join; targeted provider lookup where eligible; null and duplicate keys handled correctly.                             |
| Outer join with nullable dependency                        | Unmatched rows and callback null behavior preserved.                                                                                |
| Projection omits a one-to-many joined output               | Correct multiplicity and count; no unproven join removal.                                                                           |
| SELECT * from a public view                                | Includes public derived outputs; excludes private intermediates and undeclared physical fields. Coordinate with the wildcard task.  |
| Unused throwing computation                                | Never invoked; query succeeds if its actual dependencies are valid.                                                                 |
| Required callback throws or returns a Promise              | Tagged failure with computation context; session retains the failure without restarting work.                                       |
| Self-join, CTE reuse, separate queries                     | No accidental cache sharing across row/stage/execution identities.                                                                  |

## Subsequent optimization and composition work

Track these as separate implementation slices after the initial feature. They belong to the broader integration problem but must not become implicit promises of the first release.

### Cardinality-aware join planning

Propagate trustworthy unique keys and at-most-one-match properties through scans, projections, grouped aggregates, and supported latest-per-group relations. Use those proofs to remove unused left joins. Distinguish uniqueness from existence; removing an inner join needs stronger evidence. Preserve multiplicity for counts. Add narrow status/text selection cases that omit unrelated joins only when justified.

Use cardinality estimates or defensible bounds to choose whether native joins should precede computation or whether computing once before a multiplying join is cheaper. Define ordering and failure constraints before moving computations. A visible join alone is insufficient evidence for removal or reordering.

### Batched related-row dependencies

Build a document-rendering fixture requiring names and response metadata from another relation. First evaluate how far existing joins and grouped relations can express it. Design a collection dependency only if necessary: explicit keys, projected fields, batching, null/missing-row behavior, collection ordering, and query-local sharing must remain planner-visible. No arbitrary async per-row callbacks. Do not assume a new relational operator is required before this evaluation.

### Row expansion

Design a separate typed flatMap/expansion operation for tags and document references. Unlike derive, it changes cardinality. Define zero/many-row outputs, lineage, ordering, errors, and resource bounds. Filters on expanded values, joins, grouping, sorting, and pagination must follow the expansion where required. Callback dependency knowledge alone does not justify moving predicates across it.

### Incremental fetching

For a provider-ordered candidate stream plus local derived filtering and LIMIT, fetch batches until enough matches are found. This requires an explicit streaming/cursor contract, stable ordering and tie handling, offset semantics, scope preservation, cancellation, and limits on both returned and examined candidates. A generic OFFSET loop is not sufficient under changing source data. First release must use semantically correct materialized execution and disclose its working-set limit.

## Progress

- [x] Record the discussed interface and execution requirements.
- [x] Resolve representation, typing, ownership, and evaluation-motion details during implementation design.
- [x] Implement and verify the release scope above.
- [x] Record concrete follow-up plans or debt entries for deferred slices.

## Implementation decisions and verification

- Representation: a `local` scalar expression with an immutable operation ID and
  label. Callbacks stay in a schema-owned weak registry. The planner expands shared
  graphs into ordinary project stages, avoiding a parallel relational operator API.
- Ownership: typed definitions carry inferred values and nullability. Runtime
  declaration ownership checks reject borrowed handles; TypeScript cannot express
  the identity of a particular callback invocation. View references use qualified
  column definitions. Promise-like values are rejected even from untyped callers.
- Source coercion runs before dependency validation. Public derived values are
  validated after computation. A pre-existing gap for ordinary public source
  coercion through SQL fragments is separately recorded in the debt tracker.
- Pruning preserves joins, distinct comparison inputs, singleton aggregates and
  CTE cardinality. Unreferenced CTEs disappear. Scalar expression subqueries run the
  same rewrite pipeline in their own relation scopes.
- Safety audit exercised private-column rejection before SQL dispatch, type-changing
  private coercion, foreign handles, rejected Promises, and invalid public results.
  It found and fixed empty-projection overfetch, unused-aggregate cardinality loss,
  and async rejection escaping the query. Existing provider scope tests remain part
  of the full verification suite. Application callbacks remain trusted code.
- Resource behavior follows existing materialized execution. Each synchronous
  expression is followed by a deadline check; a callback cannot be interrupted.
  There is no new cancellation API or streaming provider contract in this release.
- `derived-columns.test.ts` provides a real SQLite fixture and bounded query
  generator with direct reference evaluation. It covers native-only work, shared
  parsing, invalid unused JSON, views, wildcard output, grouping/HAVING, windows,
  CTEs, set operations, self/outer joins, nulls, output validation and deadlines.
  Separate real SQL tests cover derived-key lookups and enriched descriptions for
  all three first-party SQL providers.
- Deferred slices are recorded in `docs/exec-plans/tech-debt-tracker.md`: cardinality
  proofs, related collections, expansion, incremental fetching and async callbacks.

### Audit outcome

No remaining security finding was confirmed in the changed derived-computation
path after the regression fixes. Public SQL remains untrusted and schema
membership is checked before provider dispatch. Schema callbacks and provider
scope implementations remain trusted. Tests cover private field containment and
real scoped reads; they do not prove arbitrary callback resource usage or
production database concurrency. Pre-existing provider behavior gaps are recorded
separately in the debt tracker.

### Final checks

- `vp lint`: passed.
- `vp run -r typecheck`: passed across all 18 workspaces.
- `pnpm -r exec tsgo --noEmit -p tsconfig.json`: passed without task-cache reuse.
- `vp test`: 88 test files, 3,340 tests passed.
- `vp fmt` and `git diff --check`: passed.

Implementation remains in the shared checkout. No worktree, commit, or publication
was created by this task.
