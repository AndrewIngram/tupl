# Runtime correctness and architecture cleanup

Status: completed on September 8, 2026. Implementation is verified.

## Completion evidence

- All six stages are implemented. The five audit findings are addressed.
- `vp lint`: passed, zero errors or warnings.
- `vp run -r typecheck`: passed all 18 workspace tasks. Because these were cache
  hits, `pnpm -r exec tsgo --noEmit -p tsconfig.json` was also run and passed fresh.
- `vp test`: 556 tests passed across 78 files.
- Playground: fresh typecheck, production build, and 133 tests across 21 files passed.
- `vp fmt` and `git diff --check`: passed.
- Browser verification covered provider-backed orders, a PostgreSQL/Redis lookup
  join, and local Fibonacci recursion. The graph displayed actual routes and
  measurements; the explanatory CTE grouping had no fabricated timing. A query
  exceeding the recursive iteration limit retained completed child observations
  and displayed the failed root's error and measured duration.
- Independent correctness/security review found missing planner/lookup limit
  validation. This is fixed, with 66 invalid boundary cases rejected before provider
  calls. No confirmed review findings remain.
- Provider (30), explain (4), and public Result API (13) cases were preserved,
  including mappings and views. Twelve internal forwarding modules were removed.

Remaining limits are deliberate: providers still return arrays, so runtime cannot
prevent provider-internal allocation; timeouts do not cancel provider work or
preempt synchronous JavaScript; unread event metadata has no quota. These limits
are documented in the runtime README. Unrelated debt remains open.

## Objective

Address the five findings from the September 8, 2026 architecture audit. Keep the
six-package layering and canonical relational model. Fix execution correctness
before changing session reporting or migrating test infrastructure.

The work is complete when sessions preserve results and failures, row limits apply
to intermediate materialization, session events describe observed execution, tests
use production contracts, and architecture checks tolerate ordinary filesystem
contents while detecting prohibited imports and forwarding modules.

## Evidence and baseline

- [Local sessions](../../../packages/runtime/src/runtime/session/rel-execution-session.ts)
  and [provider sessions](../../../packages/runtime/src/runtime/provider/provider-fragment-session.ts)
  set `executed` before awaiting execution and return empty rows on subsequent
  calls without a stored result. Temporary local-session tests reproduced both
  concurrent empty results and success after failure.
- [Local execution](../../../packages/runtime/src/runtime/execution/local-execution.ts)
  checks `maxExecutionRows` after the root finishes. A temporary test materialized
  two rows, limited the result to one, and passed a row limit of one.
- Local sessions divide total duration across planned steps and derive routes from
  step kinds. `maxConcurrency` is declared but has no runtime consumer.
- [Runtime test support](../../../packages/test-support/src/runtime.ts) changes
  Result-returning methods into throwing methods and casts weakened providers into
  production contracts.
- [Boundary tests](../../../test/__tests__/package-boundaries.test.ts) crash on
  `packages/.DS_Store`. Their forwarding-module detector recognizes only a single
  line of re-exports.

Audit baseline: 515 tests passed and two boundary tests failed on `.DS_Store`.
Lint and formatting checks passed. All 18 workspace typecheck tasks reported
cached passes. Temporary reproduction tests were removed after the audit.

## Design decisions

### Session ownership

Each session owns one execution promise. The first execution request starts it.
Every later request observes the same terminal result or failure. Store terminal
success and failure explicitly instead of combining an `executed` flag with
nullable rows. Expected failures remain Results internally. Preserve the existing
rejecting session-method contract unless a separate API change is proposed.

Share this lifecycle between local and provider-fragment sessions only where it
removes duplicated state transitions. Each executor still owns its execution work.
Serialize event consumption separately from execution so concurrent `next()` calls
cannot duplicate or skip events.

### Row limit semantics

`maxExecutionRows` caps the cardinality of each materialized row set, including
intermediate results. It is not a cumulative count of rows visited or a byte-based
memory quota. A shrinking parent operator does not excuse an oversized child.

Runtime owns the limit and error construction. Local producers check before growing
an output beyond the limit. Provider results are checked immediately after they
return, before mapping or further execution. Lookup batches must also respect the
limit on their combined materialized result.

The array-returning provider contract cannot prevent allocation inside a backend
or provider. Document this limit explicitly. Provider streaming, cancellation, and
byte-based memory limits are separate work.

### Session reporting

Keep static plans for explanation. Collect execution events where work actually
runs. Planned nodes do not acquire measured timings merely because they exist in
the plan. Omit unavailable measurements and record the route actually taken.

| Approach                                           | Decision                                                             |
| -------------------------------------------------- | -------------------------------------------------------------------- |
| Report only one measured whole-query event         | Simpler, but loses useful operation detail needed by the playground. |
| Observe the existing executor                      | Recommended. Measures actual work without introducing a scheduler.   |
| Execute the static step graph with a new scheduler | Defer. Adds scheduling semantics beyond the audit fixes.             |

Remove `maxConcurrency` and its callers because the executor does not implement
that contract. Migrate affected session contracts and consumers together. Do not
retain ignored options or synthetic measurements as compatibility behavior.

### Resolved event contract

- The executor owns observation. Observe each actual relational-node invocation,
  including remote execution at that node, and record the chosen route where it
  is selected. Parent durations include awaited children and must not be summed
  to obtain total query duration.
- `next()` starts execution once and returns the oldest unconsumed completion or
  failure event as soon as one is available. Concurrent calls consume in request
  order. `runToCompletion()` awaits the same execution without consuming events.
  It neither waits for an event consumer nor calls that consumer's callback.
- Events use actual completion order. Remove `eventOrder` together with
  `maxConcurrency`. `executionIndex` is the completion sequence number, not the
  static plan position.
- Add relational-node identity to measured plan steps and events. Each event also
  has a per-node occurrence number so repeated CTE evaluation is distinguishable.
  Keep the static step ID when a corresponding plan step exists. Operations hidden
  by a planned remote fragment can still produce events if actual execution falls
  back locally. Give them an execution ID and relational identity without inventing
  a matching static step or changing the plan after execution starts.
- `getStepState()` exposes the latest observed occurrence for that step. A plan-only
  grouping node has no fabricated completion. A planned node that never executes
  has no execution measurement. Public state and event types should distinguish
  measured completion, failure, and unmeasured planning state.
- Successful child events and failed operation events remain available after a query
  fails. Once the queue is drained, `next()` rejects with the retained query error.
  `runToCompletion()` always rejects with that same query error on failure.
- `onEvent` runs once when `next()` consumes an event, after advancing the cursor.
  If the callback throws, only that `next()` call rejects. Execution and later event
  delivery continue, the event is not replayed, and the successful query outcome
  remains successful. The callback is synchronous and is not awaited.
- Buffer unread event metadata until consumed, then release queue references.
  Keep only latest states in the state map. There is no new backpressure or event
  quota in this change. Metadata retention scales with unconsumed actual invocations.
  Preserve full row capture for the final output only, and omit unavailable
  intermediate snapshots. This avoids retaining every intermediate row set during
  recursive execution. Document this scope for `captureRows`.
- A timeout closes observation with the session failure. Ignore late notifications
  from uncancelled provider work, so terminal failure cannot become success.

Scenario checks must cover a first event arriving before a deliberately blocked
later provider operation finishes, multiple concurrent consumers, callback throws,
repeated CTE execution, async provider support differing from static planning,
failure after successful children, and late completion after a timeout.

## Delivery sequence

### Model assignments

These assignments reflect the remaining design ambiguity and correctness risk.
They are implementation recommendations, not measured model comparisons. Model
assignments do not relax the acceptance criteria or verification requirements.

| Stage                               | Model          | Reasoning effort | Responsibility                                                            |
| ----------------------------------- | -------------- | ---------------- | ------------------------------------------------------------------------- |
| 1                                   | `gpt-5.6-luna` | medium           | Implement the isolated package-discovery repair.                          |
| 2                                   | `gpt-5.6-sol`  | high             | Implement session lifecycle and concurrency correctness.                  |
| 3                                   | `gpt-5.6-sol`  | high             | Enforce materialization limits across execution paths.                    |
| 4, design                           | `gpt-6-astra`  | high             | Resolve the event contract before implementation.                         |
| 4, implementation                   | `gpt-5.6-sol`  | high             | Implement the settled event contract and migrate consumers.               |
| 5, first migration                  | `gpt-5.6-sol`  | high             | Establish and verify a representative fixture migration.                  |
| 5, repeated migrations              | `gpt-5.6-luna` | high             | Apply the reviewed pattern to bounded groups of callers.                  |
| 6                                   | `gpt-5.6-sol`  | high             | Implement syntax-aware checks and judge which structural rules to retain. |
| Correctness review after stages 2–3 | `gpt-6-astra`  | high             | Review the actual diff and regression evidence before broader cleanup.    |

For one uninterrupted implementation pass through stages 1–3, use
`gpt-5.6-sol` at high effort throughout. A separate Luna assignment for stage 1 is
optional. Neither max nor ultra effort is needed by default.

Keep Luna migrations within the pattern established in stage 5. Return unresolved
contract or ownership decisions to Sol instead of extending the pattern by guesswork.
These assignments describe execution roles and do not themselves start new tasks
or agents.

Execution note: stages 1–3 use the permitted contiguous Sol high pass. The attempt
to start Luna for repeated stage-5 fixture migrations hit the session's agent-thread
limit. The existing Sol high agent continues those bounded migrations instead.

### 1. Restore a usable verification baseline

- [x] Discover package directories using directory entries. Ignore files such as
      `.DS_Store` before attempting to access `src`. Handle directories without `src`
      explicitly and keep symlink handling consistent with the existing walker.
- [x] Test discovery against a temporary directory containing a package, an
      ordinary file, and an unrelated directory. Do not depend on a developer's
      actual `.DS_Store` or delete it as the fix.
- [x] Run the full test suite and record any failures beyond the known two.

Acceptance: boundary checks run on macOS filesystem contents and still inspect
every real package. This small repair lands before the runtime changes.

### 2. Make session execution single-flight and failure-stable

- [x] Add regression tests through the public session API for local execution and
      a real contract-conforming provider fixture.
- [x] Replace the flag-and-null state in both session implementations with one
      retained execution promise and explicit terminal outcomes.
- [x] Define event consumption separately from execution. Concurrent consumers
      receive each event at most once. A failed execution remains failed on every
      subsequent execution or terminal-result request.
- [x] Specify callback behavior and test it. A throwing observer must not change
      a successful query into a cached empty result or cause an event to be replayed.

Acceptance cases: concurrent `runToCompletion()` calls receive identical rows;
mixed `next()` and completion calls execute the provider once; repeated failures
preserve the original error; successful empty results remain distinguishable from
unstarted or failed execution; concurrent `next()` calls do not lose events.

### 3. Enforce intermediate row limits

- [x] Centralize materialization-limit checks in runtime, using the existing
      tagged guardrail error. Validate row-limit configuration at the query boundary.
- [x] Cover local values, scans, joins, aggregates, sorting, windows, set operations,
      CTEs, recursive accumulation, and remote fragments. Check producers before growth
      where runtime controls allocation, not only after constructing oversized arrays.
- [x] Check individual lookup results and their combined buffer. Stop further
      provider requests once a materialization limit fails.
- [x] Consolidate duplicate final-result checks after the producer checks are in
      place. Keep a final assertion only if it enforces a distinct boundary guarantee.

Acceptance cases: oversized input followed by `LIMIT 1` or `COUNT(*)` fails;
many-to-many join growth stops at the limit; remote root and nested fragments obey
the same rule; lookup accumulation and recursive CTE accumulation fail at the
limit; exactly-at-limit results succeed. Cover invalid limits and make the accepted
configuration range explicit in the public contract.

### 4. Replace synthetic session telemetry with observed events

- [x] Before implementation, use Astra high to specify event ordering, repeated
      CTE execution identity, callback failures, event buffering, and the interaction
      between `next()` and `runToCompletion()`. Record the chosen contracts and
      scenario expectations in this plan so Sol can implement without inventing
      lifecycle semantics during the refactor.
- [x] Add an internal execution observer to the existing execution context. Emit
      start, completion, and failure records from actual local and provider operations.
- [x] Use relational node identity to relate events to static plans. Repeated
      execution, such as recursive CTE iterations, needs an occurrence identifier.
      Static grouping nodes remain explanatory unless they correspond to actual work.
- [x] Collect actual duration, route, and available row counts. State whether
      duration includes child execution. Never divide query duration across nodes.
- [x] Let `next()` consume observed events while execution proceeds. Keep failure
      reporting and terminal-result behavior consistent with stage 2. `runToCompletion()`
      and event consumption must share execution rather than starting separate runs.
- [x] Remove unused concurrency configuration. Reconcile `eventOrder` with actual
      event delivery and update all callers in the same change.
- [x] Update the playground to tolerate unavailable measurements and consume the
      new records. Keep row capture opt-in and avoid retaining unnecessary copies.
      If display or interaction choices become material, present focused visual options
      before editing UI, as required by `AGENTS.md`.

Acceptance cases: controlled provider delays appear on the operation that incurred
them; runtime fallback reports its actual route; a failed child is identified;
provider-owned subtrees do not generate fictional local completions; recursive
execution can be distinguished from duplicate event delivery. Run the playground
and inspect its plan, measurements, and failure presentation.

### 5. Remove the alternate test API

- [x] Inventory callers of `augmentExecutableSchema`, `TestExecutableSchema`,
      `ProviderInput`, and the methods-to-provider conversion helpers.
- [x] Build fixtures with real `ProviderAdapter` contracts and canonical schema
      constructors. Keep useful dataset builders and SQLite parity infrastructure.
- [x] Migrate tests in coherent groups. Unwrap Results explicitly at the assertion
      boundary instead of replacing production methods. Preserve semantic cases and
      direct public Result API assertions throughout the migration.
- [x] Delete each obsolete helper and its casts with its final caller. Retain
      methods-based fixtures only where they test the supported table-method contract.
      Remove machinery whose only purpose is preserving historical tests.

Acceptance: fixtures compile against real contracts, executable schemas are never
monkey-patched, and parity coverage still exercises the same SQL cases. Test count
alone is not sufficient evidence that coverage survived the migration.

### 6. Make architecture checks enforce ownership

- [x] Parse imports and exports with the existing TypeScript tooling instead of
      using line-oriented regular expressions. Test multiline, multiple-export, and
      type-only cases, plus modules with real implementation.
- [x] Preserve declared package dependency rules. Cover side-effect imports,
      literal dynamic imports, re-exports, and relative imports crossing package
      boundaries so prohibited dependencies cannot bypass alias checks.
- [x] Resolve public subpath exceptions from package export declarations where
      practical. Avoid a second manually maintained list of package interfaces.
- [x] Review flagged internal forwarding modules by ownership. Delete forwarding
      layers that hide no behavior and update callers directly.
- [x] Remove historical filename checks and line budgets that only preserve a past
      refactor's shape. Retain checks that enforce a current documented constraint.

Acceptance: prohibited dependencies fail regardless of formatting or import path
style, valid implementations pass, and ordinary public export modules remain valid.

## Verification and completion

Keep each stage reviewable with its own regression evidence. Run the
repository checks for every non-trivial code change:

```sh
vp lint
vp run -r typecheck
vp test
vp fmt
```

After changing session contracts, also build the playground and verify real local
and provider-backed sessions. Inspect formatter changes before including them.

Before completion, update the runtime README and the durable relational-pipeline,
planner-invariant, and package-architecture docs where their contracts changed.
Update roadmap references to stepping and concurrency. Reconcile affected debt
entries without marking unrelated planner work complete. Move this plan to the
completed directory and update the active index in the same branch.

Out of scope: package reorganization, SQL feature expansion, a new scheduler,
provider streaming, and unrelated playground sandbox work.
