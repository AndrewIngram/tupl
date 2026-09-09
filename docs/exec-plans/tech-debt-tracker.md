# Tech Debt Tracker

These are the current architecture and process questions that remain intentionally open.

## Open items

### Explain purity vs rich provider descriptions

- Current state: `explain()` may compile provider-owned fragments to produce provider plan descriptions.
- Question: should `explain()` stay on that model, or should providers get a describe-only path that avoids compile-time side effects?

### Non-`ROWS` window frames

- Current state: `RANGE` and `GROUPS` frame modes are explicitly rejected.
- Question: implement correct semantics later or continue treating them as out of scope?

### Recursive CTE iteration guardrail

- Current state: recursive CTE execution stops after 256 iterations with a hardcoded runtime limit.
- Question: keep that fixed safety valve, or surface it as an explicit query/runtime guardrail alongside the existing row and timeout controls?

### Provider-local barrier traversal consistency

- Current state: some planner/runtime/provider-normalization switches intentionally treat local-only barrier nodes like `correlate` and `repeat_union` as unreachable in provider-owned paths and return early rather than traversing children.
- Question: keep that as an explicit invariant-only simplification, or normalize these switches to traverse children for consistency and future-proofing even when the current planner should never route them there?

### Lookup-join planning and runtime eligibility

- Resolved in derived-column execution: physical explain, session plans, and runtime use the same eligibility helper. A right input must be a bare scan with no limit or offset, and its provider must implement `lookupMany`. Private providers resolve from attached entity handles. Left keys may be computed locally.
- Runtime session events remain the evidence for executed batches; static provider SQL descriptions do not include data-dependent lookup keys.

### Planner subquery callback Result bridge

- Current state: structured/simple SELECT lowering now returns typed `Result` values for direct validation failures, but nested subquery lowering still crosses an older callback seam that expects `RelNode | null`, so the structured-select bridge temporarily rethrows `RelLoweringError` across that seam and immediately re-captures it.
- Question: should expression/subquery lowering gain a fully Result-typed callback path so planner lowering can remove that last throw-based bridge entirely?

### Mechanical plan-coverage enforcement

- Current state: substantial work is expected to have a checked-in execution plan, but enforcement is social/documented rather than diff-based.
- Question: add a low-noise repo check later, or keep it as documented workflow only?

### Derived-column follow-ups

- Initial release supports synchronous, cardinality-preserving computations with
  demand pruning and conservative filter/sort/limit motion.
- Cardinality-aware join elimination needs trustworthy uniqueness and existence
  proofs. Do not remove multiplying joins just because their outputs are unused.
- Collection dependencies need explicit keys, selected fields, batching, ordering,
  and missing-row semantics. Evaluate existing joins/grouping before adding an API.
- Row expansion needs a separate operation with zero/many-row semantics and bounds.
- Incremental fetching beneath derived filters needs stable cursors/order, examined
  row limits, cancellation, and scope preservation; an OFFSET loop is insufficient.
- Async computation is deferred until a use case justifies concurrency, shared
  in-flight evaluation, and cancellation semantics.

### Integer division contract

- Current state: ordinary tupl division produces floating-point results, including
  `9 / 2 = 4.5`, across tested local and SQL routes. SQLite literal integer
  division produces `4`. Null propagation and zero divisors are now consistent.
- Question: retain floating-point division as the portable contract, or introduce
  explicit integer-sensitive semantics? This was outside the nine audit fixes.

The embedded scan modifier, grouped navigation validation, and public source
coercion defects were resolved in the [contract audit remediation](completed/contract-audit-remediation.md).
