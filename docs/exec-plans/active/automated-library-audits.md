# Automated library audit plan

Status: proposed. No scheduled job or automation is created by this plan.

## Purpose and starting point

Make semantic regressions reproducible before release, and periodically inspect contracts that automated tests do not yet express. Start with cases from the [immediate audit](../completed/library-contract-audit.md).

The repository already has SQLite parity helpers in `packages/test-support/src/runtime.ts`, runtime compliance cases, provider conformance tests, and CI lint/typecheck/test jobs. Extend those foundations. Internal cross-package support belongs in `@tupl/test-support`; reusable public adapter conformance belongs in `@tupl/provider-kit/testing`. Preserve the documented package layering.

The completed [9 September audit](../../audits/2026-09-09-contract-audit/README.md) supplies replay cases and execution evidence. Its remediation queue and automation handoff are the initial backlog.

## 1. Establish a reusable contract suite

Give each case a stable identifier and record:

- Contract and operator combination exercised.
- Schema, fixture rows, query or relational plan, and explicit expected behavior where available.
- Applicable providers/dialects and authoring forms.
- Requested execution routes and evidence that each route actually ran.
- Ordering and value comparison rules from the immediate audit.

Adapt real provider fixtures for reuse. Add test-only capability restrictions to vary execution boundaries while retaining real compilation and database execution. Cover local operators above scans, maximum supported remote execution, and selected mixed boundaries. Assert the physical plan and calls so fallback cannot masquerade as coverage.

First migrate a small set of recent scope/order regressions and the immediate audit's confirmed cases. Retain focused owner-package tests; share setup and contract cases without moving all verification into one large test file.

## 2. Generate valid compositions and reduce failures

Generate bounded schemas, data, and well-formed relational plans. Track column scope, types, nullability, and operation outputs while constructing a plan. Begin with projection/filter/sort/pagination/aggregation, then add joins, views, CTEs, and derivations in separate increments. Invalid-input generation is a separate suite with explicit rejection expectations.

Use three complementary checks:

| Check                               | What it establishes                                                                            | Limitation                                                          |
| ----------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Placement equivalence               | Supported local, remote, and mixed routes agree on one query.                                  | Shared planner bugs can affect every route.                         |
| Independent database comparison     | Direct SQL execution agrees with tupl for the documented shared SQL subset.                    | Dialect semantics and public value conversions need explicit rules. |
| Preconditions-based transformations | Capture-free renaming, legal conjunction splitting, and equivalent wrappers preserve behavior. | A transformation is an oracle only when its preconditions hold.     |

Use hand-calculated expectations for tupl-specific derivation and coercion behavior. Direct database reference queries must bypass tupl's lowering and provider SQL builders. Do not normalize away null/type/duplicate mismatches. Queries with pagination use deterministic total ordering when equality of selected rows is required.

For each failure, store commit, dependency versions, generator version, seed, schema, data, input forms, actual plans, provider SQL/bindings, and results/errors. Reduce data, columns, expressions, and operators while preserving validity and the failing execution route. Commit the minimal case to a deterministic replay corpus after triage.

Verify the checker itself with a deliberately incorrect comparison target, such as moving a limit across a filter. Prove it detects and reduces that discrepancy before trusting large green generated runs.

## 3. Add lifecycle, resource, and consumer checks

Add deterministic contract tests for concurrent contexts, callback sharing boundaries, failed-session reuse/observation, cancellation where supported, and cleanup after provider failure. Control asynchronous completion with explicit barriers instead of sleeps. Inject faults at provider boundaries while preserving real execution for semantic tests.

Measure intermediate row growth and provider calls, not just output size. Prefer structural bounds over timing assertions. Keep performance benchmarks separate, with tracked baselines and generous regression thresholds. Report CPU, provider-side allocations, or examined-row work that current APIs cannot bound; an output row limit is not proof of bounded execution.

Build library packages with their package-owned build commands, pack tarballs, and install them in a clean temporary consumer outside workspace resolution. Check supported entry points, runtime imports, declarations, representative documented APIs, and the declared Node/TypeScript support range. Include a provider-backed query and derived column type inference. Do not publish packages as part of this test.

Add boundary cases for parameter binding, unusual identifiers, private field access, malformed public inputs, and scoped provider access. Keep security assertions tied to an actual public trust boundary.

## Proposed execution schedule

| Trigger            | Work                                                                                                                                          | Policy                                                                                                                                            |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Every pull request | Existing verification, deterministic replay corpus, a small fixed generated seed set, type contracts, and applicable provider contract cases. | Block merging on reproducible regressions. Start with an additional runtime target of about two minutes and tune from measurements.               |
| Nightly CI         | Larger generated runs with recorded rotating seeds, mixed execution boundaries, and lifecycle/failure cases.                                  | Bound each job initially to 15 minutes. Upload reproducers and coverage; timeout means incomplete coverage, not success.                          |
| Weekly CI          | Broader supported database/version combinations, clean packed consumers, and deeper resource cases.                                           | Start with SQLite; add real server databases according to advertised provider support. Keep environment failures distinct from semantic failures. |
| Before release     | Replay all confirmed cases, run the full supported consumer/provider matrix, and review unresolved findings affecting release contracts.      | Produce explicit pass/fail/unsupported/unresolved results. Never silently waive a known wrong-result case.                                        |
| Weekly agent audit | Review one rotating contract area against current source, tests, and the findings ledger.                                                     | Read and reproduce only; report new actionable findings or failed verification. Stay quiet when nothing actionable changes.                       |

These are proposed cadences and initial budgets. Choose exact scheduling times when enabling the jobs. Use disposable fixtures and databases; production data and credentials are unnecessary.

## Recurring agent audit instructions

The scheduled review should follow a durable repository checklist so it can evolve through review. Its task prompt should say:

> Audit one contract area in tupl, rotating through rewrite semantics, provider capability and scoping, value/type contracts, lifecycle and resource limits, and published consumer behavior. Read the planner invariants, known debt, previous audit coverage, and changes since that area was last reviewed. Review the whole selected contract, including unchanged callers. Run relevant existing checks and attempt a minimal reproduction for each suspected defect. Report only new reproduced findings, regressions, or verification failures, with expected/actual behavior, affected paths, and evidence. Distinguish known findings and unresolved design questions. Do not fix code, commit, publish, or send messages externally. Remain quiet when no actionable change is found. Record coverage and the next rotation area in the audit record.

When activated, use a thread heartbeat for recurring conversational follow-up unless standalone project runs are explicitly requested. Schedule only after the user requests activation. Repository CI schedules are a separate implementation step.

## Failure handling and coverage

Deduplicate findings by violated contract and minimized reproducer, not just stack trace. Each finding records status, owner, regression case, and resolution. Keep known failing cases visible with linked tracking; do not hide them behind broad skips or automatically accept new snapshots. Repeated findings should lead to a new enforceable check.

Publish coverage by operator combination, authoring form, provider, execution route, and edge dataset. Counts of generated queries alone are not useful evidence. Record unsupported combinations and cells never reached. Track shrink success, reproduction reliability, and job duration to keep the suite maintainable.

## Implementation milestones and acceptance

1. **Replay and route evidence.** Reuse real provider setup, register the initial contract cases, and prove that each requested route ran. Deliver a fast suite without adding generation yet.
2. **Bounded generation.** Add deterministic valid-plan generation, independent comparison, and reduction for the initial operator subset. A seeded deliberate defect must produce a stable minimal repro.
3. **CI operation.** Add PR and nightly jobs, bounded runtime, retained artifacts, and readable coverage. Demonstrate both a semantic failure and an infrastructure failure are classified correctly.
4. **Broader contracts.** Add lifecycle/isolation cases, packed consumer checks, and supported database combinations. Preserve a fast PR tier.
5. **Periodic review.** Check in the rotating checklist and findings ledger; activate the requested schedule. Demonstrate that an unchanged known issue does not generate repeated notifications.

The first useful stopping point is milestone 1. Complete it immediately after the manual audit's highest-priority fixes, then add generation incrementally. Do not delay concrete regression protection until a general-purpose generator is finished.

## Progress

- [ ] Contract cases and real route evidence.
- [ ] Valid-plan generator and reducer.
- [ ] PR/nightly CI and failure artifacts.
- [ ] Lifecycle, package consumer, and supported database checks.
- [ ] Reviewed recurring checklist and explicitly activated schedule.
