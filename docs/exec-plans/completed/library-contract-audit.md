# Immediate library contract audit

Status: completed on 9 September 2026. See the [audit report](../../audits/2026-09-09-contract-audit/README.md) for six new findings, three reproduced known defects, and explicit coverage limits. No production fixes were made.

## Purpose

Audit the library's observable contracts across authoring forms, planner rewrites, providers, and local execution. Recent reviews found defects around operation order and column scope. This audit starts from contracts across the library, rather than the latest diff.

The companion [automation plan](../active/automated-library-audits.md) turns the resulting cases into repeatable checks.

## Baseline and scope

Record the exact commit, working tree state, dependency versions, and test commands in the audit report. Start from the current checkout without creating a worktree. Review these sources before selecting cases:

- [Planner invariants](../../design-docs/planner-invariants.md)
- [Relational pipeline](../../design-docs/relational-pipeline.md)
- [Package architecture](../../package-architecture.md)
- [Known technical debt](../tech-debt-tracker.md)
- Existing runtime compliance tests and provider conformance tests

Run the repository's required lint, workspace typecheck, tests, and formatting checks. Separate baseline failures from audit findings. A passing baseline is evidence about existing coverage, not completion of this audit.

First reproduce the recorded embedded scan modifier and public source coercion defects. Label these as known defects, then trace adjacent paths. Also check the recorded navigation window validation and provider barrier traversal gaps. Treat unsupported window frames, async derivation, collection dependencies, and row expansion as explicit scope boundaries; test their rejection where exposed, without implementing them.

## Agree what equivalence means

Write the comparison rules into the report before interpreting differences:

- Preserve column names, public value types, nullability, and duplicate row counts. Do not stringify values to make mismatches disappear.
- Compare unordered results as multisets. For ordered results, compare order while allowing permutations within genuinely equal sort keys. Use a unique final key for pagination comparisons; ties at a page boundary can otherwise permit different valid rows.
- Distinguish database transport conversion from public column coercion. Any permitted normalization must name its contract and run symmetrically.
- Check tagged errors and their public context. Do not require identical backend error prose.
- Derived callbacks are pure and synchronous. Unused results may be pruned. Test documented sharing and demand guarantees, without requiring an incidental evaluation order.
- The local runtime is a reference implementation, not an unquestionable oracle. Use direct database execution and hand-calculated expectations to arbitrate discrepancies.

If an observable behavior has no defined contract, record a design question rather than silently choosing whichever implementation currently wins.

## Ordered audit passes

| Pass                              | Questions and concrete probes                                                                                                                                                                                                                               | Required evidence                                                                                                                                                                                       |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Execution placement            | Does the same query return equivalent results locally, through each applicable first-party provider, and across mixed boundaries? Start with projection, filter, sort, pagination, aggregation, joins, and demanded derivations.                            | Results plus actual physical plans and provider calls. A requested remote route that silently falls back does not count as remote coverage.                                                             |
| 2. Rewrite correctness            | For each rewrite, what proves that names still bind to the same values and that row count, ordering, and null extension survive? Exercise both orders of adjacent operations, then selected three-operation chains.                                         | A rewrite inventory naming preconditions, owning code, positive cases, and counterexamples where the rewrite must decline.                                                                              |
| 3. Authoring and value contracts  | Do SQL, table/view definitions, and equivalent direct relational plans agree? Check scan modifiers versus wrapper nodes, nested views, CTE aliases, private dependencies, source/derived coercion, JSON shapes, and sync-only types.                        | Equivalent input forms, public rows, and positive/negative compile-time assertions. Intentional unsupported forms must fail explicitly.                                                                 |
| 4. Provider contracts             | Does acceptance imply successful compilation of a supported fragment and correct execution on valid data? Does declining preserve semantics through fallback? Do scoped providers preserve tenant constraints through scans, lookups, aggregates, and CTEs? | Real provider/database tests, capability decisions, generated SQL/bindings, and context-specific expected rows. Operational failures remain valid failures, never empty success.                        |
| 5. Lifecycle and work limits      | Can a tiny result require uncontrolled intermediate work? Check joins, aggregate inputs, recursion, cancellation where supported, provider rejection, cleanup, and repeated session observation.                                                            | Intermediate row counts, provider calls, callback counts where guaranteed, terminal errors, and resource cleanup. State explicitly where provider allocation or CPU remains outside current guardrails. |
| 6. Reuse and isolation            | Can one prepared schema serve concurrent contexts without sharing tenant data, callback caches, cancellation, or failed outcomes? Can caller mutation corrupt later executions?                                                                             | Overlapping executions with controlled completion order, different contexts, one failed execution, and subsequent successful reuse.                                                                     |
| 7. Consumer and public boundaries | Do packed packages work without workspace aliases? Are exports, declarations, documented examples, and downward dependency rules valid? Are values bound safely and private fields inaccessible through SQL/relational inputs?                              | A clean temporary consumer using built tarballs, runtime and type checks, and adversarial identifier/value cases on real providers.                                                                     |

## Initial case matrix

Keep the first pass finite and reviewable. Inventory supported routes, then cover each relevant operator pair below with at least one adversarial dataset and each applicable provider. Record unsupported cells with a reason.

- Projection with filter/sort, including swapped names, repeated source columns, computed outputs, qualified references, and aliases shadowing source names.
- Pagination with filter/sort/aggregate/window in both orders. Compare embedded scan modifiers with explicit nodes.
- Aggregation with projection/HAVING/sort, including an empty input, no grouping keys, null values, duplicate values, and renamed metrics.
- Joins with filter/projection/derivation, including outer joins, missing matches, duplicate keys, null keys, and unused projected values whose joins still affect cardinality.
- DISTINCT and set operations with projection/pagination/derivation, checking multiplicity and enclosing aliases.
- CTEs, nested views, and subqueries wrapping the preceding cases. Check EXISTS demand pruning, correlation, and recursive boundaries separately.
- Derivation chains with shared inputs, unused outputs, private dependencies, coercion, and demanded failures. Projection-only derivation should retain legal remote filtering/sorting/pagination; an unselected derived column should introduce no local computation.

Use empty, singleton, duplicate-heavy, nullable, and alias-collision fixtures. Include negative/zero numeric values and supported numeric boundary cases. Add three-operation cases around every suspicious pair, especially projection plus filter/sort plus pagination. Do not attempt an unbounded Cartesian product.

Use existing test support and real SQL provider fixtures. For execution placement, restrict capabilities through test-only adapters that delegate accepted work to real providers. Confirm that entity bindings still refer to the intended adapter. Never make an unsupported fragment appear supported merely to force a route.

## Findings and completion

For each finding, preserve the smallest reproducer, expected and actual behavior, affected paths, actual execution route, and user impact. Distinguish new defects, known defects, missing contract decisions, and test infrastructure defects. Prioritize silent wrong results and cross-context disclosure, then crashes and unbounded work, then capability/performance regressions.

The audit is complete when:

1. Every pass has evidence and every selected matrix cell is marked passed, reproduced defect, explicitly unsupported, or unresolved with a concrete reason.
2. Each rewrite inspected has stated semantic preconditions and at least one case that must block it.
3. Confirmed defects have minimal regression cases or standalone reproductions; reports do not rely on speculative code inspection alone.
4. A report lists findings first, coverage limits second, and a prioritized remediation queue. Unresolved cells prevent an unqualified clean assessment.
5. Reusable cases are identified for the automation plan. Audit completion does not imply all findings have been fixed.

Execute the passes in order. Findings may justify a bounded adjacent search, but record added scope rather than repeatedly restarting a whole-library audit after every fix. After remediation, rerun affected cases and required repository verification, then conduct one review of the changed contracts.

## Progress

- [x] Record baseline and comparison rules.
- [x] Reproduce known defects and inventory routes/rewrites.
- [x] Complete semantic passes 1–3.
- [x] Complete provider, lifecycle, isolation, and consumer passes 4–7.
- [x] Write findings, coverage gaps, and remediation queue.
- [x] Transfer reusable cases to the automation backlog.
