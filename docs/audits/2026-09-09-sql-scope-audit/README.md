# SQL correctness and scope audit, 9 September 2026

Audited `2b961e5`. Two P1 correctness defects are confirmed. Both can silently
return incorrect results. No cross-tenant data escape or hidden-value disclosure
was demonstrated. These are separate assessments: containment does not make
incorrect query results acceptable.

**Follow-up:** expanded property tests subsequently found a confirmed Drizzle
identifier-injection scope escape. The two original findings, a related aggregate
ORDER BY defect, and that security defect are now fixed. See [remediation](remediation.md).
The assessment and result files below preserve the original audit observations.

## P1: HAVING can filter on fabricated null values

Using the existing contract-audit fixture:

```sql
SELECT category AS label, SUM(n) AS total
FROM records
GROUP BY category
HAVING label = 'b'
ORDER BY label
```

Expected `[{label:'b',total:4}]`; actual `[]` on Drizzle, Kysely, and Objection,
with native, scan-only, and no-aggregate routes. A nonexistent reference such as
`HAVING nope IS NULL` also succeeds and returns all groups instead of failing.

The scoped fixture additionally accepts both `HAVING secret IS NULL` and
`HAVING accounts.secret IS NULL` after grouping by a public column. Those queries
return all authorized groups on SQLite and PostgreSQL, despite `secret` being
undeclared. The private values are not read; the unresolved reference becomes null.

Cause: [having-lowering.ts](../../../packages/planner/src/having-lowering.ts#L43)
accepts a reference without verifying its availability. The mapping in
[select-shape.ts](../../../packages/planner/src/select/select-shape.ts#L303)
falls back to an unbound name instead of resolving a group alias or rejecting it.

Impact: valid filters omit data; invalid filters can include every group. Consumers
can receive incorrect reports without any error. This defect predates the latest
package changes and was previously observed as a HAVING alias gap.

Remediation: bind HAVING against the available grouping and metric outputs,
resolve supported SELECT aliases with explicit precedence, preserve qualifiers,
and reject unresolved or unavailable references before provider execution.
Regression tests must cover aliases, collisions, unknown/private names, qualified
references, null predicates, and native/local execution on both SQL dialects.

## P1: CTE and derived-table references are not checked against their outputs

Using the containment fixture:

```sql
WITH c AS (SELECT id FROM accounts)
SELECT secret FROM c
```

Expected a planning error: `c` exposes only `id`. Actual is three rows of
`{secret:null}`, with a provider query executed. The same problem affects derived
tables, recursive CTEs, predicates, grouping, aggregates, windows, and references
inside EXISTS. For example:

```sql
WITH c AS (SELECT id FROM accounts)
SELECT COUNT(secret) AS n FROM c
```

This succeeds with zero, fabricating an aggregate over an unavailable column.
The missing-column variants reproduce on all three adapters using native and
scan-only routes, on SQLite and PostgreSQL.

Cause: [rel-schema-validation.ts](../../../packages/schema-model/src/rel-schema-validation.ts#L96)
accepts CTE references without checking the CTE's declared output. Higher
operators traverse their inputs but do not establish that every reference resolves
against that input. A missing field is then read as null during local execution.

Impact: invalid queries silently produce plausible but false data, including counts
and filter results. The intended public-field rejection boundary is incomplete.
Changing hidden rows and values did not affect the results, so this is not evidence
of private-value disclosure.

Remediation: carry lexical CTE output schemas through validation, validate every
reference against its input relation, and reject missing columns before pruning or
execution. Include derived tables, recursive/self references, CTE shadowing,
projections, predicates, joins, aggregates, ordering, windows, and EXISTS in tests.

## Containment assessment

No authorization bypass or private-field disclosure was confirmed. The trusted
boundary remains application-authored schemas, provider scope callbacks, derived
callbacks, and caller context. SQL text is treated as untrusted.

- All 2,640 existing containment tests pass across SQLite/PostgreSQL and the three
  SQL adapters, including the property-based hidden-data mutation checks.
- Additional scope probes cover aliases, CTE shadowing, all set operators already
  in the corpus, qualified ordering, recursive CTEs, outer joins, and private names.
- 114 targeted coercion/private-derived-dependency cases verify rejection before
  SQL or callbacks, no work for unused derivations, and denied scopes.
- 24 asynchronous Drizzle/Kysely scope cases cover native queries, local execution,
  derived queries, and lookups. Async rejection fails closed.

This is a bounded audit, not a proof that every query shape is contained. Some
nested correlated CTE and constant-join shapes reject as unsupported; these are
not wrong-result findings. Existing unsupported SQL syntax remains out of scope.

## Reproduction and evidence

Run `node docs/audits/2026-09-09-sql-scope-audit/run.mjs` from the workspace.
It runs the two scoped batches and refreshes their result files. The 276-case first
batch contains controls and unsupported shapes; its failures must be classified
using the explanations above. The 108-case second batch targets missing-column
and grouped HAVING rejection and reproduces the validation defects in every case.

[Scope results](scope-results.json) and [missing-column results](missing-columns-results.json)
include expected/actual values and provider-call counts. Each scoped request runs
with two different sets of hidden rows and values. The original contract-audit
fixture supplies the separate HAVING alias reproductions.

No production code changed during this audit. Standards/security review found no
confirmed authorization defect. Spec/correctness review found the two P1 issues
above. Audit artifacts are uncommitted.
