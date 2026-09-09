# Review of contract audit fixes

Reviewed commit `3c522b4` against `841528b7636cce40d0d0254fd6ad8330e0ee62ba`.
Three actionable findings were confirmed. No production files were changed during the review.

Follow-up: the two SQL name-resolution findings are fixed in this release.
The mixed ESM/CommonJS identity finding is also fixed by shared CommonJS implementations with ESM facades. Packed checks now cover mixed-format runtime and declaration identity. The regression suite now
covers qualified grouped windows, value/default/partition/order references,
compound source and output aliases, CTE renaming, and later-branch wildcards.

## Standards

### P1: ESM and CommonJS consumers do not share schema identity

The import and require conditions in
[the schema-model manifest](../../../packages/schema-model/package.json#L24)
load independent implementations. Their builder and computation registries are
separate WeakMaps. An ESM application cannot consume a builder created by a
CommonJS dependency, even when both resolve the same installed package version.

Verified using actual package-name imports. A builder from ESM schema-model passes
ESM `isSchemaBuilder` and `createExecutableSchema`. A builder from
`createRequire(import.meta.url)("@tupl/schema-model")` fails recognition and returns
`TuplSchemaNormalizationError`, saying physical tables must be declared through
the builder. Existing packed tests exercise each format independently.

Publish one runtime implementation shared by both entry points, for example a
CommonJS implementation with an ESM facade. Add packed tests that exchange
builders and derived definitions between module formats.

## Spec

### P1: Grouped windows use a metric instead of the grouped source

F2 requires collision-free identities used by grouping, HAVING, windows, sorting,
and final projection. The new alias-first lookup in
[select-shape.ts](../../../packages/planner/src/select/select-shape.ts#L307)
violates that requirement.

```sql
SELECT category AS label, SUM(n) AS category,
       LAG(category) OVER (ORDER BY category) AS prev
FROM records
GROUP BY category
ORDER BY label
```

For the audit fixture's labels `[null, 'a', 'b', 'c']`, SQLite returns previous
values `[null, null, 'a', 'b']`. Tupl returns `[null, 4, -6, null]`. Both the window
ordering and its value bind to the metric alias. ROW_NUMBER ordering is also
wrong. This reproduces on all three adapters with native, scan-only, and
no-aggregate routes.

Resolve actual grouped source references before conflicting SELECT aliases,
retaining qualifiers. Test navigation values, defaults, partitions, and ordering
with colliding source and metric names.

### P1: Compound ordering drops table qualifiers

F3 requires ordering around the complete compound result. The resolver in
[set-op-lowering.ts](../../../packages/planner/src/select/set-op-lowering.ts#L15)
uses only the column name, so a qualified source can become a different public
output alias.

```sql
SELECT n AS x, id AS n FROM records
UNION ALL
SELECT n, id FROM records
ORDER BY records.n
LIMIT 3
```

SQLite returns `[{x:null,n:4},{x:null,n:4},{x:-6,n:5}]`. Tupl returns
`[{x:9,n:1},{x:9,n:1},{x:2,n:2}]`, ordered by ID. This reproduces on all three
adapters with native, scan-only, and no-aggregate routes.

The same resolver also rejects valid source names hidden by output aliases, such
as `SELECT n AS x ... UNION ALL SELECT id AS y ... ORDER BY n`, and names exposed
by a later branch's wildcard. Resolve ordering against expanded SELECT expressions
from each branch, preserving qualification, then map the matching expression to
its output position. Test qualified references, source names with aliases, and
later-branch wildcards.

## Verification and limits

Targeted probes used the committed audit fixture and real SQLite connections.
The compound probes completed 72 comparisons, including controls for derived
columns, coercion, and an aliased embedded scan. Three compound cases failed in
all nine adapter/route combinations. Grouped-window probes independently covered
the same nine combinations. Mixed-format imports used built package exports.

The previously passing 3,458-test suite did not cover these combinations. No
production edits were made, so it was not rerun as a substitute for the targeted
probes. General HAVING SELECT-alias support also fails without alias collisions;
it is excluded from this change review as a pre-existing gap. Existing unsupported
negative LIMIT and NULLS LAST syntax are also excluded.

Standards: one finding, mixed-format identity. Spec: two findings, both wrong-result
errors in name resolution. All three are P1.
