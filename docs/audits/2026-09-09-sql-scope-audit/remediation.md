# Column-scope remediation and generated findings

The two original correctness defects are fixed. Expanded fast-check coverage
also found a related ORDER BY defect and a critical Drizzle SQL-injection defect.
All four are fixed in this release. The earlier bounded audit's statement
that no scope escape was confirmed is superseded by this evidence.

## Original fixes

A subsequent correctness review found ambiguous unqualified HAVING references
resolving against only grouped columns. This is also fixed: ambiguity is checked
across all joined inputs first. Deterministic and generated tests cover rejection,
qualified references, and unique unqualified names across all three SQL providers,
both dialects, and native/local execution.

HAVING binds actual grouped sources and supported SELECT aliases before evaluating
expressions. Missing references and ungrouped source columns fail planning. The
recursive expression helpers now receive one resolver instead of repeatedly
passing the complete binding context.

Relational validation tracks lexical CTE output schemas and recursive seed outputs.
It checks operator references against input columns, including projections,
filters, aggregate inputs, sorts, window arguments, and join keys. Unknown columns
fail before provider execution rather than becoming nulls or zero counts.

## Additional P1 correctness defect

The missing-reference generator shrank a failure to:

```sql
WITH q0 AS (SELECT id, org FROM accounts)
SELECT id, COUNT(*) AS n
FROM q0 exposed
GROUP BY id
ORDER BY exposed.secret
```

This succeeded despite an unavailable ordering key. The key had been converted
to an unqualified name after grouping and bypassed scan validation. Input-reference
validation now rejects it; aggregate ordering also rejects non-grouped source
references instead of falling back to a potentially colliding metric alias.
The shrunk example is retained as a deterministic regression.

## Critical security defect: Drizzle identifier injection

The CTE-renaming property shrank a failure to the alias `a"b`. Drizzle emitted
`AS "a"b"`, producing a SQL syntax error. Its SQLite/PostgreSQL `escapeName`
implementation wraps names in double quotes without escaping embedded quotes.
Tupl had passed SQL-authored aliases directly to it.

A follow-up exploit against the test fixture used a valid quoted SELECT alias
containing `FROM vault ... UNION ALL SELECT secret FROM vault --`. By retaining
the expected parameter placeholders, the emitted query read the undeclared vault
table and returned `vault_9` on both SQLite and PostgreSQL. The caller's SQL AST
referenced only the public `accounts` table. This was a demonstrated scope escape,
not just malformed SQL or a hypothetical risk. No writes were tested or claimed.

All Drizzle translation sites that emit dynamic aliases or identifier names now
double embedded quotes before passing them to Drizzle. The same payload returns
only the three authorized literal values after the fix. The regression runs across
all three SQL adapters, both execution modes, both dialects, and changing hidden
values. Additional cases cover column, aggregate, window, and compound aliases.

The application-authored physical table definitions and provider scope callbacks
remain trusted configuration. No public names are prohibited to work around the
backend escaping requirement.

## Why previous fast-check coverage missed this

The existing containment properties varied predicates, hidden data, and a bounded
set of query shapes. Their column and alias vocabulary stayed mostly fixed and
valid. This left scope transitions and identifier quoting largely untested.

The new [column-scope property suite](../../../test/__tests__/column-scope.property.test.ts)
adds three generated contracts:

- Missing references across 17 consumer positions, nested CTEs and derived tables
  must fail before any SQL call.
- Renaming grouped outputs must preserve values and HAVING semantics against an
  independent SQL oracle, including reserved and quoted names.
- Declared CTE output renaming must preserve values through nested and recursive
  CTEs, predicates, and ordering.

Each runs on Drizzle, Kysely, and Objection, with native and scan-only execution,
using SQLite and PostgreSQL. A standard run samples 2,160 generated cases across
those combinations, plus deterministic regressions. Fast-check records seeds and
shrinks failures. Existing predicate and hidden-data noninterference tests remain.
These are bounded properties, not a claim of complete SQL coverage.

## Verification

The full suite passes 3,674 tests across 93 files. Packed ESM, CommonJS, mixed
runtime, and strict declaration consumers pass. Workspace lint, canonical and
uncached types, formatting, and diff whitespace checks also pass. These fixes are included in 0.8.0. The subsequent HAVING ambiguity
fix passed 3,698 tests across the same 93 files, including 360 additional generated
ambiguity cases.
