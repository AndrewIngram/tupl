# Provider scope isolation

## Goal

Every supported provider read must apply the execution context's mandatory scope before relational operations, including outer joins. Query construction must not execute thenable backend builders.

## Changes

- Add database-backed scope regressions for Drizzle, Kysely, and Objection using a shared fixture and relational operation matrix.
- Build scoped derived sources for Drizzle and Kysely, preserving aliases and physical column references.
- Carry Objection builders in a non-thenable translated-query object through all translation hooks.
- Verify context changes, lookup isolation, joins, aggregation, set operations, and CTEs.
- Document the provider scope and query translation contracts.

## Verification

Run the new suite before and after the fixes, existing provider tests, then `vp lint`, `vp run -r typecheck`, `vp test`, and `vp fmt`. Review the final diff for scope bypasses and premature execution.

## Progress

- Audit reproduced missing Drizzle scope, incorrect Kysely outer joins, and premature Objection execution against SQLite.
- Implemented scoped derived inputs for Drizzle and Kysely and non-thenable Objection translation.
- Corrected real Knex runtime detection, full outer join method naming, and window projection construction exposed by the database tests.
- Added 57 database-backed regressions, including grouped aggregates and CTE windows, plus three fast-check properties with 100 generated lookup cases each. All pass.
- Final verification passed: `vp lint`, uncached `vp run -r typecheck --pretty true` across all 18 packages, `vp test` with 616 passing tests across 79 files, and `vp fmt`.
- A playground sandbox test failed once during the first full run, then passed in isolation and on the final full run. No playground changes were made.
- Reviewed scoped source construction, execution-context propagation, and every Objection translation hook. Scope callbacks remain parameterized and failures propagate; builders remain wrapped until execution.
- Validation uses SQLite. PostgreSQL-specific execution was not exercised.

## Toolchain follow-up

- Retained Vite Plus after reviewing its shared configuration and task caching against using the underlying tools directly.
- Upgraded Vite Plus to 0.3.1 (Vite 8.2.2), Vitest and coverage to 4.1.11, and the React plugin to 6.1.1. Added fast-check 4.9.0 and its Vitest integration 0.4.1.
- Applied the upstream migration for Vite aliases, runner pins, packaging defaults, and CI setup. Added an explicit playground Jiti 2 dependency so Vite resolves consistently despite Tailwind using Jiti 1.
- Removed unused imports and an empty export flagged by the newer linter; made existing source roots explicit in TypeScript configs.
- Verified lint, all 18 package typechecks without caching, all 616 tests, coverage, and all 16 builds without caching.

## Remaining assurance work

The subsequent [query containment assurance pass](./query-containment-assurance.md) added public SQL tests with nested Boolean expressions, independently authorized-only reference databases, hidden-row mutations, and PostgreSQL execution through PGlite. It also fixed the projection, predicate, and schema-validation failures exposed by that harness. The tests cover a bounded grammar; arbitrary SQL correctness and error/timing leakage are not proved.
