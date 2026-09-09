# Aggregate ordering across output aliases

Fix the regression found in the audit of 1b171d3. SQL compilation must preserve
aggregate sort expressions when a final projection renames or omits their values.

- [x] Resolve aggregate sort keys to explicit metric expressions in the shared SQL
      compiler, independent of final SELECT names.
- [x] Render metric ordering through existing Drizzle, Kysely and Objection
      aggregate expression builders. Keep WITH-body ordering limited to its
      supported column/window outputs.
- [x] Verify renamed, hidden, ordinal, distinct and source-collision cases against
      real SQLite execution; retain grouped source ordering.
- [x] Update durable docs and run lint, workspace typechecks, tests and formatting.

Use the current checkout and the existing single subagent for backend adapters.
Keep shared resolution, integration coverage and final verification here.

## Verification

- 36 real SQLite integration cases pass across Drizzle, Kysely and Objection,
  with native SQL result comparisons and remote-ordering plan checks.
- Full suite: 89 files and 3,422 tests passed.
- Lint, canonical and uncached workspace typechecks, formatting and diff
  whitespace checks passed. Focused integration tests passed again after narrowing
  an optional SQL-description field in the test assertion.
- The bounded follow-up review found no introduced correctness issues.
- Changes remain uncommitted in the current checkout.
