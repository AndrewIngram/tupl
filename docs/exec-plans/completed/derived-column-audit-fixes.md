# Derived-column audit fixes

Status: completed.

Resolve all four findings from the review of 2816502:

- [x] Propagate existence-only output demand into subquery planning. Preserve
      cardinality-changing operators, predicates, distinct comparisons and pagination.
- [x] Reject callback return unions containing Promise-like values.
- [x] Preserve inferred derived JSON types through typed columns and view references.
- [x] Apply explicit CTE column aliases positionally, including recursive CTE seeds,
      and reject invalid alias lists with tagged diagnostics.
- [x] Add runtime and compile-time regressions, update durable docs, and run lint,
      canonical and uncached workspace typechecks, all tests, and formatting.

Use the current checkout and one existing subagent for the typing fixes. Keep
planner changes here. No publication or additional commit is requested.

## Resolution

- Expression subquery rewriting carries scalar/existence mode into local
  dependency planning. Existence starts with no requested output values; existing
  backward demand retains filter, grouping, window, set-comparison and paging
  requirements. Unneeded sorting is removed only for cardinality-only demand.
- Public schema validation still precedes pruning. A private column in an EXISTS
  target list remains an error, even though its output would otherwise be unused.
- CTE aliases are retained in the parsed AST, checked for uniqueness/arity and
  applied in the final projection after SELECT-local clause resolution, so duplicate inner column names can acquire
  distinct enclosing names. Recursive seed names are available to recursive terms.
- SELECT-local HAVING, window and ORDER BY references retain their original names
  until the final projection applies enclosing aliases. Duplicate inner values use
  distinct internal names when the enclosing relation supplies unique aliases.
- Callback return types reject Promise-like union constituents. JSON column
  definitions preserve known derived value shapes when no coercer can change them,
  while leaving coerced or unknown inputs unknown and retaining nullability checks.
- Real SQLite regressions cover native/derived CTE aliases, recursive aliases,
  wildcard outputs, invalid alias lists, ordering name collisions, scalar and
  correlated EXISTS/NOT EXISTS, throwing unused values, distinct/paging/set cases,
  and private-field validation. Compile-time regressions cover both typing findings.

## Initial verification

- All four audit findings resolved. Additional compile regressions reject known
  null/undefined results for non-nullable derived JSON while preserving unknown
  JSON for runtime validation.
- `vp test`: 88 test files and 3,375 tests passed.
- `vp lint`, `vp run -r typecheck`, `vp fmt`, and `git diff --check`: passed.
- `pnpm -r exec tsgo --noEmit -p tsconfig.json`: passed without task-cache reuse.
- Committed as `e887587` before the follow-up audit.

## Follow-up audit of e887587

- [x] Fix SELECT-local HAVING aliases under explicit CTE output names by moving
      enclosing renaming to the final projection. Remove the AST ordering rewrite.
- [x] Erase JSON shape when coercion may change it across derived, native and view
      column definitions. Preserve inference when no coercer is present.
- [x] Add grouped/derived HAVING, set-branch, alias-collision, duplicate-value and
      coercion runtime regressions, plus direct/view/optional-options type checks.
- [x] Preserve SELECT order when aggregate and window projections are interleaved,
      so positional CTE aliases cannot exchange their values.

### Follow-up verification

- `vp test`: 88 files and 3,386 tests passed.
- `vp lint`, `vp run -r typecheck`, `vp fmt`, and `git diff --check`: passed.
- `pnpm -r exec tsgo --noEmit -p tsconfig.json`: passed without task-cache reuse.
- Follow-up changes remain uncommitted in the current checkout.
