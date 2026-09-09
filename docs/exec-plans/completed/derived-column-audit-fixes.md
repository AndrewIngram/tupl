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
  applied before lowering outputs, so duplicate inner column names can acquire
  distinct enclosing names. Recursive seed names are available to recursive terms.
- ORDER BY output references become ordinals before enclosing output renaming;
  source references remain qualified source references. This prevents new aliases
  from capturing existing ORDER BY names.
- Callback return types reject Promise-like union constituents. JSON column
  definitions preserve known derived value shapes while leaving unknown inputs
  unknown and retaining nullability checks.
- Real SQLite regressions cover native/derived CTE aliases, recursive aliases,
  wildcard outputs, invalid alias lists, ordering name collisions, scalar and
  correlated EXISTS/NOT EXISTS, throwing unused values, distinct/paging/set cases,
  and private-field validation. Compile-time regressions cover both typing findings.

## Verification

- All four audit findings resolved. Additional compile regressions reject known
  null/undefined results for non-nullable derived JSON while preserving unknown
  JSON for runtime validation.
- `vp test`: 88 test files and 3,375 tests passed.
- `vp lint`, `vp run -r typecheck`, `vp fmt`, and `git diff --check`: passed.
- `pnpm -r exec tsgo --noEmit -p tsconfig.json`: passed without task-cache reuse.
- Changes remain uncommitted in the current checkout.
