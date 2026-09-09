# Preserve relational operation order during SQL compilation

Fix sorting after renamed aggregate projections and investigate similar namespace
and operation-order losses in flattened SQL pipelines.

- [x] Bind sort keys in the input namespace of their original sort node, including
      projections above and below sorting.
- [x] Check equivalent basic, WITH and set-operation compositions. Reject unsafe
      flattening during capability discovery so the runtime keeps the boundary.
- [x] Add direct relational-plan regressions and real provider execution coverage.
- [x] Update durable invariants and run the required workspace verification.

Use the current checkout and the existing single subagent for independent probes.

## Resolution

- Resolve sort keys through projections only when the projection precedes the
  sort. Preserve source qualifications and native computed output references.
- Carry source/projected predicate scope to backend translation. Drizzle no longer
  interprets source predicates or qualified source sorts through a later rename.
- Reject noncommuting operation inversions during basic, set-op and WITH capability
  analysis. Runtime/view tests confirm limits retain their position relative to
  filtering, sorting and aggregation.
- Rebind predicates and sort references when planner normalization removes an
  intervening projection. Retain computed projections beneath sorting when their
  values cannot be substituted as column references.
- Give Drizzle aggregate/computed selections explicit SQL aliases so WITH consumers
  can reference them.
- Correct the existing calculated-projection unit fixture to put computation
  before predicates that read it, rather than referring to future outputs.

## Verification

- 91 test files and 3,452 tests passed.
- Real SQLite coverage exercises source and aggregate sorts before/after renames,
  qualified names, source filtering before a rename, WITH aggregate outputs,
  seven unsafe flattening cases and native computed-sort controls across all
  three SQL providers. Existing playground pushdown tests remain green.
- `vp lint`, `vp run -r typecheck`, `vp fmt`, `git diff --check` and uncached
  `pnpm -r exec tsgo --noEmit -p tsconfig.json` passed.
- Changes are included in the 0.8.0 release.

## Focused security review

No confirmed vulnerabilities found in the changed paths. Public SQL validation
still precedes provider normalization, projections determine exported fields,
SQL identifiers use backend quoting APIs, and predicate values retain parameter
binding. This review covered the changed namespace and operation-order paths;
it was not a fresh audit of every provider. Compositions that need local execution
remain subject to existing materialization and execution limits.
