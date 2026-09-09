# Planner Invariants

These invariants should hold unless a deliberate architecture change updates this file.

## Logical model

- There is no SQL rel fallback node in the canonical logical IR.
- Supported queries lower to canonical rel or fail with tagged planning errors.
- SELECT wildcards expand to public column references before projection analysis. CTE scopes carry ordered output names, including recursive seed outputs.
- SELECT outputs have unique names because result rows are name-keyed objects. Reject collisions before provider planning; never silently overwrite them.
- Set-operation branches have equal arity before positional output alignment.
- Final executable plans should not contain unresolved scalar/EXISTS subquery expressions.

## Provider ownership

- `cte_ref` is a local barrier for provider ownership.
- `values`, `correlate`, and `repeat_union` are also local ownership barriers under the current
  execution model.
- Cross-provider or local-only nodes must not be misattributed to one provider just because nearby scans share a provider.
- Providers receive normalized rel subtrees, not planner-private wrapper nodes.

## Rewrite and execution

- The executor consumes rewritten rel, not raw lowered rel.
- Planner rewrite work does not happen again inside the executor.
- View expansion and decorrelation belong to planner rewrite stages.
- Runtime query/explain entrypoints consume prepared runtime schema artifacts, not raw schema plus
  provider maps that still need final normalization.
- `explain()` has explicit basic and enriched provider-description modes; only enriched mode may
  compile provider fragments.

## Window semantics

- `ROWS` frames are supported.
- Non-`ROWS` frame modes are explicitly unsupported until semantics are implemented correctly.

## Error and type surfaces

- Expected library failures should use tagged `Result` errors.
- Internal helper signatures should prefer inference over wide explicit return annotations.

## Runtime resource ownership

- Runtime owns materialization limits. `maxExecutionRows` caps each intermediate
  or final row set, rather than only the final query output.
- Local operators check growth before appending beyond the limit. Runtime checks
  provider arrays before mapping them, but does not control provider allocation.
- Query sessions retain one execution outcome, including failures. Observing a
  session must not start the query a second time or replace failure with empty rows.

## Derived computation

- Declaring a derived column does not make its relation local. Only query demand
  for it or its transitive dependencies introduces computation stages.
- Demand includes predicates, keys, aggregates, windows, sorting, and distinctness.
  Unused output pruning must preserve cardinality, including zero-input aggregates.
- Local expression descriptors carry identities and arguments, never callbacks.
  Provider ownership stops at local expressions; native descendants remain eligible.
- Sharing is per operation, stage, row occurrence, and execution. Never memoize by
  primary key, value equality, or across queries.
- Split only independently movable conjunctions. Preserve OR, outer-join null
  extension, grouping, windows, and pagination boundaries.
- Projection-only work may follow native ordering and pagination. Values used by a
  local predicate must be computed before that predicate and its downstream limit.
- Private dependencies are not public SQL fields, including under wildcards.
- Source dependency coercion precedes target validation. Nullable dependencies are
  explicit callback inputs; public outputs obey their declared SQL types.
- Application callbacks are trusted, pure and synchronous. Reject Promise-like
  results and wrap application failures; never turn them into null or empty output.

## Existence demand and CTE aliases

- An EXISTS subquery demands cardinality, not projected values. Preserve predicates,
  grouping, distinct/set comparison inputs and pagination while removing unused
  computations. Ordering can disappear only when no downstream value is demanded.
  Validate public column access before pruning.
- Preserve explicit CTE column lists in the parser and apply them positionally to
  outputs. Recursive seeds expose these names to the recursive term. Alias count
  must match output arity, and aliases must be unique. SELECT-local ORDER BY aliases
  still resolve before the enclosing CTE renames its outputs.
- Derived callback return unions containing Promise-like values are rejected.
  Typed derived JSON retains its inferred shape through column definitions and
  qualified view references; unvalidated provider JSON stays unknown.
