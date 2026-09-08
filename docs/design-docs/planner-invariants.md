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
