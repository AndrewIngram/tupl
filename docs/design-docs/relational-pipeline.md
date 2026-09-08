# Relational Pipeline

`tupl` uses a staged, rel-first pipeline.

Schema finalization is outside the query planner/runtime loop:

- schema build/finalization happens before execution
- linked enums are materialized before runtime query/explain entry
- provider bindings are validated when the prepared runtime schema artifact is created

## Canonical stages

1. Parse SQL.
2. Lower to initial logical rel.
3. Rewrite logical rel.
4. Validate executable logical rel.
5. Assign physical conventions and fragments.
6. Execute mixed local/provider physical plans over canonical rows.

`explain()` exposes the major artifacts from that pipeline:

- normalized SQL
- initial rel
- rewritten rel
- physical plan
- fragment boundaries
- provider plan descriptions in either basic or enriched mode

## Conventions and ownership

- `logical` is planner-owned rewrite space.
- `local` is canonical in-memory execution.
- `provider:<name>` marks provider-owned physical subtrees.
- Whole-query pushdown is just the case where a provider owns the root fragment.

## Fragment planning

- Fragment selection stays maximal-first, but support discovery is bottom-up and memoized.
- Providers compile canonical rel subtrees.
- Unsupported or cross-provider portions fall back to local execution.
- Fragment boundaries materialize canonical rows.

## Execution observations

Session plans describe possible work. Runtime observations describe relational
node invocations that actually execute, including the selected provider or local
route. A provider-owned subtree produces one fragment observation, not fictional
completions for its unexecuted local children.

Completion events have a relational node ID, a unique execution ID, and a per-node
occurrence number. A matching static step ID is included when available. Runtime
fallback can expose operations hidden by the static plan. Recursive evaluation can
produce several observations for one node; step state keeps its latest occurrence.

Events arrive in completion order while execution continues. Timings include time
spent awaiting children, so summing step durations does not measure query duration.
Unexecuted steps and explanatory grouping nodes have no measured completion.

Sessions retain unread event metadata until consumed and capture rows only for the
final output when requested. Timeouts close observation; late provider completion
cannot change the session's retained failure.

## Current limitations

- Physical planning is not cost-based.
- Non-`ROWS` window frame modes are explicitly rejected.
- Remaining architecture questions are tracked in [tech debt](../exec-plans/tech-debt-tracker.md).
