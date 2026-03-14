# Relational Pipeline

`tupl` uses a staged, rel-first pipeline.

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
- provider plan descriptions when available

## Conventions and ownership

- `logical` is planner-owned rewrite space.
- `local` is canonical in-memory execution.
- `provider:<name>` marks provider-owned physical subtrees.
- Whole-query pushdown is just the case where a provider owns the root fragment.

## Fragment planning

- Current fragment planning is greedy and maximal-first.
- Providers compile canonical rel subtrees.
- Unsupported or cross-provider portions fall back to local execution.
- Fragment boundaries materialize canonical rows.

## Current limitations

- Physical planning is not cost-based.
- `explain()` currently compiles provider-owned fragments for description.
- Non-`ROWS` window frame modes are explicitly rejected.
- Remaining architecture questions are tracked in [tech debt](../exec-plans/tech-debt-tracker.md).
