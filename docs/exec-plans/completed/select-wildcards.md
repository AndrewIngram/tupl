# SELECT wildcard projections

Expand bare and qualified wildcards before SELECT analysis using public schema column order and ordered CTE outputs. Carry CTE output shapes through nested scopes and seed recursive scope from the anchor query. Keep COUNT(*) unchanged. Reject duplicate projection names because rows are name-keyed objects; require explicit aliases. Validate set-operation arity before aligning outputs.

Verify parser behavior, public/calculated column visibility, aliases, joins, CTEs, derived tables, recursive queries, mixed projections, aggregates, and set operations using real query execution and plans. Run the four required workspace checks and document SQL behavior.

## Outcome

Implemented expansion before simple SELECT analysis, ordered CTE scopes, recursive seed visibility, duplicate-name errors, and set-operation arity validation. Derived tables in later set-operation branches also pass through structured lowering.

Regression coverage includes SQLite query parity and plan output order, calculated public columns and aggregate views, and authorization containment across Drizzle, Kysely, and Objection on SQLite and PostgreSQL in both local and pushed-down execution. The existing Fibonacci recursive test continues to pass with positional output names.

The full workspace lint, typecheck, tests, and formatter are required before final delivery. Duplicate names intentionally produce an error; callers must alias explicit projections. No TypeScript-derived-column API was added.
