# SQL name-resolution review fixes

Status: complete. Follow-up to the two SQL findings in the
[commit review](../../audits/2026-09-09-contract-audit/commit-review.md).

- Grouped window inputs resolve grouped source identities before colliding metric
  aliases. Qualified references retain their relation and must match a grouped
  source. Navigation values, defaults, partitions, and ordering share resolution.
- Structured SELECT lowering retains each branch's expanded SELECT metadata with
  its lowered relation. Compound ORDER BY resolves against those branch scopes,
  then maps to an output position. Enclosing CTE names do not replace SQL scope.
- Wildcard expansion failures remain tagged planning errors.
- Regression queries compare with real SQLite on all three SQL adapters, using
  normal provider execution and scan-only execution. Existing containment tests
  verify invalid wildcards still fail through the Result boundary.

Verification: full workspace lint, canonical and uncached types, tests, formatting,
and diff whitespace checks. The separate ESM/CommonJS package identity defect is
outside this follow-up's scope and remains open.
