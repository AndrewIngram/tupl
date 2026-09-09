# Column scope correctness

Status: complete. Fix unresolved HAVING and CTE references, and expand generative
coverage to find related wrong-result failures.

- Keep CTE output schemas in lexical validation scope, including recursive seeds;
  reject unavailable references before provider execution or demand pruning.
- Resolve HAVING source columns and SELECT aliases against grouping/metric values,
  preserving qualification and rejecting unknown or unavailable inputs.
- Add regression examples for both audit defects across real SQL providers.
- Add fast-check properties generating relation wrappers, aliases, clause placement,
  and missing-column mutations. Check valid queries against an independent SQL
  oracle; invalid mutations must return errors before provider calls. Preserve
  minimized counterexamples as permanent regressions.
- Run the expanded properties, existing containment tests, full workspace checks,
  and update durable invariants and the audit report with findings and limits.

Expanded properties found and fixed aggregate ordering on missing columns and a
Drizzle identifier-injection scope escape. Results and coverage are recorded in
[the remediation report](../../audits/2026-09-09-sql-scope-audit/remediation.md).
