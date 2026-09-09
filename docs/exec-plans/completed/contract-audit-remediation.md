# Contract audit remediation

Status: complete. Fix all nine confirmed findings from the [9 September audit](../../audits/2026-09-09-contract-audit/README.md). Preserve existing floating-point division behavior; the separate integer-division question is not one of the nine fixes.

## Implementation and acceptance

- [x] F1: publish built roots/subpaths and declarations; real packed ESM/CommonJS/NodeNext consumer checks.
- [x] F2: separate aggregate internal names from public aliases; grouping/HAVING/order/window regression coverage.
- [x] F3: lower compound ordering/pagination around the entire set operation; all four set operators and local/provider routes.
- [x] F4: null-propagating binary arithmetic and defined zero-divisor behavior; local/native regressions.
- [x] F5: align Drizzle expression capabilities with translation; decline unsupported functions safely.
- [x] F6: preserve reserved property names in output and SQL projection maps.
- [x] K1: preserve embedded scan order/limit/offset by canonicalization or safe provider handling.
- [x] K2: represent public source coercion as demanded local computation; apply once before dependent operations and retain standalone mapping.
- [x] K3: validate aggregate navigation inputs/default expressions against available values.
- [x] Rerun audit probes, consumer checks, lint, full workspace types/tests, and formatter.
- [x] Update durable invariants/debt and complete this plan.

Packaging is independently owned by the existing single subagent. Planner/runtime/schema implementation stays in this task. Regression tests should assert public semantics and real execution routes, not implementation snapshots. The audit's original baseline remains historical evidence; record post-fix results separately.

Results and verification are recorded in [the remediation report](../../audits/2026-09-09-contract-audit/remediation.md).
