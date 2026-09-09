# Contract audit remediation, 9 September 2026

Packaging follow-up: 0.8.0 now publishes native ESM only. CommonJS and mixed-format
checks below describe the earlier remediation, not the current package contract.
See [published entry points](../../package-architecture.md#published-entry-points).

All six new findings and three previously known defects are fixed in the same checkout.
The [original report](README.md) and its result files retain the pre-fix baseline.

| Finding                     | Result                                                                                                                                                |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| F1, packed imports          | Every core public entry point exports built JavaScript and matching ESM/CommonJS declarations.                                                        |
| F2, aggregate aliases       | Group values use collision-free internal names shared by HAVING, ordering, windows, and final projection.                                             |
| F3, compound pagination     | Trailing ordering and pagination apply to the complete set operation, including CTE output alignment.                                                 |
| F4, arithmetic              | Null operands propagate null; division and remainder by zero return null.                                                                             |
| F5, Drizzle capabilities    | Unsupported projection expressions decline native execution before compilation.                                                                       |
| F6, reserved names          | Output maps preserve own properties such as `__proto__`; Drizzle uses local final projection when its decoder cannot.                                 |
| K1, embedded scan modifiers | The planner expands scan ordering and pagination into explicit nodes, retaining hidden ordering inputs. SQL capabilities reject unexpanded modifiers. |
| K2, source coercion         | Demanded coercions become local expressions before dependent operations. Unused coercions do no work; standalone mapping remains supported.           |
| K3, grouped navigation      | Invalid value/default references fail planning; valid group and aggregate aliases resolve correctly.                                                  |

## Evidence

The durable [regression suite](../../../packages/test-support/src/__tests__/providers/contract-audit-regressions.test.ts)
runs each scenario against Drizzle, Kysely, and Objection, both with ordinary
provider pushdown and with scan-only execution. It covers public values and actual
session routes, reserved own properties, coercion demand, grouped alias collisions,
and rejection of unsupported recursive CTE body pagination.

The post-fix audit rerun completed 360 comparisons. Of those, 342 agree with
SQLite. Nine differences are the existing floating-point integer-division policy;
the other nine are grouped navigation queries now rejected as intended. All 14
lifecycle checks pass. These probes can write separate evidence using
`TUPL_AUDIT_OUTPUT_DIR`, preserving the historical baseline.

`pnpm test:packed` passes for all 22 core exports in ESM and CommonJS consumers,
strict NodeNext consumers in both module modes, and actual native and derived
SQLite queries. The consumers use the published tarballs and do not skip checking
library declarations.

Full workspace lint, canonical and uncached TypeScript checks, tests, formatting,
and diff whitespace checks pass. The complete suite contains 3,458 tests in 92
files.

## Deliberate limits

Ordinary division remains floating point. Choosing integer-sensitive division is
a separate contract decision. Recursive CTE body ORDER BY/LIMIT now fails explicitly;
implementing SQLite's recursive queue semantics is separate work. Ordering and
pagination of the outer query remain supported.
