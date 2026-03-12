# `@tupl/provider-kit`

Provider contracts, entity binding helpers, and reusable shape utilities for `tupl` adapters.

Use this package when authoring custom providers or adapter-style integrations.

Stable adapter-authoring surfaces:

- `@tupl/provider-kit`: adapter contracts, request/row types, entity handles, capability helpers
- `@tupl/provider-kit/shapes`: reusable provider-shape analysis and relational pushdown helpers
- `@tupl/provider-kit/testing`: framework-neutral adapter conformance cases

Ordinary adapter code should not need to import `@tupl/schema-model` directly.

For SQL-like adapters, the ordinary path is now `createRelationalProviderAdapter(...)` on the main
package root. It absorbs the standard entity binding and capability-report wiring so provider
packages can focus on backend-specific compilation and execution.
