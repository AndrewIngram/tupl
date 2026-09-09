# Package Architecture

`tupl` uses six semantic packages with one public application-facing facade:

- `@tupl/foundation`: relational model primitives, diagnostics, and value helpers
- `@tupl/provider-kit`: provider contracts, adapter-authoring helpers, entity binding, and reusable provider-shape helpers
- `@tupl/schema-model`: schema DSL, core schema/query contracts, and explicit subpaths for DSL internals, normalization, mapping, planning, and validation
- `@tupl/planner`: SQL parsing, lowering, and physical planning
- `@tupl/runtime`: query execution, guardrails, constraints, and sessions
- `@tupl/schema`: canonical application-facing surface for schema authors

Terminology:

- `provider`: the runtime object registered under a provider name
- `adapter`: the authoring layer or helper that constructs a provider
- `backend`: the wrapped external engine or query builder

Allowed dependency directions:

- `provider-kit` -> `foundation`
- `schema-model` -> `foundation`, `provider-kit`
- `planner` -> `foundation`, `provider-kit`, `schema-model`
- `runtime` -> `foundation`, `provider-kit`, `schema-model`, `planner`
- `schema` -> `schema-model`, `runtime`

Layer invariants:

- `@tupl/foundation` owns the relational vocabulary, diagnostics, and value helpers. Callers may rely on its data model, but must not assume execution or schema-building behavior.
- `@tupl/provider-kit` owns provider contracts, adapter-authoring helpers, entity handles, optional capability-analysis helpers, provider-shape analysis, and provider testing helpers. Callers may build providers against it, but must not assume how schemas are normalized or how runtime sessions are orchestrated.
- `@tupl/schema-model` owns logical schema authoring and schema/query contracts. Its root stays focused on schema-builder entrypoints, query/request contracts, and simple table behavior; DSL-token detail, planning hooks, normalization, mapping, enum resolution, definition helpers, and validation live on explicit subpaths.
- `@tupl/planner` owns SQL lowering and physical planning. Callers may rely on relational planning output, but must not assume runtime guardrail policy or provider execution semantics.
- `@tupl/runtime` owns executable-schema construction, guardrails, query orchestration, and sessions. Callers may rely on execution contracts, but must not depend on planner-internal shapes beyond the published explain surface.
- `@tupl/schema` owns the application-facing facade. It should expose the documented schema/runtime workflow, not mirror the full lower-layer export surface.

Cross-module rules:

- Import from the narrowest package that owns the concept.
- Do not import upward within the six-package graph.
- Package roots may aggregate real concepts; internal alias layers should be deleted instead of documented.
- Public subpaths should resolve directly to real modules, not to one-hop wrapper files.
- Package-local tests and support helpers should import from their owning package or lower layers, not from `@tupl/schema`.
- `@tupl/foundation` must remain product-only and must not absorb test fixtures, harnesses, or adapter conformance logic.
- Internal cross-package test infrastructure lives in the private `@tupl/test-support` workspace package.
- External provider authors should use `@tupl/provider-kit/testing` instead of importing repo-only helpers.

The workspace boundary tests parse TypeScript imports and re-exports, including
type imports, literal dynamic imports, and relative references across packages.
Public forwarding-module exceptions come from package export declarations.
Internal re-export-only modules must give way to direct imports from the owning
module. File length and historical filenames do not define package ownership.

Consumer guidance:

- Provider implementations should prefer `@tupl/provider-kit`, `@tupl/provider-kit/shapes`, and `@tupl/provider-kit/testing` for ordinary adapter work.
- Ordinary schema consumers should stay on `@tupl/schema-model` root or, preferably, `@tupl/schema`; advanced tooling should opt into `@tupl/schema-model/dsl`, `@tupl/schema-model/planning`, `@tupl/schema-model/normalized`, or `@tupl/schema-model/table-planning` explicitly.
- Ordinary SQL-like provider adapters should start with `createSqlRelationalProviderAdapter(...)`; lower-level `createRelationalProviderAdapter(...)` remains for unusual adapters that do not fit that shape.
- The SQL-relational helper should keep provider roots close to manual provider authoring: top-level lifecycle/config fields, one nested `queryBackend` for backend query translation, and an `advanced` escape hatch only for real backend exceptions.
- The primary provider contract is rel-first: `canExecute(rel)`, `compile(rel)`, optional `describeCompiledPlan(plan)`, and `execute(plan)`.
- Provider authoring should prefer rel-shape and field-policy helpers over any second capability language.
- `@tupl/foundation` remains available for primitive relational helpers, but it is not the primary extension boundary.
- Provider implementations should not normally import `@tupl/schema-model`; that package owns schema internals, not the ordinary adapter-authoring surface.
- Provider conformance belongs on `@tupl/provider-kit/testing`; internal test fixtures do not.
- Application docs and examples should prefer `@tupl/schema` and first-party provider packages.
- Maintainers should use [`maintainer-bug-map.md`](./maintainer-bug-map.md) as the starting point for bug triage across provider, planner, and runtime layers.

Local computation ownership follows the same layering: foundation defines the
`local` expression and operation descriptor; schema-model owns typed handles,
dependency normalization, and the callback registry; planner owns demand pruning
and stage placement; runtime evaluates registered operations and records actual
work. Provider-kit and provider packages never receive executable callbacks.

## Published entry points

All ten public packages publish native ESM JavaScript (`.mjs`) and ESM declarations
(`.d.mts`). Public roots and subpaths expose `import` and `types` entries, with a
`source` condition for workspace tooling. There are no CommonJS builds or
forwarding facades. Applications use ESM imports. Shared state stays in the owning
package's ESM modules, so builders and derived-operation registries retain their
identity across public entry points.

Workspace test aliases derive from package export declarations and resolve source
modules. Run tests from the repository root with `vp test`; package builds are
not prerequisites for that suite.

Run `pnpm test:packed` to build and verify actual tarballs in an isolated consumer.
It checks every public export across all ten packages, native and derived SQLite
queries using both builders and normalized schemas, and strict NodeNext ESM type
resolution. Core and Objection consumer checks do not skip library declarations.
An additional import check covers every provider with `skipLibCheck`, because
Drizzle's dependency declarations include optional drivers and upstream type
errors. The verifier also rejects CommonJS artifacts or export conditions in
public packages.
