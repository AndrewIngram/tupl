# Shared package identity

Superseded by [native ESM-only publishing](esm-only-packages.md) in 0.8.0.
This document records the earlier dual-format implementation.

Status: complete. Resolve the mixed ESM/CommonJS finding from the contract audit.

- Publish one CommonJS implementation for each core package, with ESM runtime and
  declaration facades forwarding to it. Preserve every existing root/subpath and
  the workspace source condition. Do not introduce global registries.
- Generate facades from package export manifests during package builds so adding
  an entry point cannot silently create a separate runtime implementation.
- Extend isolated packed consumers to compare export identities, exchange builders
  and derived definitions in both directions, execute real SQLite queries, and
  typecheck mixed-format inputs without skipping declarations.
- Run the packed build/consumer check and required workspace lint, types, tests,
  and formatter. Update package architecture and close the review finding.

Verification passed: all six packed runtime/type consumers, 3,458 workspace tests,
lint, canonical and uncached workspace types, formatting, and diff whitespace.
The preceding SQL fixes were committed separately as `34b3f2e`.
