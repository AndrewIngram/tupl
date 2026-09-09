# ESM-only packages

Publish all ten public packages as native ESM in 0.8.0. Remove CommonJS builds,
conditional require exports, and generated runtime/declaration forwarding facades.
Keep source conditions for workspace tooling and preserve public subpath names.

- [x] Align manifests and build configurations on native ESM outputs.
- [x] Delete the shared CommonJS identity helper and mixed-format consumer checks.
- [x] Verify every public package export and declaration from real tarballs;
      preserve builder/registry identity across public ESM entry points.
- [x] Run real native and derived SQLite queries from the packed consumer.
- [x] Update current documentation and mark the former dual-format plan superseded.
- [x] Run lint, workspace typecheck, tests, formatting, and packed-consumer checks.

## Verification

All 3,698 tests across 93 files pass, along with lint, canonical and uncached
workspace typechecks, and formatting. All ten packages build as native ESM.
Packed checks import all 25 public entry points, execute native/derived SQLite
queries using both builders and normalized schemas, and validate core/Objection
declarations with strict NodeNext settings. Provider-wide import resolution uses
`skipLibCheck` because Drizzle dependency declarations contain upstream errors.

The former CommonJS implementation/facade helper is deleted. Tarball verification
rejects CommonJS files and export conditions. CI is checked after pushing.
