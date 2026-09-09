# ESM-only packages

Publish all ten public packages as native ESM in 0.8.0. Remove CommonJS builds,
conditional require exports, and generated runtime/declaration forwarding facades.
Keep source conditions for workspace tooling and preserve public subpath names.

- [ ] Align manifests and build configurations on native ESM outputs.
- [ ] Delete the shared CommonJS identity helper and mixed-format consumer checks.
- [ ] Verify every public package export and declaration from real tarballs;
  preserve builder/registry identity across public ESM entry points.
- [ ] Run real native and derived SQLite queries from the packed consumer.
- [ ] Update current documentation and mark the former dual-format plan superseded.
- [ ] Run lint, workspace typecheck, tests, formatting, and packed-consumer checks.
- [ ] Commit, push, and inspect GitHub CI.
