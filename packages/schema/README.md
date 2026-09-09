# `@tupl/schema`

Application-facing schema authoring and executable-schema APIs for tupl.

Use this package when you need the canonical documented workflow:

- `createSchemaBuilder(...)`
- `createExecutableSchema(...)`
- application-facing schema/query/runtime types

`createExecutableSchema(...)` is also the normal boundary where tupl prepares the runtime-ready
schema artifact: finalized schema, resolved linked enums, and validated provider bindings.

Lower-level planner, runtime, and normalization modules intentionally live in other packages.

## Install

```bash
pnpm add @tupl/schema
```

## Docs

- Repository: <https://github.com/AndrewIngram/tupl>
- Lower-level execution APIs: [`@tupl/runtime`](https://github.com/AndrewIngram/tupl/tree/main/packages/runtime)

### Derived columns

Table and view `columns` callbacks provide `derive(dependencies, compute)` for
pure synchronous TypeScript calculations. Dependencies are column definitions or
other derive handles; expose a result with `col.string(...)`, `col.integer(...)`,
or another existing typed builder. Unreturned inputs and intermediates stay
private. Queries that do not reference derived values avoid their computation and
exclusive source reads. See [the schema guide](../../docs/building-a-schema.md#derived-columns-in-typescript)
for shared parsing/rendering and native filter/pagination examples.
