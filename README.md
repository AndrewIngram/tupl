# sqlql

Monorepo scaffold for a publishable TypeScript SQL-facade library.

## Packages

- `@sqlql/core`: schema + table method contracts (`scan`, optional `lookup`, optional `aggregate`) and `toSqlDDL(schema)`
- `@sqlql/sql`: SQL parsing and execution (`query({ schema, methods, sql })`)
- `@sqlql/executor-memory`: in-memory executor for early PoCs
- `@sqlql/example-basic`: local example app using all packages

## Design docs

- `docs/resolver-plan-api.md`: draft resolver + planning API

## Install

```bash
pnpm install
```

## Build everything

```bash
pnpm build
```

## Typecheck everything

```bash
pnpm typecheck
```

## Test

```bash
pnpm test
```

## Lint and format

```bash
pnpm lint
pnpm fmt:check
```

## Run example

```bash
pnpm --filter @sqlql/example-basic build
pnpm --filter @sqlql/example-basic start
```

## Publish packages

Build + publish each package from `packages/*`:

```bash
pnpm --filter @sqlql/core publish --access public
pnpm --filter @sqlql/sql publish --access public
pnpm --filter @sqlql/executor-memory publish --access public
```

Each publishable package uses `tsdown` in `prepublishOnly` to generate ESM, CJS, and type declarations into `dist/`. `@sqlql/sql` uses `node-sql-parser` for AST parsing.
