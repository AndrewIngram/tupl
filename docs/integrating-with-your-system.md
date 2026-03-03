# Integrating With Your System (v1)

This guide covers the provider-first runtime in `sqlql` v1.

## Runtime surface

1. `defineSchema(...)` with `provider` per table.
2. `defineProviders(...)` with one adapter per provider name.
3. `query({ schema, providers, context, sql, queryGuardrails? })`.

## Provider interface

A provider implements:

- `canExecute(fragment, context)`
- `compile(fragment, context)`
- `execute(compiled, context)`
- optional `lookupMany(request, context)`
- optional `estimate(fragment, context)`

`lookupMany` enables cross-provider lookup joins (`INNER`/`LEFT`) without full scans on the lookup side.

## Minimal provider example

```ts
import { defineProviders, type ProviderAdapter } from "sqlql";

const warehouseProvider: ProviderAdapter<{ tenantId: string }> = {
  canExecute() {
    return true;
  },
  async compile(fragment) {
    return {
      provider: "warehouse",
      kind: fragment.kind,
      payload: fragment,
    };
  },
  async execute(_compiled, _context) {
    return [];
  },
  async lookupMany(_request, _context) {
    return [];
  },
};

const providers = defineProviders({
  warehouse: warehouseProvider,
});
```

## Guardrails

Use `queryGuardrails` for global execution safety:

- `maxPlannerNodes`
- `maxExecutionRows`
- `maxLookupKeysPerBatch`
- `maxLookupBatches`
- `timeoutMs`

Example:

```ts
const rows = await query({
  schema,
  providers,
  context,
  sql,
  queryGuardrails: {
    maxExecutionRows: 100_000,
    timeoutMs: 10_000,
  },
});
```

## Provider packages

- `@sqlql/drizzle`
- `@sqlql/objection`
- `@sqlql/kysely`

These implement the same provider contract with backend-specific execution behavior.
