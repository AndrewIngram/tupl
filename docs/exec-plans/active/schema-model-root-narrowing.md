# Schema-Model Root Narrowing

## Goal

Narrow the `@tupl/schema-model` package root so it stops mixing application-facing DSL exports with normalization and binding-resolution internals.

## Intended changes

- Keep the root focused on the logical schema contract needed by advanced consumers.
- Move normalization, mapping, and binding-resolution internals behind explicit subpaths where appropriate.
- Update internal consumers and boundary tests in the same branch.

## Notes

- This follow-up is intentionally separate from SQL provider convergence.
- It should preserve the existing `@tupl/schema` application-facing facade.
