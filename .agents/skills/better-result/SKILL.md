---
name: better-result
description: Use when migrating library code from thrown or rejected error flows to typed Result-based flows with better-result. Follow official better-result patterns for tagged errors, Result.try, generator composition, and matching.
---

# better-result

Follow the official `better-result` guidance closely and adapt it to the local codebase only where naming or module boundaries require it.

## Core Principles

- Model expected failures in the type signature with `Result<T, E>` or `Promise<Result<T, E>>`.
- Keep domain errors, infrastructure errors, and programmer defects separate.
- Use tagged errors for expected failures you want to handle.
- Let defects throw instead of forcing every problem into `Result`.
- Migrate incrementally from boundaries inward.

## Recommended Migration Order

1. Wrap throwing or rejecting boundaries.
2. Introduce tagged errors for the boundary you are changing.
3. Return `Result` from the changed functions.
4. Replace manual early-return plumbing with generator composition where it helps.
5. Leave compatibility wrappers at the edge if the public API still throws or rejects.

## Tagged Errors

Prefer `TaggedError` for expected failures you want to preserve and match on later.

Do:

```ts
class ValidationError extends TaggedError("ValidationError")<{
  field: string;
  message: string;
}>() {}

class NotFoundError extends TaggedError("NotFoundError")<{
  resource: string;
  id: string;
  message: string;
}>() {}
```

Do not:

```ts
class AppError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}
```

That loses the narrow tagged structure that `better-result` is designed to support.

## `Result.try` and `Result.tryPromise`

Use these only at real exception or rejection boundaries.

Do:

```ts
function parseJsonResult(input: string): Result<unknown, ValidationError> {
  return Result.try({
    try: () => JSON.parse(input),
    catch: () =>
      new ValidationError({
        field: "payload",
        message: "Invalid JSON",
      }),
  });
}
```

Do:

```ts
async function fetchUserResult(id: string): Promise<Result<User, NetworkError>> {
  return Result.tryPromise({
    try: () => apiClient.fetchUser(id),
    catch: (cause) =>
      new NetworkError({
        endpoint: "/users",
        message: "Request failed",
        cause,
      }),
  });
}
```

Do not:

```ts
function requirePositiveResult(value: number): Result<number, ValidationError> {
  return Result.try({
    try: () => {
      if (value <= 0) {
        return Result.err(new ValidationError({ field: "value", message: "Must be positive" }));
      }
      return Result.ok(value);
    },
    catch: () => new ValidationError({ field: "value", message: "Bad value" }),
  });
}
```

That function is pure. Use direct `Result.err(...)` or `Result.ok(...)` instead.

## Direct Domain Checks

Do:

```ts
function requirePositiveResult(value: number): Result<number, ValidationError> {
  if (value <= 0) {
    return Result.err(
      new ValidationError({
        field: "value",
        message: "Must be positive",
      }),
    );
  }

  return Result.ok(value);
}
```

Do not hide straightforward validation behind `try` or `gen`.

## Generator Composition

Use `Result.gen(...)` for multi-step flows where each step can fail.

Do:

```ts
function loadConfigResult(raw: string): Result<Config, ValidationError | NotFoundError> {
  return Result.gen(function* () {
    const json = yield* parseJsonResult(raw);
    const env = yield* readEnvironmentResult(json);
    const config = yield* validateConfigResult(env);
    return Result.ok(config);
  });
}
```

Do not:

```ts
function loadConfigResult(raw: string): Result<Config, ValidationError | NotFoundError> {
  const json = parseJsonResult(raw);
  if (Result.isError(json)) {
    return json;
  }

  const env = readEnvironmentResult(json.value);
  if (Result.isError(env)) {
    return env;
  }

  return validateConfigResult(env.value);
}
```

Use `Result.await(...)` for async result-returning steps:

```ts
const user = yield* Result.await(fetchUserResult(userId));
```

Do not use `Result.gen(...)` for trivial one-step functions:

```ts
function parsePortResult(value: number): Result<number, ValidationError> {
  if (value < 1 || value > 65535) {
    return Result.err(new ValidationError({ field: "port", message: "Out of range" }));
  }
  return Result.ok(value);
}
```

## Matching

Use matching when behavior genuinely depends on the error tag.

Do:

```ts
return matchError(error, {
  ValidationError: (err) => ({ status: 400, body: err.message }),
  NotFoundError: (err) => ({ status: 404, body: `${err.resource} not found` }),
});
```

Do not match when you only need a pass-through check:

```ts
if (ValidationError.is(error) || NotFoundError.is(error)) {
  return error;
}
```

That branch is simpler than a match and should stay simple.

## Error Checks

Prefer class guards from `TaggedError`.

Do:

```ts
if (ValidationError.is(error) || NotFoundError.is(error)) {
  return error;
}
```

Avoid `instanceof` in migrated code when the generated `.is(...)` guard is available.

Avoid one-off helper abstractions unless reused enough to reduce duplication.

## Preserve Narrow Error Shape

Do:

```ts
return Result.err(
  new NotFoundError({
    resource: "User",
    id,
    message: `User ${id} was not found`,
  }),
);
```

Do not:

```ts
return Result.err(
  new Error(`Operation failed: user ${id} missing`),
);
```

Do not collapse a specific, actionable error into a generic fallback unless the boundary genuinely lacks enough structure to do better.

## Defects vs Expected Errors

Expected failure:

```ts
function findUserResult(id: string): Result<User, NotFoundError> {
  const user = users.get(id);
  if (!user) {
    return Result.err(new NotFoundError({ resource: "User", id, message: "User not found" }));
  }
  return Result.ok(user);
}
```

Defect:

```ts
function renderUserCard(user: User): string {
  if (!user.profile) {
    throw new Error("Invariant violated: profile must be present before rendering");
  }
  return user.profile.displayName;
}
```

Do not convert internal invariant failures into routine tagged errors just because `Result` is available.

## Compatibility Edges

If the public API still throws or rejects, keep that behavior as a thin adapter over result-native internals.

Do:

```ts
async function getUser(id: string): Promise<User> {
  return Result.unwrap(await getUserResult(id));
}
```

Do not:

```ts
async function getUserResult(id: string): Promise<User> {
  const user = Result.unwrap(await fetchUserResult(id));
  return user;
}
```

The inner function should stay result-native. The outer compatibility edge unwraps.

## Review Checklist

- Does the function signature now show expected failures explicitly?
- Is `Result.try` or `Result.tryPromise` used only at throw or reject boundaries?
- Is `Result.gen(...)` simplifying a real multi-step flow rather than adding ceremony?
- Are tagged errors narrow and meaningful?
- Are defects still allowed to throw?
- Are there direct tests for the result-returning API?
- If a compatibility API remains, do tests still cover its throw or reject behavior?

## Avoid

- Wrapping already-result-native functions in `try`.
- Re-throwing inside `Result` functions instead of returning `Result.err(...)`.
- Using matching where a simple guard is clearer.
- Exporting `better-result` implementation details from public APIs unless that API is intentionally Result-native.

## References

- Official best practices: `https://better-result.dev/advanced/best-practices`
- AI agent guidance: `https://better-result.dev/guides/ai-agents`
