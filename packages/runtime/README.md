# `@tupl/runtime`

Query execution, guardrails, sessions, and constraint validation for `tupl`.

Most application code should prefer `@tupl/schema`; use this package for lower-level runtime integration points.

## Execution limits

`maxExecutionRows` limits each materialized row set, including intermediate local
results, provider results, combined lookup batches, and recursive CTE accumulation.
An oversized child fails even if its parent would reduce the output with `LIMIT`,
filtering, or aggregation. Exactly-at-limit results are allowed.

Row, planner-node, lookup-key, and lookup-batch limits must be positive safe integers.
The row limit measures cardinality, not cumulative rows processed or memory in bytes.
Providers return complete arrays, so runtime can reject an oversized response before
mapping it but cannot prevent allocation inside the provider. Timeout behavior does
not provide provider cancellation or preempt synchronous JavaScript execution.

## Query sessions

Use `createExecutableSchemaSession` from `@tupl/runtime/session` with a schema
returned by `createExecutableSchema`. Session creation returns a Result. Session
execution methods return promises and reject on query failure.

A session executes once. Concurrent or repeated `runToCompletion()` calls share
the same execution result or failure. `getResult()` returns `null` before successful
completion and after failure. A successful query with no rows returns `[]`.

`next()` starts the same execution and returns the oldest unread operation event
as soon as it is available. Concurrent consumers receive events in request order.
`runToCompletion()` waits for the result without consuming events. After failure,
`next()` drains recorded events, then rejects with the retained query error.

Events measure actual relational-node invocations in completion order. They include
`relNodeId`, a unique `executionId`, and a per-node `occurrence`; `stepId` is present
when the static plan contains a corresponding step. `getStepState()` reports the
latest occurrence for a static step. Planned work that never executes has no
fabricated duration or completion. Parent timings include their awaited children
and must not be summed to calculate total duration.

`captureRows: "full"` includes only the final output. Intermediate events carry
available row counts, not row snapshots. Unread event metadata remains buffered
until consumed; there is no event quota or backpressure. The executor chooses
execution order; sessions do not expose concurrency or event-order settings.

An `onEvent` callback runs when `next()` consumes an event. If it throws, that
`next()` call rejects, but the event remains consumed and the query outcome does
not change. Calling `runToCompletion()` does not invoke event-consumer callbacks.
