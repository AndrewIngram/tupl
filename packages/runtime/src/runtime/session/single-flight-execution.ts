import { Result, type Result as BetterResult } from "better-result";

export type ExecutionOutcome<T, E> = { kind: "succeeded"; value: T } | { kind: "failed"; error: E };

/**
 * A session starts execution at most once and retains both successful and failed outcomes.
 */
export function createSingleFlightExecution<T, E>(execute: () => Promise<BetterResult<T, E>>) {
  let executionPromise: Promise<BetterResult<T, E>> | null = null;
  let outcome: ExecutionOutcome<T, E> | null = null;

  return {
    runResult() {
      executionPromise ??= Promise.resolve()
        .then(execute)
        .then((result) => {
          outcome = Result.isError(result)
            ? { kind: "failed", error: result.error }
            : { kind: "succeeded", value: result.value };
          return result;
        });
      return executionPromise;
    },
    getOutcome() {
      return outcome;
    },
  };
}
