import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import type { ProviderAdapter } from "@tupl/provider-kit";
import type { ProviderRelTarget } from "@tupl/planner";
import type { QueryRow } from "@tupl/schema-model";

import type { QueryGuardrails, TuplDiagnostic } from "../contracts";
import { unwrapQueryResult } from "../diagnostics";
import type {
  QueryExecutionPlan,
  QuerySession,
  QuerySessionInput,
  QueryStepEvent,
  QueryStepState,
} from "../session/contracts";
import { createSessionEventBuffer } from "../session/session-event-buffer";
import { createSingleFlightExecution } from "../session/single-flight-execution";
import { withTimeoutResult } from "./provider-execution";
import { runProviderFragmentOnceResult } from "./provider-fragment-replay";
import {
  createInitialProviderFragmentState,
  createProviderFragmentPlan,
} from "./provider-session-lifecycle";

/** Provider-fragment sessions expose one measured remote invocation. */
export function createProviderFragmentSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  provider: ProviderAdapter<TContext>,
  providerName: string,
  fragment: ProviderRelTarget,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QuerySession {
  const plan: QueryExecutionPlan = createProviderFragmentPlan(providerName, fragment, diagnostics);
  let state: QueryStepState = createInitialProviderFragmentState(
    providerName,
    fragment.rel.id,
    diagnostics,
  );
  const events = createSessionEventBuffer<QueryStepEvent>();

  const execution = createSingleFlightExecution(async () => {
    const executionId = "execution_1";
    const occurrence = 1;
    const startedAt = Date.now();
    state = {
      id: "remote_fragment_1",
      relNodeId: fragment.rel.id,
      executionId,
      occurrence,
      kind: "remote_fragment" as const,
      status: "running",
      summary: `Execute provider fragment (${providerName})`,
      dependsOn: [],
      startedAt,
      routeUsed: "provider_fragment" as const,
      ...(diagnostics.length > 0 ? { diagnostics } : {}),
    };

    const rowsResult = await withTimeoutResult(
      "execute provider fragment",
      () =>
        runProviderFragmentOnceResult({
          provider,
          fragment,
          rel,
          sessionInput: input,
          maxExecutionRows: guardrails.maxExecutionRows,
        }).then(unwrapQueryResult),
      guardrails.timeoutMs,
    );
    const endedAt = Date.now();
    const eventBase = {
      id: state.id,
      executionId,
      stepId: state.id,
      relNodeId: fragment.rel.id,
      occurrence,
      kind: "remote_fragment" as const,
      summary: state.summary,
      dependsOn: [],
      executionIndex: 1,
      startedAt,
      endedAt,
      durationMs: Math.max(endedAt - startedAt, 0),
      routeUsed: "provider_fragment" as const,
      ...(diagnostics.length > 0 ? { diagnostics } : {}),
    };
    const event: QueryStepEvent = Result.isError(rowsResult)
      ? {
          ...eventBase,
          status: "failed",
          error: rowsResult.error.message,
        }
      : {
          ...eventBase,
          status: "done",
          rowCount: rowsResult.value.length,
          outputRowCount: rowsResult.value.length,
          ...(input.options?.captureRows === "full" ? { rows: rowsResult.value } : {}),
        };
    state = event;
    events.publish(event);
    events.close();
    return rowsResult;
  });

  const run = async (): Promise<QueryRow[]> => unwrapQueryResult(await execution.runResult());
  let nextRequest = Promise.resolve();

  return {
    getPlan: () => plan,
    next: () => {
      const resultPromise = execution.runResult();
      const request = nextRequest.then(async () => {
        const event = await events.take();
        if (event) {
          input.options?.onEvent?.(event);
          return event;
        }
        return {
          done: true as const,
          result: unwrapQueryResult(await resultPromise),
        };
      });
      nextRequest = request.then(
        () => undefined,
        () => undefined,
      );
      return request;
    },
    runToCompletion: run,
    getResult: () => {
      const outcome = execution.getOutcome();
      return outcome?.kind === "succeeded" ? outcome.value : null;
    },
    getStepState: (id: string) => (id === state.id ? state : undefined),
  };
}
