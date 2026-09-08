import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";
import type { QueryRow } from "@tupl/schema-model";

import type { QueryGuardrails, TuplDiagnostic } from "../contracts";
import { unwrapQueryResult } from "../diagnostics";
import { executeRelWithProvidersResult } from "../executor";
import { buildRelExecutionPlan } from "../execution/execution-plan-builder";
import { withTimeoutResult } from "../provider/provider-execution";
import type { QuerySession, QuerySessionInput, QueryStepEvent, QueryStepState } from "./contracts";
import { createSessionExecutionObserver } from "./execution-observation";
import { createSessionEventBuffer } from "./session-event-buffer";
import { createSingleFlightExecution } from "./single-flight-execution";

/**
 * Rel-execution sessions expose actual node completions while retaining one query execution.
 */
export function createRelExecutionSession<TContext>(
  input: QuerySessionInput<TContext>,
  guardrails: QueryGuardrails,
  rel: RelNode,
  diagnostics: TuplDiagnostic[] = [],
): QuerySession {
  const plan = buildRelExecutionPlan(input, rel, diagnostics);
  const states = new Map<string, QueryStepState>(
    plan.steps.map((step) => [
      step.id,
      {
        id: step.id,
        ...(step.relNodeId ? { relNodeId: step.relNodeId } : {}),
        kind: step.kind,
        status: "ready",
        summary: step.summary,
        dependsOn: step.dependsOn,
        ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
      },
    ]),
  );
  const events = createSessionEventBuffer<QueryStepEvent>();
  const observer = createSessionExecutionObserver({
    plan,
    states,
    rootRelNodeId: rel.id,
    captureRows: input.options?.captureRows,
    publish: events.publish,
  });

  const execution = createSingleFlightExecution(async () => {
    const rowsResult = await withTimeoutResult(
      "execute relational query",
      () =>
        executeRelWithProvidersResult(
          rel,
          input.preparedSchema.schema,
          input.preparedSchema.providers,
          input.context,
          {
            maxExecutionRows: guardrails.maxExecutionRows,
            maxLookupKeysPerBatch: guardrails.maxLookupKeysPerBatch,
            maxLookupBatches: guardrails.maxLookupBatches,
          },
          {
            ...(input.constraintValidation
              ? { constraintValidation: input.constraintValidation }
              : {}),
            observer,
          },
        ).then(unwrapQueryResult),
      guardrails.timeoutMs,
    );

    observer.close(Result.isError(rowsResult) ? rowsResult.error : undefined);
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
    getStepState: (id: string) => states.get(id),
  };
}
