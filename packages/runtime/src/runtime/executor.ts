import { Result, type Result as BetterResult } from "better-result";

import type { ConstraintValidationOptions } from "./constraints";
import { executeRelLocallyResult, type RelExecutionContext } from "./execution/local-execution";
import { type RelNode, type TuplError } from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";
import { validateExecutionGuardrailsResult } from "./policy";
import type { RelExecutionObserver } from "./execution/execution-observer";

/**
 * Executor is the stable public subpath for runtime execution helpers.
 * It owns the top-level execution entrypoint and delegates lower-level node evaluation.
 */
export interface RelExecutionGuardrails {
  timeoutMs?: number;
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}

export async function executeRelWithProvidersResult<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  guardrails: RelExecutionGuardrails,
  options: {
    constraintValidation?: ConstraintValidationOptions;
    observer?: RelExecutionObserver;
  } = {},
): Promise<BetterResult<QueryRow[], TuplError>> {
  const guardrailResult = validateExecutionGuardrailsResult(guardrails);
  if (Result.isError(guardrailResult)) {
    return guardrailResult;
  }

  const executionContext: RelExecutionContext<TContext> = {
    schema,
    providers,
    context,
    guardrails,
    ...(guardrails.timeoutMs && guardrails.timeoutMs > 0 && Number.isFinite(guardrails.timeoutMs)
      ? { deadline: { at: Date.now() + guardrails.timeoutMs, timeoutMs: guardrails.timeoutMs } }
      : {}),
    ...(options.constraintValidation ? { constraintValidation: options.constraintValidation } : {}),
    ...(options.observer ? { observer: options.observer } : {}),
    lookupBatches: 0,
    cteRows: new Map<string, QueryRow[]>(),
    subqueryResults: new Map<string, unknown>(),
  };

  return executeRelLocallyResult(rel, executionContext);
}
