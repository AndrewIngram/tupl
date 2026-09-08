import { Result, type Result as BetterResult } from "better-result";

import type { ConstraintValidationOptions } from "../constraints";
import { executeFilterResult, executeJoinResult } from "./local-filter-join";
import { executeAggregateResult, executeProjectResult } from "./local-projection-aggregation";
import { executeValuesResult } from "./local-values";
import {
  executeLimitOffsetResult,
  executeRepeatUnionResult,
  executeSetOpResult,
  executeSortResult,
  executeWithResult,
} from "./local-ordering-cte";
import { tryExecuteRemoteSubtreeResult } from "./remote-subtree";
import { executeCteRefResult, executeScanResult } from "./scan-execution";
import { prepareSubqueryResultsResult } from "./subquery-preparation";
import { executeWindowResult } from "./window-execution";
import type { InternalRow } from "./row-ops";
import {
  TuplExecutionError,
  TuplGuardrailError,
  type RelNode,
  type TuplError,
} from "@tupl/foundation";
import type { ProvidersMap } from "@tupl/provider-kit";
import type { QueryRow, SchemaDefinition } from "@tupl/schema-model";
import { enforceMaterializationLimitResult } from "../policy";
import { describeRelExecution, type RelExecutionObserver } from "./execution-observer";

export interface RelExecutionGuardrails {
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}

export interface RelExecutionContext<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  guardrails: RelExecutionGuardrails;
  constraintValidation?: ConstraintValidationOptions;
  lookupBatches: number;
  cteRows: Map<string, QueryRow[]>;
  subqueryResults: Map<string, unknown>;
  observer?: RelExecutionObserver;
}

export type RelExecutionResult = BetterResult<QueryRow[] | InternalRow[], TuplError>;

export type RemoteExecutionResult = BetterResult<QueryRow[] | null, TuplError>;

function toTuplExecutionError(error: unknown, operation: string) {
  if (TuplExecutionError.is(error) || TuplGuardrailError.is(error)) {
    return error;
  }

  return new TuplExecutionError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    cause: error,
  });
}

export function tryExecutionStep<T>(operation: string, fn: () => T) {
  return Result.try({
    try: () => fn() as Awaited<T>,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

export async function tryExecutionStepAsync<T>(operation: string, fn: () => Promise<T>) {
  return Result.tryPromise({
    try: fn,
    catch: (error) => toTuplExecutionError(error, operation),
  });
}

export async function executeRelLocallyResult<TContext>(
  rel: RelNode,
  executionContext: RelExecutionContext<TContext>,
): Promise<BetterResult<QueryRow[], TuplError>> {
  // Scalar and EXISTS subqueries are prepared once up front so downstream node execution can treat
  // them as memoized inputs instead of recursively re-running subtrees at each expression use site.
  const subqueryPrepResult = await prepareSubqueryResultsResult(rel, executionContext);
  if (Result.isError(subqueryPrepResult)) {
    return subqueryPrepResult;
  }

  const rowsResult = await executeRelNodeResult(rel, executionContext);
  if (Result.isError(rowsResult)) {
    return rowsResult;
  }

  return Result.ok(rowsResult.value as QueryRow[]);
}

export async function executeRelNodeResult<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<RelExecutionResult> {
  const observation = context.observer?.start(node, describeRelExecution(node));

  try {
    // Each subtree gets a remote-execution chance first so provider pushdown can absorb work even
    // after higher-level orchestration has decided the full query cannot stay remote end to end.
    const remoteRowsResult = await tryExecuteRemoteSubtreeResult(node, context, observation);
    if (Result.isError(remoteRowsResult)) {
      observation?.fail(remoteRowsResult.error);
      return remoteRowsResult;
    }
    if (remoteRowsResult.value) {
      const limitedResult = enforceMaterializationLimitResult(
        remoteRowsResult.value,
        context.guardrails,
      );
      if (Result.isError(limitedResult)) {
        observation?.fail(limitedResult.error);
        return limitedResult;
      }
      observation?.complete(limitedResult.value);
      return limitedResult;
    }

    const result = await (async () => {
      switch (node.kind) {
        case "scan":
          return executeScanResult(node, context);
        case "values":
          return executeValuesResult(node, context);
        case "cte_ref":
          return executeCteRefResult(node, context);
        case "correlate":
          return Result.err(
            new TuplExecutionError({
              operation: "execute relational node",
              message: "Correlate nodes must be decorrelated before execution.",
            }),
          );
        case "join":
          return executeJoinResult(node, context, observation);
        case "filter":
          return executeFilterResult(node, context);
        case "project":
          return executeProjectResult(node, context);
        case "aggregate":
          return executeAggregateResult(node, context);
        case "window":
          return executeWindowResult(node, context);
        case "sort":
          return executeSortResult(node, context);
        case "limit_offset":
          return executeLimitOffsetResult(node, context);
        case "set_op":
          return executeSetOpResult(node, context);
        case "with":
          return executeWithResult(node, context);
        case "repeat_union":
          return executeRepeatUnionResult(node, context);
      }
    })();

    if (Result.isError(result)) {
      observation?.fail(result.error);
      return result;
    }
    const limitedResult = enforceMaterializationLimitResult(result.value, context.guardrails);
    if (Result.isError(limitedResult)) {
      observation?.fail(limitedResult.error);
      return limitedResult;
    }
    observation?.complete(limitedResult.value);
    return limitedResult;
  } catch (error) {
    observation?.fail(error);
    throw error;
  }
}
