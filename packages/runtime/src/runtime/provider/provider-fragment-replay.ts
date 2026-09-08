import { Result, type Result as BetterResult } from "better-result";

import type { RelNode, TuplError } from "@tupl/foundation";
import { unwrapProviderOperationResult, type FragmentProviderAdapter } from "@tupl/provider-kit";
import type { ProviderRelTarget } from "@tupl/planner";
import type { QueryRow } from "@tupl/schema-model";
import { mapProviderRowsToRelOutput } from "@tupl/schema-model/mapping";

import type { QuerySessionInput } from "../session/contracts";
import { tryQueryStep, tryQueryStepAsync } from "../diagnostics";
import { enforceMaterializationLimitResult } from "../policy";

/** Provider fragment replay owns one compile, execute, limit, and mapping operation. */
export async function runProviderFragmentOnceResult<TContext>(input: {
  provider: FragmentProviderAdapter<TContext>;
  fragment: ProviderRelTarget;
  rel: RelNode;
  sessionInput: QuerySessionInput<TContext>;
  maxExecutionRows: number;
}): Promise<BetterResult<QueryRow[], TuplError>> {
  const compiledResult = await tryQueryStepAsync("compile provider fragment", async () =>
    unwrapProviderOperationResult(
      await Promise.resolve(input.provider.compile(input.fragment.rel, input.sessionInput.context)),
    ),
  );
  if (Result.isError(compiledResult)) {
    return compiledResult;
  }

  const executeRowsResult = await tryQueryStepAsync("execute provider fragment", async () =>
    unwrapProviderOperationResult(
      await input.provider.execute(compiledResult.value, input.sessionInput.context),
    ),
  );
  if (Result.isError(executeRowsResult)) {
    return executeRowsResult;
  }

  const providerRowsLimitResult = enforceMaterializationLimitResult(executeRowsResult.value, {
    maxExecutionRows: input.maxExecutionRows,
  });
  if (Result.isError(providerRowsLimitResult)) {
    return providerRowsLimitResult;
  }

  return tryQueryStep("map provider rows to logical rel output rows", () =>
    mapProviderRowsToRelOutput(
      executeRowsResult.value,
      input.rel,
      input.sessionInput.preparedSchema.schema,
    ),
  );
}
