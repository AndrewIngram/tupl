import { Result } from "better-result";

import type { RelNode } from "@tupl/foundation";

import { enforceMaterializationLimitResult } from "../policy";
import type { RelExecutionContext, RelExecutionResult } from "./local-execution";

/**
 * Values execution owns in-memory literal row materialization for SELECT cores without backing scans.
 */
export function executeValuesResult<TContext>(
  values: Extract<RelNode, { kind: "values" }>,
  context: RelExecutionContext<TContext>,
): RelExecutionResult {
  const sizeResult = enforceMaterializationLimitResult(values.rows, context.guardrails);
  if (Result.isError(sizeResult)) {
    return sizeResult;
  }

  return Result.ok(
    values.rows.map((row) =>
      Object.fromEntries(values.output.map((column, index) => [column.name, row[index] ?? null])),
    ),
  );
}
