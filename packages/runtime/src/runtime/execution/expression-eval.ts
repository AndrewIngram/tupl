import { evaluateLocalOperation } from "@tupl/schema-model/normalization";
import { Result, type Result as BetterResult } from "better-result";

import { TuplExecutionError, type RelExpr } from "@tupl/foundation";

import { evaluateScalarFunctionResult } from "./expression-scalar-functions";
import { readRowValue, toColumnKey, type InternalRow } from "./row-ops";
export { evaluateAggregateMetricResult } from "./aggregate-metric-eval";

/**
 * Expression eval owns recursive local relational expression execution and delegates scalar/aggregate semantics.
 */
export function evaluateRelExprResult(
  expr: RelExpr,
  row: InternalRow,
  subqueryResults: Map<string, unknown>,
  localValues = new Map<import("@tupl/foundation").RelLocalOperation, unknown>(),
  onCompute?: (operation: import("@tupl/foundation").RelLocalOperation) => void,
): BetterResult<unknown, TuplExecutionError> {
  switch (expr.kind) {
    case "literal":
      return Result.ok(expr.value);
    case "column":
      return Result.ok(readRowValue(row, toColumnKey(expr.ref)) ?? null);
    case "subquery":
      return Result.ok(subqueryResults.get(expr.id) ?? null);
    case "local": {
      if (localValues.has(expr.operation)) return Result.ok(localValues.get(expr.operation));
      const args: unknown[] = [];
      for (const arg of expr.args) {
        const value = evaluateRelExprResult(arg, row, subqueryResults, localValues, onCompute);
        if (Result.isError(value)) return value;
        args.push(value.value);
      }
      onCompute?.(expr.operation);
      const result = Result.try({
        try: () => evaluateLocalOperation(expr.operation, args),
        catch: (error) =>
          new TuplExecutionError({
            operation: `compute ${expr.operation.label}`,
            message: `Computation ${expr.operation.label} (${expr.operation.id}) failed: ${error instanceof Error ? error.message : String(error)}`,
            cause: error,
          }),
      });
      if (Result.isOk(result)) localValues.set(expr.operation, result.value);
      return result;
    }
    case "function": {
      const args: unknown[] = [];
      for (const arg of expr.args) {
        const argResult = evaluateRelExprResult(arg, row, subqueryResults, localValues, onCompute);
        if (Result.isError(argResult)) {
          return argResult;
        }
        args.push(argResult.value);
      }

      return evaluateScalarFunctionResult(expr.name, args);
    }
  }
}
