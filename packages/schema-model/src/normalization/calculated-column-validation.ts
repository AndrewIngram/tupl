import { Result, type Result as BetterResult } from "better-result";
import type { TuplSchemaNormalizationError } from "@tupl/foundation";

import { createSchemaNormalizationError } from "../schema-errors";
import type { NormalizedColumnBinding } from "../types";
import { collectUnqualifiedExprColumns } from "./view-normalization";

/**
 * Calculated column validation owns sibling-dependency checks for normalized expr bindings.
 */
export function validateCalculatedColumnDependencies(
  tableName: string,
  columnBindings: Record<string, NormalizedColumnBinding>,
): BetterResult<void, TuplSchemaNormalizationError> {
  const exprColumns = new Set(
    Object.entries(columnBindings)
      .filter(([, binding]) => binding.kind === "expr")
      .map(([column]) => column),
  );

  for (const [columnName, binding] of Object.entries(columnBindings)) {
    if (binding.kind !== "expr") {
      continue;
    }

    for (const dependency of collectUnqualifiedExprColumns(binding.expr)) {
      if (!exprColumns.has(dependency)) {
        continue;
      }
      return Result.err(
        createSchemaNormalizationError({
          operation: "validate calculated column dependencies",
          message: `Calculated column ${tableName}.${columnName} cannot reference calculated sibling ${tableName}.${dependency} in the same columns block.`,
          table: tableName,
          column: columnName,
        }),
      );
    }
  }

  return Result.ok(undefined);
}
