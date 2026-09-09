import type {
  NormalizedColumnBinding,
  NormalizedPhysicalTableBinding,
  NormalizedTableBinding,
  NormalizedSourceColumnBinding,
} from "../contracts/normalized-contracts";
import type { RelExpr, RelLocalOperation } from "@tupl/foundation";
import { registerLocalOperation } from "../dsl/derive";
import { normalizeProviderRowValue } from "../mapping/row-coercion";

const sourceComputations = new WeakMap<NormalizedSourceColumnBinding, RelLocalOperation>();

/** Source coercion is a value computation; standalone row mapping still uses the source binding. */
export function sourceColumnValueExpression(
  binding: NormalizedSourceColumnBinding,
  input: RelExpr,
): RelExpr {
  if (!binding.coerce) return input;
  let operation = sourceComputations.get(binding);
  if (!operation) {
    operation = registerLocalOperation(`coerce ${binding.source}`, {
      dependencies: {
        value: {
          kind: "dsl_calculated_column",
          expr: input,
          definition: binding.definition ?? "json",
        },
      },
      evaluate: ({ value }) => normalizeProviderRowValue(value, binding),
    });
    sourceComputations.set(binding, operation);
  }
  return { kind: "local", operation, args: [input] };
}

/**
 * Normalized column sources own lookup and source-map helpers for normalized bindings.
 */
export function getNormalizedColumnBindings(
  binding: Pick<
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
): Record<string, NormalizedColumnBinding> {
  if (binding.columnBindings && Object.keys(binding.columnBindings).length > 0) {
    return binding.columnBindings;
  }

  return Object.fromEntries(
    Object.entries(binding.columnToSource).map(([column, source]) => [
      column,
      { kind: "source", source },
    ]),
  );
}

export function getNormalizedColumnSourceMap(
  binding: Pick<
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
): Record<string, string> {
  const entries = Object.entries(getNormalizedColumnBindings(binding)).flatMap(
    ([column, columnBinding]) =>
      columnBinding.kind === "source" ? [[column, columnBinding] as const] : [],
  );
  return Object.fromEntries(
    entries.map(([column, columnBinding]) => [column, columnBinding.source]),
  );
}

export function resolveNormalizedColumnSource(
  binding: Pick<
    NormalizedPhysicalTableBinding | Extract<NormalizedTableBinding, { kind: "view" }>,
    "columnBindings" | "columnToSource"
  >,
  logicalColumn: string,
): string {
  const bindingByColumn = getNormalizedColumnBindings(binding)[logicalColumn];
  return bindingByColumn?.kind === "source" ? bindingByColumn.source : logicalColumn;
}

export function buildColumnSourceMapFromBindings(
  columnBindings: Record<string, NormalizedColumnBinding>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(columnBindings).flatMap(([column, binding]) =>
      binding.kind === "source" ? [[column, binding.source] as const] : [],
    ),
  );
}
