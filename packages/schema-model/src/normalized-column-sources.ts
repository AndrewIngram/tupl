import type { NormalizedColumnBinding, NormalizedPhysicalTableBinding } from "./types";

/**
 * Normalized column sources own lookup and source-map helpers for normalized bindings.
 */
export function getNormalizedColumnBindings(
  binding: Pick<
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
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
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
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
    | NormalizedPhysicalTableBinding
    | Extract<import("./types").NormalizedTableBinding, { kind: "view" }>,
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
