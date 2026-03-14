import type { WindowClauseEntryAst, WindowSpecificationAst } from "../sqlite-parser/ast";
import type { RelWindowFrame } from "@tupl/foundation";

/**
 * Window specifications own named-window resolution and supported frame parsing.
 */
export function parseNamedWindowSpecifications(
  entries: WindowClauseEntryAst[] | undefined,
): Map<string, WindowSpecificationAst> {
  const out = new Map<string, WindowSpecificationAst>();
  for (const entry of entries ?? []) {
    const spec = entry.as_window_specification?.window_specification;
    if (!spec) {
      continue;
    }
    out.set(entry.name, spec);
  }
  return out;
}

export function parseWindowOver(
  over: unknown,
  windowDefinitions: Map<string, WindowSpecificationAst>,
): WindowSpecificationAst | null {
  if (!over || typeof over !== "object") {
    return null;
  }

  const rawSpec = (over as { as_window_specification?: unknown }).as_window_specification;
  if (!rawSpec) {
    return null;
  }

  if (typeof rawSpec === "string") {
    const resolved = windowDefinitions.get(rawSpec);
    if (!resolved) {
      return null;
    }
    return resolved;
  }

  if (typeof rawSpec !== "object") {
    return null;
  }

  const spec = (rawSpec as { window_specification?: unknown }).window_specification;
  if (!spec || typeof spec !== "object") {
    return null;
  }
  return spec as WindowSpecificationAst;
}

export function parseWindowFrameClause(raw: string | undefined): RelWindowFrame | null {
  if (!raw) {
    return null;
  }

  const normalized = raw.replace(/\s+/g, " ").trim().toUpperCase();
  switch (normalized) {
    case "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW":
      return {
        mode: "rows",
        start: "unbounded_preceding",
        end: "current_row",
      };
    case "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING":
      return {
        mode: "rows",
        start: "unbounded_preceding",
        end: "unbounded_following",
      };
    case "ROWS BETWEEN CURRENT ROW AND CURRENT ROW":
      return {
        mode: "rows",
        start: "current_row",
        end: "current_row",
      };
    case "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW":
      return {
        mode: "range",
        start: "unbounded_preceding",
        end: "current_row",
      };
    case "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING":
      return {
        mode: "range",
        start: "unbounded_preceding",
        end: "unbounded_following",
      };
    default:
      return null;
  }
}
