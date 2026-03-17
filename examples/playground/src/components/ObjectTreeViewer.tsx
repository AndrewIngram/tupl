import { ChevronDown, ChevronRight } from "lucide-react";
import { useState } from "react";
import type React from "react";

import { cn } from "../lib/utils";

const DEFAULT_EXPAND_DEPTH = 1;
const MAX_INLINE_ARRAY_ITEMS = 4;
const MAX_INLINE_OBJECT_ITEMS = 3;
const MAX_EXPANDABLE_PREVIEW_LENGTH = 56;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return Object.prototype.toString.call(value) === "[object Object]";
}

function isExpandableValue(value: unknown): value is Record<string, unknown> | unknown[] {
  return Array.isArray(value) || isPlainObject(value);
}

function compareObjectKeys(left: string, right: string): number {
  if (left === right) {
    return 0;
  }

  if (left === "kind") {
    return -1;
  }

  if (right === "kind") {
    return 1;
  }

  return left.localeCompare(right);
}

function getObjectPrimaryLabel(value: Record<string, unknown>, childCount: number): string {
  return typeof value.kind === "string" ? `${value.kind}(${childCount})` : `Object(${childCount})`;
}

function getValueClassName(value: unknown): string {
  if (value === null) {
    return "object-tree-viewer__value--null";
  }

  switch (typeof value) {
    case "boolean":
      return "object-tree-viewer__value--boolean";
    case "number":
    case "bigint":
      return "object-tree-viewer__value--number";
    case "string":
      return "object-tree-viewer__value--string";
    case "undefined":
      return "object-tree-viewer__value--undefined";
    case "function":
      return "object-tree-viewer__value--function";
    case "object":
      return Array.isArray(value)
        ? "object-tree-viewer__value--array"
        : "object-tree-viewer__value--object";
    default:
      return "object-tree-viewer__value--default";
  }
}

function formatPrimitiveValue(value: unknown): string {
  if (typeof value === "string") {
    return JSON.stringify(value);
  }

  if (typeof value === "bigint") {
    return `${value}n`;
  }

  if (typeof value === "function") {
    return value.name ? `[Function ${value.name}]` : "[Function]";
  }

  if (typeof value === "symbol") {
    return value.toString();
  }

  return String(value);
}

function summarizeValue(value: unknown): string {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "[]";
    }

    const inlineItems = value
      .slice(0, MAX_INLINE_ARRAY_ITEMS)
      .map((item) => (isExpandableValue(item) ? summarizeValue(item) : formatPrimitiveValue(item)));
    const remainder = value.length - inlineItems.length;
    return `[${inlineItems.join(", ")}${remainder > 0 ? `, +${remainder} more` : ""}]`;
  }

  if (isPlainObject(value)) {
    const entries = Object.entries(value);
    if (entries.length === 0) {
      return "{}";
    }

    const inlineItems = entries
      .sort(([left], [right]) => compareObjectKeys(left, right))
      .slice(0, MAX_INLINE_OBJECT_ITEMS)
      .map(([key, childValue]) => {
        const renderedValue = isExpandableValue(childValue)
          ? Array.isArray(childValue)
            ? `Array(${childValue.length})`
            : getObjectPrimaryLabel(childValue, Object.keys(childValue).length)
          : formatPrimitiveValue(childValue);
        return `${key}: ${renderedValue}`;
      });
    const remainder = entries.length - inlineItems.length;
    return `{ ${inlineItems.join(", ")}${remainder > 0 ? `, +${remainder} more` : ""} }`;
  }

  return formatPrimitiveValue(value);
}

function truncatePreview(value: string): string {
  if (value.length <= MAX_EXPANDABLE_PREVIEW_LENGTH) {
    return value;
  }

  return `${value.slice(0, MAX_EXPANDABLE_PREVIEW_LENGTH - 1)}…`;
}

function getChildEntries(value: Record<string, unknown> | unknown[]): Array<[string, unknown]> {
  if (Array.isArray(value)) {
    return value.map((child, index) => [String(index), child]);
  }

  return Object.entries(value).sort(([left], [right]) => compareObjectKeys(left, right));
}

function ObjectTreeNode({
  label,
  value,
  depth,
}: {
  label?: string;
  value: unknown;
  depth: number;
}) {
  const expandable = isExpandableValue(value);
  const childEntries = expandable ? getChildEntries(value) : [];
  const canExpand = childEntries.length > 0;
  const [expanded, setExpanded] = useState(depth < DEFAULT_EXPAND_DEPTH);
  const fullPreview = expandable
    ? Array.isArray(value)
      ? `Array(${value.length}) ${summarizeValue(value)}`
      : `${getObjectPrimaryLabel(value, childEntries.length)} ${summarizeValue(value)}`
    : summarizeValue(value);
  const preview = expandable ? truncatePreview(fullPreview) : fullPreview;

  return (
    <div>
      <div className="flex w-full min-w-0 items-start gap-1 font-mono text-xs leading-5">
        {canExpand ? (
          <button
            type="button"
            aria-label={expanded ? "Collapse value" : "Expand value"}
            className="mt-0.5 flex h-4 w-4 items-center justify-center rounded-sm text-slate-500 hover:bg-slate-200 hover:text-slate-700"
            onClick={() => setExpanded((open) => !open)}
          >
            {expanded ? (
              <ChevronDown className="h-3.5 w-3.5" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5" />
            )}
          </button>
        ) : (
          <span className="mt-0.5 block h-4 w-4 shrink-0" />
        )}

        <div className="w-0 min-w-0 flex-1 overflow-hidden">
          <div className="flex w-full min-w-0 items-start gap-1 overflow-hidden">
            {label ? (
              <>
                <span className="shrink-0 text-slate-800">{label}</span>
                <span className="shrink-0 text-slate-400">=</span>
              </>
            ) : null}
            <span
              className={cn(
                "block w-0 min-w-0 max-w-full flex-1",
                expandable ? "block overflow-hidden text-ellipsis whitespace-nowrap" : "break-all",
                getValueClassName(value),
              )}
              title={expandable ? fullPreview : undefined}
            >
              {preview}
            </span>
          </div>
        </div>
      </div>

      {canExpand && expanded ? (
        <div className="ml-[0.45rem] border-l border-slate-200 pl-4">
          {childEntries.map(([childLabel, childValue]) => (
            <ObjectTreeNode
              key={`${depth}:${childLabel}`}
              label={childLabel}
              value={childValue}
              depth={depth + 1}
            />
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function ObjectTreeViewer({
  value,
  className,
}: {
  value: unknown;
  className?: string;
}): React.JSX.Element {
  return (
    <div
      className={cn(
        "object-tree-viewer w-full min-w-0 max-w-full rounded-md bg-white p-3",
        className,
      )}
    >
      {isExpandableValue(value) ? (
        <div className="space-y-0.5">
          {getChildEntries(value).length === 0 ? (
            <div className={cn("font-mono text-xs", getValueClassName(value))}>
              {summarizeValue(value)}
            </div>
          ) : (
            getChildEntries(value).map(([label, childValue]) => (
              <ObjectTreeNode key={label} label={label} value={childValue} depth={0} />
            ))
          )}
        </div>
      ) : (
        <div className={cn("font-mono text-xs", getValueClassName(value))}>
          {summarizeValue(value)}
        </div>
      )}
    </div>
  );
}
