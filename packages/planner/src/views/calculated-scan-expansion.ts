import { containsLocalExpression } from "../provider/provider-ownership";
import type { RelExpr, RelNode, RelScanNode } from "@tupl/foundation";
import { isNormalizedSourceColumnBinding } from "@tupl/schema-model/mapping";
import type { NormalizedPhysicalTableBinding } from "@tupl/schema-model/normalized";
import { getNormalizedColumnBindings } from "@tupl/schema-model/normalization";

import { nextRelId } from "../physical/planner-ids";
import type { ViewAliasColumnMap } from "../planner-types";

/**
 * Calculated scan expansion owns rewriting physical scans with calculated columns into local rels.
 */
export function hasCalculatedColumns(binding: NormalizedPhysicalTableBinding): boolean {
  return Object.values(getNormalizedColumnBindings(binding)).some(
    (columnBinding) => !isNormalizedSourceColumnBinding(columnBinding),
  );
}

export function expandCalculatedScan(
  node: RelScanNode,
  binding: NormalizedPhysicalTableBinding,
): { node: RelNode; aliases: Map<string, ViewAliasColumnMap> } | null {
  const columnBindings = getNormalizedColumnBindings(binding);
  const referencedColumns = new Set<string>(node.select);
  for (const clause of node.where ?? []) {
    referencedColumns.add(clause.column);
  }
  for (const term of node.orderBy ?? []) {
    referencedColumns.add(term.column);
  }

  const referencedCalculated = [...referencedColumns].filter((column) => {
    const columnBinding = columnBindings[column];
    return !!columnBinding && !isNormalizedSourceColumnBinding(columnBinding);
  });
  if (referencedCalculated.length === 0) {
    return null;
  }

  const requiredSourceColumns = new Set<string>();
  for (const column of referencedColumns) {
    const columnBinding = columnBindings[column];
    if (!columnBinding) {
      requiredSourceColumns.add(column);
      continue;
    }
    if (isNormalizedSourceColumnBinding(columnBinding)) {
      requiredSourceColumns.add(column);
      continue;
    }
    for (const dependency of collectExprColumns(columnBinding.expr)) {
      requiredSourceColumns.add(dependency);
    }
  }

  const alias = node.alias ?? node.table;
  const needsPrivateInputs = referencedCalculated.some((column) => {
    const entry = columnBindings[column];
    return entry?.kind === "expr" && containsLocalExpression(entry.expr);
  });
  const entity =
    needsPrivateInputs && binding.sourceHandle
      ? {
          ...binding.sourceHandle,
          columns: Object.fromEntries([
            ...Object.values(binding.sourceHandle.columns ?? {})
              .slice(0, 1)
              .map((metadata) => ["__tupl_cardinality", metadata] as const),
            ...Object.entries(columnBindings).flatMap(([name, entry]) => {
              if (entry.kind !== "source") return [];
              const raw = Object.values(binding.sourceHandle?.columns ?? {}).find(
                (metadata) => metadata.source === entry.source,
              );
              return [
                [
                  name,
                  { ...(raw ?? { type: "json" as const, nullable: true }), source: entry.source },
                ],
              ];
            }),
          ]),
        }
      : undefined;
  let current: RelNode = {
    id: node.id,
    kind: "scan",
    convention: node.convention,
    table: entity ? `__derived_source_${node.id}` : node.table,
    ...(entity ? { entity } : {}),
    alias,
    select: [...requiredSourceColumns],
    where: (node.where ?? []).filter((clause) => columnBindings[clause.column]?.kind === "source"),
    output: [...requiredSourceColumns].map((column) => ({
      name: `${alias}.${column}`,
    })),
  };

  current = {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: [...referencedColumns].map((column) => {
      const columnBinding = columnBindings[column];
      if (!columnBinding || isNormalizedSourceColumnBinding(columnBinding)) {
        return {
          kind: "column" as const,
          source: { alias, column },
          output: `${alias}.${column}`,
        };
      }
      return {
        kind: "expr" as const,
        expr: qualifyExprColumns(columnBinding.expr, alias),
        output: `${alias}.${column}`,
      };
    }),
    output: [...referencedColumns].map((column) => ({ name: `${alias}.${column}` })),
  };

  const residualWhere = (node.where ?? []).filter(
    (clause) => columnBindings[clause.column]?.kind !== "source",
  );
  if (residualWhere.length > 0) {
    current = {
      id: nextRelId("filter"),
      kind: "filter",
      convention: "local",
      input: current,
      where: residualWhere.map((clause) => ({ ...clause, column: `${alias}.${clause.column}` })),
      output: current.output,
    };
  }

  if (node.orderBy && node.orderBy.length > 0) {
    current = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: current,
      orderBy: node.orderBy.map((term) => ({
        source: { alias, column: term.column },
        direction: term.direction,
      })),
      output: current.output,
    };
  }

  if (node.limit != null || node.offset != null) {
    current = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: current,
      ...(node.limit != null ? { limit: node.limit } : {}),
      ...(node.offset != null ? { offset: node.offset } : {}),
      output: current.output,
    };
  }

  const aliasMap: ViewAliasColumnMap = Object.fromEntries(
    [...referencedColumns].map((column) => [column, { alias, column }]),
  );
  return {
    node: current,
    aliases: new Map([[alias, aliasMap]]),
  };
}

function qualifyExprColumns(expr: RelExpr, alias: string): RelExpr {
  switch (expr.kind) {
    case "literal":
      return expr;
    case "column":
      return {
        kind: "column",
        ref: {
          alias: expr.ref.alias ?? expr.ref.table ?? alias,
          column: expr.ref.column,
        },
      };
    case "local":
    case "function":
      return {
        ...expr,
        args: expr.args.map((arg) => qualifyExprColumns(arg, alias)),
      };
    case "subquery":
      return expr;
  }
}

function collectExprColumns(expr: RelExpr): Set<string> {
  const columns = new Set<string>();

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
        return;
      case "column":
        columns.add(current.ref.column);
        return;
      case "local":
      case "function":
        for (const arg of current.args) {
          visit(arg);
        }
        return;
      case "subquery":
        return;
    }
  };

  visit(expr);
  return columns;
}
