import { Result, type Result as BetterResult } from "better-result";

import {
  ProviderFragmentBuildError,
  isRelProjectColumnMapping,
  type RelNode,
  type RelColumnRef,
  type RelProjectNode,
} from "@tupl/foundation";
import type { SchemaDefinition } from "@tupl/schema-model";

import {
  collectAliasToSourceMappings,
  mapColumnNameForAlias,
  mapColumnRefForAlias,
  mapRelExprRefsForAliasSource,
} from "./provider-alias-mapping";
import { normalizeScanForProvider } from "./provider-scan-normalization";

/**
 * Provider rel normalization owns full rel-tree rewriting into provider-facing column/source space.
 */
export function normalizeRelForProvider(
  node: RelNode,
  schema: SchemaDefinition,
): BetterResult<RelNode, ProviderFragmentBuildError> {
  const aliasToSource = collectAliasToSourceMappings(node, schema);

  const visit = (current: RelNode): BetterResult<RelNode, ProviderFragmentBuildError> => {
    switch (current.kind) {
      case "values":
      case "cte_ref":
        return Result.ok(current);
      case "scan":
        return normalizeScanForProvider(current, schema);
      case "filter":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
            ...(current.where
              ? {
                  where: current.where.map((clause) => ({
                    ...clause,
                    column: mapColumnNameForAlias(clause.column, aliasToSource),
                  })),
                }
              : {}),
            ...(current.expr
              ? {
                  expr: mapRelExprRefsForAliasSource(current.expr, aliasToSource),
                }
              : {}),
          });
        });
      case "project":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
            columns: current.columns.map((column) =>
              isRelProjectColumnMapping(column)
                ? {
                    ...column,
                    source: mapColumnRefForAlias(column.source, aliasToSource),
                  }
                : {
                    ...column,
                    expr: mapRelExprRefsForAliasSource(column.expr, aliasToSource),
                  },
            ),
          });
        });
      case "correlate":
        return Result.gen(function* () {
          const left = yield* visit(current.left);
          const right = yield* visit(current.right);
          return Result.ok({
            ...current,
            left,
            right,
            correlation: {
              outer: mapColumnRefForAlias(current.correlation.outer, aliasToSource),
              inner: mapColumnRefForAlias(current.correlation.inner, aliasToSource),
            },
            apply:
              current.apply.kind === "scalar_filter"
                ? {
                    ...current.apply,
                    outerCompare: mapColumnRefForAlias(current.apply.outerCompare, aliasToSource),
                  }
                : current.apply,
          });
        });
      case "join":
        return Result.gen(function* () {
          const left = yield* visit(current.left);
          const right = yield* visit(current.right);
          return Result.ok({
            ...current,
            left,
            right,
            leftKey: mapColumnRefForAlias(current.leftKey, aliasToSource),
            rightKey: mapColumnRefForAlias(current.rightKey, aliasToSource),
          });
        });
      case "aggregate":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
            groupBy: current.groupBy.map((column) => mapColumnRefForAlias(column, aliasToSource)),
            metrics: current.metrics.map((metric) => ({
              ...metric,
              ...(metric.column
                ? { column: mapColumnRefForAlias(metric.column, aliasToSource) }
                : {}),
            })),
          });
        });
      case "window":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
            functions: current.functions.map((fn) => ({
              ...fn,
              partitionBy: fn.partitionBy.map((column) =>
                mapColumnRefForAlias(column, aliasToSource),
              ),
              orderBy: fn.orderBy.map((term) => ({
                ...term,
                source: mapColumnRefForAlias(term.source, aliasToSource),
              })),
              ...("column" in fn && fn.column
                ? { column: mapColumnRefForAlias(fn.column, aliasToSource) }
                : {}),
              ...("value" in fn
                ? { value: mapRelExprRefsForAliasSource(fn.value, aliasToSource) }
                : {}),
              ...("defaultExpr" in fn && fn.defaultExpr
                ? { defaultExpr: mapRelExprRefsForAliasSource(fn.defaultExpr, aliasToSource) }
                : {}),
            })),
          });
        });
      case "sort":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
            orderBy: current.orderBy.map((term) => ({
              ...term,
              source: mapColumnRefForAlias(term.source, aliasToSource),
            })),
          });
        });
      case "limit_offset":
        return Result.gen(function* () {
          const input = yield* visit(current.input);
          return Result.ok({
            ...current,
            input,
          });
        });
      case "set_op":
        return Result.gen(function* () {
          const left = yield* visit(current.left);
          const right = yield* visit(current.right);
          return Result.ok({
            ...current,
            left,
            right,
          });
        });
      case "with":
        return Result.gen(function* () {
          const ctes = [];
          for (const cte of current.ctes) {
            ctes.push({
              ...cte,
              query: yield* visit(cte.query),
            });
          }
          const body = yield* visit(current.body);
          return Result.ok({
            ...current,
            ctes,
            body,
          });
        });
      case "repeat_union":
        return Result.ok(current);
    }
  };

  return Result.gen(function* () {
    const visited = yield* visit(node);
    return Result.ok(simplifyProviderProjects(visited));
  });
}

function simplifyProviderProjects(node: RelNode): RelNode {
  switch (node.kind) {
    case "values":
    case "cte_ref":
      return node;
    case "scan":
      return node;
    case "filter":
      return { ...node, input: simplifyProviderProjects(node.input) };
    case "project":
      return hoistProjectAcrossUnaryChain({
        ...node,
        input: simplifyProviderProjects(node.input),
      });
    case "aggregate":
    case "window":
    case "sort":
    case "limit_offset":
      return {
        ...node,
        input: simplifyProviderProjects(node.input),
      };
    case "correlate":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
      };
    case "join":
    case "set_op":
      return {
        ...node,
        left: simplifyProviderProjects(node.left),
        right: simplifyProviderProjects(node.right),
      };
    case "with":
      return {
        ...node,
        ctes: node.ctes.map((cte) => ({
          ...cte,
          query: simplifyProviderProjects(cte.query),
        })),
        body: simplifyProviderProjects(node.body),
      };
    case "repeat_union":
      return node;
  }
}

function hoistProjectAcrossUnaryChain(project: RelProjectNode): RelNode {
  const unaryChain: Array<Extract<RelNode, { kind: "filter" | "sort" | "limit_offset" }>> = [];
  let current = project.input;

  while (current.kind === "filter" || current.kind === "sort" || current.kind === "limit_offset") {
    unaryChain.push(current);
    current = current.input;
  }

  if (current.kind !== "project") {
    return project;
  }

  const mergedColumns = composeProjectMappings(project.columns, current.columns);
  if (!mergedColumns) {
    return project;
  }

  const remappedChain = unaryChain.map((unary) => remapUnaryProjectRefs(unary, current));
  if (remappedChain.some((unary) => unary === null)) {
    // Computed sort keys cannot move below their projection. If the outer projection
    // retains those values, merge below the chain and rename its ordering references.
    let rebuilt: RelNode = { ...project, input: current.input, columns: mergedColumns };
    for (let index = unaryChain.length - 1; index >= 0; index -= 1) {
      const unary = unaryChain[index]!;
      if (unary.kind === "filter") return project;
      if (unary.kind === "limit_offset") {
        rebuilt = { ...unary, input: rebuilt, output: project.output };
        continue;
      }
      const orderBy = [];
      for (const term of unary.orderBy) {
        if (term.source.alias || term.source.table) return project;
        const retained = project.columns.find(
          (column) =>
            isRelProjectColumnMapping(column) && column.source.column === term.source.column,
        );
        if (!retained) return project;
        orderBy.push({ ...term, source: { column: retained.output } });
      }
      rebuilt = { ...unary, input: rebuilt, orderBy, output: project.output };
    }
    return rebuilt;
  }

  let rebuiltInput: RelNode = current.input;
  for (let index = remappedChain.length - 1; index >= 0; index -= 1) {
    const unary = remappedChain[index];
    if (!unary) {
      continue;
    }
    rebuiltInput = {
      ...unary,
      input: rebuiltInput,
      output: rebuiltInput.output,
    };
  }

  return {
    ...project,
    input: rebuiltInput,
    columns: mergedColumns,
  };
}

/** A removed projection also changes the namespace of every operation crossed above it. */
function remapUnaryProjectRefs(
  unary: Extract<RelNode, { kind: "filter" | "sort" | "limit_offset" }>,
  project: RelProjectNode,
) {
  const sourceFor = (ref: RelColumnRef) => {
    const qualifier = ref.alias ?? ref.table;
    const name = qualifier ? `${qualifier}.${ref.column}` : ref.column;
    const mapping = project.columns.find((column) => column.output === name);
    return mapping && isRelProjectColumnMapping(mapping) ? mapping.source : undefined;
  };
  if (unary.kind === "limit_offset") return unary;
  if (unary.kind === "sort") {
    const orderBy = [];
    for (const term of unary.orderBy) {
      const source = sourceFor(term.source);
      if (!source) return null;
      orderBy.push({ ...term, source });
    }
    return { ...unary, orderBy };
  }
  // Expression predicates may contain computed values or subqueries: retain their projection boundary.
  if (unary.expr) return null;
  const where = [];
  for (const clause of unary.where ?? []) {
    const source = sourceFor({ column: clause.column });
    if (!source) return null;
    const qualifier = source.alias ?? source.table;
    where.push({ ...clause, column: qualifier ? `${qualifier}.${source.column}` : source.column });
  }
  return { ...unary, where };
}

function composeProjectMappings(
  outer: RelProjectNode["columns"],
  inner: RelProjectNode["columns"],
): RelProjectNode["columns"] | null {
  const innerByOutput = new Map(inner.map((mapping) => [mapping.output, mapping] as const));
  const merged: RelProjectNode["columns"] = [];

  for (const mapping of outer) {
    if (!isRelProjectColumnMapping(mapping) || mapping.source.alias || mapping.source.table) {
      return null;
    }

    const innerMapping = innerByOutput.get(mapping.source.column);
    if (!innerMapping) {
      return null;
    }

    if (isRelProjectColumnMapping(innerMapping)) {
      merged.push({
        kind: "column",
        source: innerMapping.source,
        output: mapping.output,
      });
      continue;
    }

    merged.push({
      kind: "expr",
      expr: innerMapping.expr,
      output: mapping.output,
    });
  }

  return merged;
}
