import { Result, type Result as BetterResult } from "better-result";

import { RelLoweringError, type RelNode } from "@tupl/foundation";
import type { CteAst, FromEntryAst, SelectAst } from "./sqlite-parser/ast";
import type { SchemaDefinition } from "@tupl/schema-model";
import { nextRelId } from "./physical/planner-ids";
import { collectTablesFromSelectAst } from "./sql-expr-lowering";
import { toRelLoweringError } from "./planner-errors";
import { tryLowerSimpleSelect } from "./simple-select-lowering";
import { parseRelColumnRef } from "./select/select-from-lowering";
import { expandSelectWildcards } from "./select/select-wildcards";
import { applyCompoundModifiers, parseSetOp } from "./select/set-op-lowering";

/**
 * Structured select lowering owns select/set-op/CTE lowering into relational nodes.
 */
export function tryLowerStructuredSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteColumns: Map<string, string[]>,
  outputNames?: string[],
): BetterResult<RelNode | null, RelLoweringError> {
  return lowerStructuredSelect(ast, schema, cteColumns, outputNames).map(
    (result) => result?.node ?? null,
  );
}

interface LoweredSelect {
  node: RelNode;
  ast: SelectAst;
}

function lowerStructuredSelect(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteColumns: Map<string, string[]>,
  outputNames?: string[],
): BetterResult<LoweredSelect | null, RelLoweringError> {
  return Result.gen(function* () {
    const normalizedAst = rewriteDerivedTables(ast);
    const scopedCteColumns = new Map(cteColumns);
    const loweredCtes: Array<{ name: string; query: RelNode }> = [];
    const withClauses = Array.isArray(normalizedAst.with) ? normalizedAst.with : [];

    for (const clause of withClauses) {
      const rawName = (clause as { name?: unknown }).name;
      const cteName =
        typeof rawName === "string"
          ? rawName
          : rawName &&
              typeof rawName === "object" &&
              typeof (rawName as { value?: unknown }).value === "string"
            ? (rawName as { value: string }).value
            : null;
      const cteAst = (clause as { stmt?: { ast?: unknown } }).stmt?.ast;
      if (!cteName || !cteAst || typeof cteAst !== "object") {
        return Result.ok(null);
      }
      if (clause.columns && new Set(clause.columns).size !== clause.columns.length) {
        return Result.err(
          new RelLoweringError({
            operation: "lower CTE column aliases",
            message: `CTE "${cteName}" column aliases must be unique.`,
          }),
        );
      }
      const visibleCteColumns = new Map(scopedCteColumns);
      const loweredCte = yield* clause.recursive && isRecursiveCteBody(cteAst as SelectAst, cteName)
        ? lowerRecursiveCte(cteName, cteAst as SelectAst, schema, visibleCteColumns, clause.columns)
        : tryLowerStructuredSelect(cteAst as SelectAst, schema, visibleCteColumns, clause.columns);
      if (!loweredCte) {
        return Result.ok(null);
      }
      if (clause.columns && clause.columns.length !== loweredCte.output.length) {
        return Result.err(
          new RelLoweringError({
            operation: "lower CTE column aliases",
            message: `CTE "${cteName}" declares ${clause.columns.length} column aliases but returns ${loweredCte.output.length} columns.`,
          }),
        );
      }
      loweredCtes.push({ name: cteName, query: loweredCte });
      scopedCteColumns.set(
        cteName,
        loweredCte.output.map((column) => column.name),
      );
    }

    const hasSetOp = typeof normalizedAst.set_op === "string" && !!normalizedAst._next;
    if (!hasSetOp) {
      const { with: _ignoredWith, ...withoutWith } = normalizedAst;
      const expandedAst = yield* Result.try({
        try: () => expandSelectWildcards(withoutWith, schema, scopedCteColumns),
        catch: (error) => toRelLoweringError(error, "expand SELECT wildcard"),
      });
      const simple = yield* tryLowerSimpleSelectWithinStructuredLowering(
        expandedAst,
        schema,
        scopedCteColumns,
        outputNames,
      );
      if (!simple) {
        return Result.ok(null);
      }

      if (loweredCtes.length === 0) {
        return Result.ok({ node: simple, ast: expandedAst });
      }

      const withNode: RelNode = {
        id: nextRelId("with"),
        kind: "with",
        convention: "local",
        ctes: loweredCtes,
        body: simple,
        output: simple.output,
      };

      return Result.ok({ node: withNode, ast: expandedAst });
    }

    const { with: _ignoredWith, ...withoutWith } = normalizedAst;
    let currentAst: SelectAst = withoutWith as SelectAst;
    const {
      set_op: _ignoredSetOp,
      _next: _ignoredNext,
      orderby: _compoundOrder,
      limit: _compoundLimit,
      ...currentBaseAst
    } = currentAst;
    const firstBranchAst = yield* Result.try({
      try: () => expandSelectWildcards(currentBaseAst, schema, scopedCteColumns),
      catch: (error) => toRelLoweringError(error, "expand SELECT wildcard"),
    });
    let currentNode: RelNode | null = yield* tryLowerSimpleSelectWithinStructuredLowering(
      firstBranchAst,
      schema,
      scopedCteColumns,
      outputNames,
    );
    if (!currentNode) {
      return Result.ok(null);
    }

    const branches = [firstBranchAst];
    while (typeof currentAst.set_op === "string" && currentAst._next) {
      const op = parseSetOp(currentAst.set_op);
      if (!op) {
        return Result.ok(null);
      }

      const {
        set_op: _ignoredRightSetOp,
        _next: _ignoredRightNext,
        ...rightBaseAst
      } = currentAst._next;
      const rightBase: LoweredSelect | null = yield* lowerStructuredSelect(
        rightBaseAst,
        schema,
        scopedCteColumns,
        currentNode.output.map((column) => column.name),
      );
      if (!rightBase) {
        return Result.ok(null);
      }
      branches.push(rightBase.ast);
      const alignedRightBase: RelNode = yield* alignRelOutputShape(
        rightBase.node,
        currentNode.output,
      );

      currentNode = {
        id: nextRelId("set_op"),
        kind: "set_op",
        convention: "local",
        op,
        left: currentNode,
        right: alignedRightBase,
        output: currentNode.output,
      };

      currentAst = currentAst._next;
    }

    currentNode = yield* applyCompoundModifiers(currentNode, normalizedAst, branches);
    if (outputNames && outputNames.length === currentNode.output.length) {
      currentNode = yield* alignRelOutputShape(
        currentNode,
        outputNames.map((name) => ({ name })),
      );
    }

    if (loweredCtes.length === 0) {
      return Result.ok({ node: currentNode, ast: firstBranchAst });
    }

    const withNode: RelNode = {
      id: nextRelId("with"),
      kind: "with",
      convention: "local",
      ctes: loweredCtes,
      body: currentNode,
      output: currentNode.output,
    };

    return Result.ok({ node: withNode, ast: firstBranchAst });
  });
}

export { collectTablesFromSelectAst };

function lowerRecursiveCte(
  cteName: string,
  ast: SelectAst,
  schema: SchemaDefinition,
  cteColumns: Map<string, string[]>,
  outputNames?: string[],
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.gen(function* () {
    if (!ast.set_op || !ast._next) {
      return Result.ok(null);
    }
    if (ast.orderby?.length || ast.limit)
      return Result.err(
        new RelLoweringError({
          operation: "lower recursive CTE",
          message: "ORDER BY and LIMIT inside a recursive CTE are not supported.",
        }),
      );

    const op = parseSetOp(ast.set_op);
    if (op !== "union" && op !== "union_all") {
      return Result.ok(null);
    }

    const { with: _ignoredWith, set_op: _ignoredSetOp, _next: _ignoredNext, ...seedAst } = ast;
    const seed = yield* tryLowerSimpleSelectWithinStructuredLowering(
      seedAst as SelectAst,
      schema,
      cteColumns,
      outputNames,
    );
    if (!seed) {
      return Result.ok(null);
    }

    const recursiveScope = new Map(cteColumns);
    recursiveScope.set(
      cteName,
      seed.output.map((column) => column.name),
    );
    const recursiveAst = rewriteDerivedTables(ast._next);
    const recursiveTerm = yield* tryLowerStructuredSelect(
      recursiveAst,
      schema,
      recursiveScope,
      seed.output.map((column) => column.name),
    );
    if (!recursiveTerm) {
      return Result.ok(null);
    }
    const alignedRecursiveTerm = yield* alignRelOutputShape(recursiveTerm, seed.output);

    const repeatUnionNode: RelNode = {
      id: nextRelId("repeat_union"),
      kind: "repeat_union",
      convention: "logical",
      cteName,
      mode: op,
      seed,
      iterative: alignedRecursiveTerm,
      output: seed.output,
    };

    return Result.ok(repeatUnionNode);
  });
}

function tryLowerSimpleSelectWithinStructuredLowering(
  ast: SelectAst,
  schema: SchemaDefinition,
  cteColumns: Map<string, string[]>,
  outputNames?: string[],
): BetterResult<RelNode | null, RelLoweringError> {
  return Result.try({
    try: () => {
      const expanded = expandSelectWildcards(ast, schema, cteColumns);
      const result = tryLowerSimpleSelect(
        expanded,
        schema,
        new Set(cteColumns.keys()),
        (subqueryAst) => {
          const subqueryResult = tryLowerStructuredSelect(subqueryAst, schema, cteColumns);
          if (Result.isError(subqueryResult)) {
            throw subqueryResult.error;
          }
          return subqueryResult.value;
        },
        (subqueryAst) => expandSelectWildcards(subqueryAst, schema, cteColumns),
        outputNames,
      );
      if (Result.isError(result)) {
        throw result.error;
      }
      return result.value;
    },
    catch: (error) => toRelLoweringError(error, "lower structured SELECT"),
  });
}

function alignRelOutputShape(
  rel: RelNode,
  output: RelNode["output"],
): BetterResult<RelNode, RelLoweringError> {
  if (rel.output.length !== output.length) {
    return Result.err(
      new RelLoweringError({
        operation: "align set operation",
        message: "Set operation branches must have the same number of columns",
      }),
    );
  }
  if (rel.output.every((column, index) => column.name === output[index]?.name)) {
    return Result.ok(rel);
  }

  return Result.ok({
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: rel,
    columns: output.map((column, index) => ({
      kind: "column" as const,
      source: parseRelColumnRef(rel.output[index]?.name ?? column.name),
      output: column.name,
    })),
    output,
  });
}

function isRecursiveCteBody(ast: SelectAst, cteName: string): boolean {
  const visit = (value: unknown): boolean => {
    if (!value || typeof value !== "object") {
      return false;
    }
    if (Array.isArray(value)) {
      return value.some(visit);
    }

    const record = value as Record<string, unknown>;
    if (typeof record.table === "string" && record.table === cteName) {
      return true;
    }

    return Object.values(record).some(visit);
  };

  return visit(ast._next);
}

function rewriteDerivedTables(ast: SelectAst): SelectAst {
  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (!from.some((entry) => !!entry.stmt)) {
    return ast;
  }

  const existingCteNames = new Set(
    (ast.with ?? []).flatMap((clause) => {
      const name = clause.name;
      if (typeof name === "string") {
        return [name];
      }
      if (name && typeof name === "object" && typeof name.value === "string") {
        return [name.value];
      }
      return [];
    }),
  );

  const syntheticCtes: CteAst[] = [];
  const rewrittenFrom: FromEntryAst[] = from.map((entry, index) => {
    if (!entry.stmt) {
      return entry;
    }

    const alias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : `derived_${index + 1}`;
    let syntheticName = `__tupl_derived_${index + 1}`;
    while (existingCteNames.has(syntheticName)) {
      syntheticName = `${syntheticName}_next`;
    }
    existingCteNames.add(syntheticName);
    syntheticCtes.push({
      name: { value: syntheticName },
      stmt: {
        ast: rewriteDerivedTables(entry.stmt.ast),
      },
    });

    return {
      table: syntheticName,
      as: alias,
      ...(entry.join ? { join: entry.join } : {}),
      ...(entry.on ? { on: entry.on } : {}),
    };
  });

  return {
    ...ast,
    from: rewrittenFrom,
    with: [...(ast.with ?? []), ...syntheticCtes],
  };
}
