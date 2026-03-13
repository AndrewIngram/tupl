import { Result, type Result as BetterResult } from "better-result";
import type { RelExpr, TuplSchemaNormalizationError } from "@tupl/foundation";

import { isSchemaDataEntityHandle } from "../dsl/dsl-tokens";
import type {
  SchemaColRefToken,
  SchemaDataEntityHandle,
  SchemaDslTableToken,
} from "../contracts/schema-contracts";
import { createSchemaNormalizationError } from "../schema-errors";
import type { SchemaViewRelNode, SchemaViewRelNodeInput } from "../types";

/**
 * View normalization owns normalization of DSL view definitions into schema-facing view contracts.
 */
export function resolveColRefToken(
  token: SchemaColRefToken,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): BetterResult<string, TuplSchemaNormalizationError> {
  if (token.ref) {
    return Result.ok(token.ref);
  }

  if (token.table && token.column) {
    return Result.ok(`${resolveTableToken(token.table)}.${token.column}`);
  }

  if (token.entity && token.column) {
    return Result.ok(`${resolveEntityToken(token.entity)}.${token.column}`);
  }

  return Result.err(
    createSchemaNormalizationError({
      operation: "resolve schema column reference",
      message: "Invalid schema column reference token.",
    }),
  );
}

export function resolveEnumRef(
  enumFrom: SchemaColRefToken | string,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): BetterResult<string, TuplSchemaNormalizationError> {
  if (typeof enumFrom === "string") {
    return Result.ok(enumFrom);
  }

  return resolveColRefToken(enumFrom, resolveTableToken, resolveEntityToken);
}

export function resolveViewRelDefinition(
  definition: unknown,
  resolveTableToken: (token: SchemaDslTableToken<string>) => string,
  resolveEntityToken: (entity: SchemaDataEntityHandle<string>) => string,
): BetterResult<unknown, TuplSchemaNormalizationError> {
  if (
    definition &&
    typeof definition === "object" &&
    typeof (definition as { convention?: unknown }).convention === "string"
  ) {
    return Result.ok(definition);
  }

  if (
    !definition ||
    typeof definition !== "object" ||
    typeof (definition as { kind?: unknown }).kind !== "string"
  ) {
    return Result.ok(definition);
  }

  const asRef = (token: SchemaColRefToken) =>
    Result.gen(function* () {
      const ref = yield* resolveColRefToken(token, resolveTableToken, resolveEntityToken);
      return Result.ok({
        kind: "dsl_col_ref" as const,
        ref,
      });
    });

  const resolveNode = (
    node: SchemaViewRelNodeInput,
  ): BetterResult<SchemaViewRelNode, TuplSchemaNormalizationError> => {
    switch (node.kind) {
      case "scan":
        if (isSchemaDataEntityHandle((node as { entity?: unknown }).entity)) {
          const entity = (node as unknown as { entity: SchemaDataEntityHandle<string> }).entity;
          return Result.ok({
            kind: "scan" as const,
            table: typeof node.table === "string" ? node.table : resolveEntityToken(entity),
            entity,
          });
        }
        if (isSchemaDataEntityHandle(node.table)) {
          return Result.ok({
            kind: "scan" as const,
            table: resolveEntityToken(node.table),
            entity: node.table,
          });
        }
        return Result.ok({
          kind: "scan" as const,
          table: typeof node.table === "string" ? node.table : resolveTableToken(node.table),
        });
      case "join":
        return Result.gen(function* () {
          const left = yield* resolveNode(node.left);
          const right = yield* resolveNode(node.right);
          const leftRef = yield* asRef(node.on.left);
          const rightRef = yield* asRef(node.on.right);
          return Result.ok({
            kind: "join" as const,
            left,
            right,
            on: {
              kind: "eq" as const,
              left: leftRef,
              right: rightRef,
            },
            type: node.type,
          });
        });
      case "aggregate":
        return Result.gen(function* () {
          const from = yield* resolveNode(node.from);
          const groupByEntries: [string, SchemaColRefToken][] = [];
          for (const [name, token] of Object.entries(node.groupBy)) {
            groupByEntries.push([name, yield* asRef(token)]);
          }

          const measureEntries: [string, (typeof node.measures)[string]][] = [];
          for (const [name, metric] of Object.entries(node.measures)) {
            if (!metric.column) {
              measureEntries.push([name, metric]);
              continue;
            }

            measureEntries.push([
              name,
              {
                ...metric,
                column: yield* asRef(metric.column),
              },
            ]);
          }

          return Result.ok({
            kind: "aggregate" as const,
            from,
            groupBy: Object.fromEntries(groupByEntries),
            measures: Object.fromEntries(measureEntries),
          });
        });
    }
  };

  return resolveNode(definition as SchemaViewRelNodeInput);
}

export function parseColumnSource(ref: string): string {
  const idx = ref.lastIndexOf(".");
  return idx >= 0 ? ref.slice(idx + 1) : ref;
}

export function collectUnqualifiedExprColumns(expr: RelExpr): Set<string> {
  const out = new Set<string>();

  const visit = (current: RelExpr): void => {
    switch (current.kind) {
      case "literal":
      case "subquery":
        return;
      case "function":
        current.args.forEach(visit);
        return;
      case "column":
        if (!current.ref.table && !current.ref.alias) {
          out.add(current.ref.column);
        }
        return;
    }
  };

  visit(expr);
  return out;
}
