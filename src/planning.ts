import { defaultSqlAstParser } from "./parser";
import type { SchemaDefinition } from "./schema";
import {
  collectRelTables,
  createSqlRel,
  type RelColumnRef,
  type RelJoinNode,
  type RelNode,
  validateRelAgainstSchema,
} from "./rel";
import {
  normalizeCapability,
  resolveTableProvider,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import type { PhysicalPlan, PhysicalStep } from "./physical";

interface SelectAst {
  type?: unknown;
  from?: unknown;
  with?: unknown;
  _next?: unknown;
  set_op?: unknown;
}

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

let physicalStepIdCounter = 0;

function nextPhysicalStepId(prefix: string): string {
  physicalStepIdCounter += 1;
  return `${prefix}_${physicalStepIdCounter}`;
}

export function lowerSqlToRel(sql: string, schema: SchemaDefinition): RelLoweringResult {
  const ast = defaultSqlAstParser.astify(sql) as SelectAst;
  const tables = collectTablesFromSelectAst(ast);
  const rel = createSqlRel(sql, tables);
  validateRelAgainstSchema(rel, schema);
  return {
    rel,
    tables,
  };
}

export async function planPhysicalQuery<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  sql: string,
): Promise<PhysicalPlan> {
  const tables = collectRelTables(rel);
  const providerByTable = new Map<string, string>();

  for (const table of tables) {
    providerByTable.set(table, resolveTableProvider(schema, table));
  }

  const uniqueProviders = new Set(providerByTable.values());

  if (uniqueProviders.size === 1) {
    const provider = [...uniqueProviders][0];
    if (!provider) {
      throw new Error("Unable to resolve provider for relational fragment.");
    }

    const adapter = providers[provider];
    if (!adapter) {
      throw new Error(`Missing provider adapter: ${provider}`);
    }

    const fragment: ProviderFragment = {
      kind: "sql_query",
      provider,
      sql,
      rel,
    };

    const capability = normalizeCapability(await adapter.canExecute(fragment, context));
    if (capability.supported) {
      const step: PhysicalStep = {
        id: nextPhysicalStepId("remote_fragment"),
        kind: "remote_fragment",
        dependsOn: [],
        summary: `Execute provider fragment (${provider})`,
        provider,
        fragment,
      };

      return {
        rel,
        rootStepId: step.id,
        steps: [step],
      };
    }
  }

  const lookupJoin = findLookupJoinCandidate(rel, schema, providers);
  if (lookupJoin) {
    const leftStep: PhysicalStep = {
      id: nextPhysicalStepId("remote_fragment"),
      kind: "remote_fragment",
      dependsOn: [],
      summary: `Fetch driver rows from ${lookupJoin.leftProvider}`,
      provider: lookupJoin.leftProvider,
      fragment: {
        kind: "scan",
        provider: lookupJoin.leftProvider,
        table: lookupJoin.leftTable,
        request: {
          table: lookupJoin.leftTable,
          select: [lookupJoin.leftKey],
        },
      },
    };

    const joinStep: PhysicalStep = {
      id: nextPhysicalStepId("lookup_join"),
      kind: "lookup_join",
      dependsOn: [leftStep.id],
      summary: `Lookup join ${lookupJoin.leftTable}.${lookupJoin.leftKey} -> ${lookupJoin.rightTable}.${lookupJoin.rightKey}`,
      leftProvider: lookupJoin.leftProvider,
      rightProvider: lookupJoin.rightProvider,
      leftTable: lookupJoin.leftTable,
      rightTable: lookupJoin.rightTable,
      leftKey: lookupJoin.leftKey,
      rightKey: lookupJoin.rightKey,
      joinType: lookupJoin.joinType,
    };

    return {
      rel,
      rootStepId: joinStep.id,
      steps: [leftStep, joinStep],
    };
  }

  const localStep: PhysicalStep = {
    id: nextPhysicalStepId("local_hash_join"),
    kind: "local_hash_join",
    dependsOn: [],
    summary: "Execute mixed-provider plan locally",
  };

  return {
    rel,
    rootStepId: localStep.id,
    steps: [localStep],
  };
}

function findLookupJoinCandidate<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftTable: string;
  rightTable: string;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  const join = findFirstJoinNode(rel);
  if (!join || (join.joinType !== "inner" && join.joinType !== "left")) {
    return null;
  }

  const leftTable = findFirstScanTable(join.left);
  const rightTable = findFirstScanTable(join.right);
  if (!leftTable || !rightTable) {
    return null;
  }

  const leftProvider = resolveTableProvider(schema, leftTable);
  const rightProvider = resolveTableProvider(schema, rightTable);
  if (leftProvider === rightProvider) {
    return null;
  }

  const rightAdapter = providers[rightProvider];
  if (!rightAdapter?.lookupMany) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftTable,
    rightTable,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanTable(node: RelNode): string | null {
  switch (node.kind) {
    case "scan":
      return node.table;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return findFirstScanTable(node.input);
    case "join":
    case "set_op":
      return findFirstScanTable(node.left) ?? findFirstScanTable(node.right);
    case "with":
      return findFirstScanTable(node.body);
    case "sql":
      return node.tables[0] ?? null;
  }
}

function findFirstJoinNode(node: RelNode): RelJoinNode | null {
  switch (node.kind) {
    case "join":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return findFirstJoinNode(node.input);
    case "set_op":
      return findFirstJoinNode(node.left) ?? findFirstJoinNode(node.right);
    case "with":
      return findFirstJoinNode(node.body);
    default:
      return null;
  }
}

function collectTablesFromSelectAst(ast: SelectAst): string[] {
  const tables = new Set<string>();
  const cteNames = new Set<string>();

  const visit = (value: unknown): void => {
    if (!value || typeof value !== "object") {
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
      }
      return;
    }

    const record = value as Record<string, unknown>;
    if (typeof record.name === "string") {
      cteNames.add(record.name);
    }

    const from = record.from;
    if (Array.isArray(from)) {
      for (const entry of from) {
        if (entry && typeof entry === "object") {
          const table = (entry as { table?: unknown }).table;
          if (typeof table === "string" && !cteNames.has(table)) {
            tables.add(table);
          }
        }
      }
    }

    for (const nested of Object.values(record)) {
      visit(nested);
    }
  };

  visit(ast.with);
  visit(ast);

  return [...tables];
}

export function makeJoinNode(
  left: RelNode,
  right: RelNode,
  joinType: "inner" | "left" | "right" | "full",
  leftKey: RelColumnRef,
  rightKey: RelColumnRef,
): RelJoinNode {
  return {
    id: `join_${left.id}_${right.id}`,
    kind: "join",
    convention: "local",
    left,
    right,
    joinType,
    leftKey,
    rightKey,
    output: [...left.output, ...right.output],
  };
}
