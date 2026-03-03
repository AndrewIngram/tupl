import {
  normalizeCapability,
  resolveTableProvider,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import type { RelJoinNode, RelNode, RelProjectNode, RelScanNode } from "./rel";
import type { QueryRow, SchemaDefinition, TableScanRequest } from "./schema";

export interface RelExecutionGuardrails {
  maxExecutionRows: number;
  maxLookupKeysPerBatch: number;
  maxLookupBatches: number;
}

export class UnsupportedRelExecutionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedRelExecutionError";
  }
}

type InternalRow = Record<string, unknown>;

interface RelExecutionContext<TContext> {
  schema: SchemaDefinition;
  providers: ProvidersMap<TContext>;
  context: TContext;
  guardrails: RelExecutionGuardrails;
  lookupBatches: number;
}

export async function executeRelWithProviders<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  guardrails: RelExecutionGuardrails,
): Promise<QueryRow[]> {
  const executionContext: RelExecutionContext<TContext> = {
    schema,
    providers,
    context,
    guardrails,
    lookupBatches: 0,
  };

  const rows = await executeRelNode(rel, executionContext);
  if (rows.length > guardrails.maxExecutionRows) {
    throw new Error(
      `Query exceeded maxExecutionRows guardrail (${guardrails.maxExecutionRows}). Received ${rows.length} rows.`,
    );
  }

  return rows;
}

async function executeRelNode<TContext>(
  node: RelNode,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  switch (node.kind) {
    case "scan":
      return executeScan(node, context);
    case "join":
      return executeJoin(node, context);
    case "project":
      return executeProject(node, context);
    case "sort":
      return executeSort(node, context);
    case "limit_offset":
      return executeLimitOffset(node, context);
    case "filter":
      throw new UnsupportedRelExecutionError("Local filter node execution is not implemented yet.");
    case "aggregate":
      throw new UnsupportedRelExecutionError("Aggregate rel execution is not implemented yet.");
    case "set_op":
      throw new UnsupportedRelExecutionError("Set operation rel execution is not implemented yet.");
    case "with":
      throw new UnsupportedRelExecutionError("WITH rel execution is not implemented yet.");
    case "sql":
      throw new UnsupportedRelExecutionError("SQL fallback rel nodes require provider sql_query execution.");
  }
}

async function executeScan<TContext>(
  scan: RelScanNode,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const providerName = resolveTableProvider(context.schema, scan.table);
  const provider = context.providers[providerName];
  if (!provider) {
    throw new Error(`Missing provider adapter: ${providerName}`);
  }

  const request: TableScanRequest = {
    table: scan.table,
    ...(scan.alias ? { alias: scan.alias } : {}),
    select: scan.select,
    ...(scan.where ? { where: scan.where } : {}),
    ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
    ...(scan.limit != null ? { limit: scan.limit } : {}),
    ...(scan.offset != null ? { offset: scan.offset } : {}),
  };

  const fragment: ProviderFragment = {
    kind: "scan",
    provider: providerName,
    table: scan.table,
    request,
  };

  const capability = normalizeCapability(await provider.canExecute(fragment, context.context));
  if (!capability.supported) {
    throw new UnsupportedRelExecutionError(
      `Provider ${providerName} cannot execute scan for table ${scan.table}${
        capability.reason ? `: ${capability.reason}` : ""
      }`,
    );
  }

  const compiled = await provider.compile(fragment, context.context);
  const rows = await provider.execute(compiled, context.context);

  const alias = scan.alias ?? scan.table;
  return rows.map((row) => prefixRow(row, alias));
}

async function executeJoin<TContext>(
  join: RelJoinNode,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const leftRows = (await executeRelNode(join.left, context)) as InternalRow[];

  const lookupResult = await maybeExecuteLookupJoin(join, leftRows, context);
  if (lookupResult) {
    return lookupResult;
  }

  const rightRows = (await executeRelNode(join.right, context)) as InternalRow[];
  return applyLocalHashJoin(join, leftRows, rightRows);
}

async function maybeExecuteLookupJoin<TContext>(
  join: RelJoinNode,
  leftRows: InternalRow[],
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[] | null> {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScan(join.left);
  const rightScan = findFirstScan(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }

  const leftProviderName = resolveTableProvider(context.schema, leftScan.table);
  const rightProviderName = resolveTableProvider(context.schema, rightScan.table);
  if (leftProviderName === rightProviderName) {
    return null;
  }

  const rightProvider = context.providers[rightProviderName];
  if (!rightProvider?.lookupMany) {
    return null;
  }

  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightKey = join.rightKey.column;
  const dedupedKeys = [...new Set(leftRows.map((row) => row[leftKey]).filter((value) => value != null))];

  const rightRows: InternalRow[] = [];
  for (
    let startIndex = 0;
    startIndex < dedupedKeys.length;
    startIndex += context.guardrails.maxLookupKeysPerBatch
  ) {
    context.lookupBatches += 1;
    if (context.lookupBatches > context.guardrails.maxLookupBatches) {
      throw new Error(
        `Query exceeded maxLookupBatches guardrail (${context.guardrails.maxLookupBatches}).`,
      );
    }

    const batch = dedupedKeys.slice(
      startIndex,
      startIndex + context.guardrails.maxLookupKeysPerBatch,
    );

    const lookedUp = await rightProvider.lookupMany(
      {
        table: rightScan.table,
        key: rightKey,
        keys: batch,
        select: rightScan.select,
        ...(rightScan.where ? { where: rightScan.where } : {}),
      },
      context.context,
    );

    const rightAlias = rightScan.alias ?? rightScan.table;
    for (const row of lookedUp) {
      rightRows.push(prefixRow(row, rightAlias));
    }
  }

  return applyLocalHashJoin(join, leftRows, rightRows);
}

function applyLocalHashJoin(
  join: RelJoinNode,
  leftRows: InternalRow[],
  rightRows: InternalRow[],
): InternalRow[] {
  const leftKey = `${join.leftKey.alias}.${join.leftKey.column}`;
  const rightKey = `${join.rightKey.alias}.${join.rightKey.column}`;

  const rightIndex = new Map<unknown, InternalRow[]>();
  rightRows.forEach((row) => {
    const key = row[rightKey];
    if (key == null) {
      return;
    }

    const bucket = rightIndex.get(key) ?? [];
    bucket.push(row);
    rightIndex.set(key, bucket);
  });

  const joined: InternalRow[] = [];
  const matchedRightRows = new Set<InternalRow>();

  for (const leftRow of leftRows) {
    const key = leftRow[leftKey];
    const matches = key == null ? [] : (rightIndex.get(key) ?? []);

    if (matches.length === 0) {
      if (join.joinType === "left" || join.joinType === "full") {
        joined.push({ ...leftRow });
      }
      continue;
    }

    for (const match of matches) {
      matchedRightRows.add(match);
      joined.push({
        ...leftRow,
        ...match,
      });
    }
  }

  if (join.joinType === "right" || join.joinType === "full") {
    for (const rightRow of rightRows) {
      if (matchedRightRows.has(rightRow)) {
        continue;
      }
      joined.push({ ...rightRow });
    }
  }

  return joined;
}

async function executeProject<TContext>(
  project: RelProjectNode,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeRelNode(project.input, context)) as InternalRow[];

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const mapping of project.columns) {
      const alias = mapping.source.alias;
      const key = `${alias}.${mapping.source.column}`;
      out[mapping.output] = row[key] ?? null;
    }
    return out;
  });
}

async function executeSort<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: RelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeRelNode(sort.input, context)) as InternalRow[];
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const key = `${term.source.alias}.${term.source.column}`;
      const comparison = compareNullableValues(left[key], right[key]);
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return sorted;
}

async function executeLimitOffset<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: RelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  let rows = await executeRelNode(limitOffset.input, context);

  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }

  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return rows;
}

function findFirstScan(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return findFirstScan(node.input);
    case "join":
    case "set_op":
      return findFirstScan(node.left) ?? findFirstScan(node.right);
    case "with":
      return findFirstScan(node.body);
    case "sql":
      return null;
  }
}

function prefixRow(row: QueryRow, alias: string): InternalRow {
  const out: InternalRow = {};
  for (const [column, value] of Object.entries(row)) {
    out[`${alias}.${column}`] = value;
  }
  return out;
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString < rightString ? -1 : 1;
}
