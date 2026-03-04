import {
  type AnyColumn,
  and,
  asc,
  desc,
  eq,
  gt,
  gte,
  inArray,
  isNotNull,
  isNull,
  lt,
  lte,
  ne,
  sql,
  type SQL,
} from "drizzle-orm";
import {
  createDataEntityHandle,
  type DataEntityHandle,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProviderLookupManyRequest,
  type QueryRow,
  type RelNode,
  type ScanFilterClause,
  type ScanOrderBy,
  type TableScanRequest,
} from "sqlql";

export type DrizzleColumnMap<TColumn extends string = string> = Record<TColumn, AnyColumn>;

export interface DrizzleQueryExecutor {
  select: (...args: unknown[]) => unknown;
}

export interface DrizzleProviderTableConfig<
  TContext,
  TColumn extends string = string,
> {
  table: object;
  /**
   * Optional explicit column map. If omitted, columns are derived from the
   * Drizzle table object and exposed by both property key and DB column name.
   */
  columns?: DrizzleColumnMap<TColumn>;
  scope?:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
}

export interface CreateDrizzleProviderOptions<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext, string>> = Record<
    string,
    DrizzleProviderTableConfig<TContext, string>
  >,
> {
  name?: string;
  dialect?: "postgres" | "sqlite";
  db: DrizzleQueryExecutor;
  tables: TTables;
}

interface DrizzleRelCompiledPlan {
  rel: RelNode;
}

type InferDrizzleTableColumns<TConfig> = TConfig extends DrizzleProviderTableConfig<any, infer TColumn>
  ? TColumn
  : string;

export function createDrizzleProvider<
  TContext,
  TTables extends Record<string, DrizzleProviderTableConfig<TContext, string>> = Record<
    string,
    DrizzleProviderTableConfig<TContext, string>
  >,
>(
  options: CreateDrizzleProviderOptions<TContext, TTables>,
): ProviderAdapter<TContext> & {
  entities: { [K in keyof TTables]: DataEntityHandle<InferDrizzleTableColumns<TTables[K]>> };
  tables: { [K in keyof TTables]: DataEntityHandle<InferDrizzleTableColumns<TTables[K]>> };
} {
  const providerName = options.name ?? "drizzle";
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext, string>>;
  const dialect = options.dialect ?? inferDrizzleDialect(options.db, tableConfigs);
  void dialect;

  const handles = {} as {
    [K in keyof TTables]: DataEntityHandle<InferDrizzleTableColumns<TTables[K]>>;
  };
  for (const tableName of Object.keys(options.tables) as Array<Extract<keyof TTables, string>>) {
    handles[tableName] = createDataEntityHandle<InferDrizzleTableColumns<TTables[typeof tableName]>>({
      entity: tableName,
      provider: providerName,
    });
  }

  return {
    entities: handles,
    tables: handles,
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return !!tableConfigs[fragment.table];
        case "rel":
          return canCompileRel(fragment.rel, tableConfigs)
            ? true
            : {
                supported: false,
                reason: hasSqlNode(fragment.rel)
                  ? "rel fragment must not contain sql nodes."
                  : "Rel fragment is not supported for single-query drizzle pushdown.",
              };
        case "sql_query":
          return {
            supported: false,
            reason: "Drizzle adapter does not support sql_query fragments.",
          };
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      switch (fragment.kind) {
        case "scan":
          return {
            provider: providerName,
            kind: fragment.kind,
            payload: fragment,
          };
        case "rel":
          if (!canCompileRel(fragment.rel, tableConfigs)) {
            throw new Error("Unsupported relational fragment for drizzle provider.");
          }

          return {
            provider: providerName,
            kind: fragment.kind,
            payload: {
              rel: fragment.rel,
            } satisfies DrizzleRelCompiledPlan,
          };
        case "sql_query":
          throw new Error("Drizzle adapter does not support sql_query fragments.");
        default:
          throw new Error(`Unsupported drizzle fragment kind: ${(fragment as { kind?: unknown }).kind}`);
      }
    },
    async execute(plan, context): Promise<QueryRow[]> {
      return executeDrizzlePlan(plan, options, context);
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      return lookupManyWithDrizzle(options, request, context);
    },
  };
}

function inferDrizzleDialect<TContext>(
  db: DrizzleQueryExecutor,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext, string>>,
): "postgres" | "sqlite" {
  const tableDialects = new Set<"postgres" | "sqlite">();
  for (const tableConfig of Object.values(tableConfigs)) {
    const detected = normalizeDialectName(readTableDialectName(tableConfig.table));
    if (detected) {
      tableDialects.add(detected);
    }
  }

  if (tableDialects.size > 1) {
    throw new Error(
      `Unable to infer drizzle dialect: provider tables declare mixed dialects (${[
        ...tableDialects,
      ].join(", ")}).`,
    );
  }

  const fromTables = [...tableDialects][0];
  if (fromTables) {
    return fromTables;
  }

  const fromDb = normalizeDialectName(readDbDialectHint(db));
  if (fromDb) {
    return fromDb;
  }

  return "sqlite";
}

function readTableDialectName(table: object): string | undefined {
  const candidate = (table as { _?: { config?: { dialect?: unknown } } })._?.config?.dialect;
  return typeof candidate === "string" ? candidate : undefined;
}

function readDbDialectHint(db: DrizzleQueryExecutor): string | undefined {
  const maybeDialect = (db as { dialect?: unknown }).dialect;
  if (typeof maybeDialect === "string") {
    return maybeDialect;
  }

  const constructorName = (db as { constructor?: { name?: unknown } }).constructor?.name;
  if (typeof constructorName === "string" && constructorName.length > 0) {
    return constructorName;
  }

  const sessionName = (
    db as unknown as { _: { session?: { constructor?: { name?: unknown } } } }
  )._?.session?.constructor?.name;
  return typeof sessionName === "string" ? sessionName : undefined;
}

function normalizeDialectName(name: string | undefined): "postgres" | "sqlite" | null {
  if (!name) {
    return null;
  }
  const normalized = name.toLowerCase();
  if (normalized === "pg" || normalized === "postgres" || normalized.includes("pglite")) {
    return "postgres";
  }
  if (normalized === "sqlite" || normalized.includes("sqlite")) {
    return "sqlite";
  }
  return null;
}

async function executeDrizzlePlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext, string>>;

  switch (plan.kind) {
    case "rel": {
      const compiled = plan.payload as DrizzleRelCompiledPlan;
      try {
        return await executeDrizzleRelSingleQuery(compiled.rel, options, context);
      } catch (error) {
        if (error instanceof UnsupportedSingleQueryPlanError) {
          return executeDrizzleRel(compiled.rel, options, context);
        }
        throw error;
      }
    }
    case "scan": {
      const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
      const tableConfig = tableConfigs[fragment.table];
      if (!tableConfig) {
        throw new Error(`Unknown drizzle table config: ${fragment.table}`);
      }

      const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
      return runDrizzleScan({
        db: options.db,
        tableName: fragment.table,
        table: tableConfig.table,
        columns: resolveColumns(tableConfig, fragment.table),
        request: fragment.request,
        ...(scope ? { scope } : {}),
      });
    }
    case "sql_query":
      throw new Error("Drizzle adapter does not support sql_query fragments.");
    default:
      throw new Error(`Unsupported drizzle compiled plan kind: ${plan.kind}`);
  }
}

async function lookupManyWithDrizzle<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext, string>>;
  const tableConfig = tableConfigs[request.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${request.table}`);
  }

  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
  return runDrizzleScan({
    db: options.db,
    tableName: request.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, request.table),
    request: {
      table: request.table,
      select: request.select,
      where,
    },
    ...(scope ? { scope } : {}),
  });
}

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: DrizzleQueryExecutor;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
}

export async function runDrizzleScan<TTable extends string, TColumn extends string>(
  options: RunDrizzleScanOptions<TTable, TColumn>,
): Promise<QueryRow[]> {
  const selection = buildSelection(options.request.select, options.columns, options.tableName);
  const filterConditions = (options.request.where ?? []).map((clause) =>
    toSqlCondition(clause, options.columns, options.tableName),
  );
  const scopeConditions = normalizeScope(options.scope);
  const whereConditions = [...scopeConditions, ...filterConditions];

  const selectable = options.db.select(selection) as {
    from: (table: never) => {
      where: (condition: SQL) => unknown;
      orderBy: (...clauses: SQL[]) => unknown;
      limit: (value: number) => unknown;
      offset: (value: number) => unknown;
      execute: () => Promise<QueryRow[]>;
    };
  };

  let builder = selectable.from(options.table as never) as {
    where: (condition: SQL) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    execute: () => Promise<QueryRow[]>;
  };

  const where = and(...whereConditions);
  if (where) {
    builder = builder.where(where) as typeof builder;
  }

  const orderBy = buildOrderBy(options.request.orderBy, options.columns, options.tableName);
  if (orderBy.length > 0) {
    builder = builder.orderBy(...orderBy) as typeof builder;
  }

  if (options.request.limit != null) {
    builder = builder.limit(options.request.limit) as typeof builder;
  }

  if (options.request.offset != null) {
    builder = builder.offset(options.request.offset) as typeof builder;
  }

  return builder.execute();
}

function resolveColumns<TContext>(
  tableConfig: DrizzleProviderTableConfig<TContext, string>,
  tableName: string,
): DrizzleColumnMap<string> {
  if (tableConfig.columns) {
    return tableConfig.columns;
  }

  const derived = deriveColumnsFromTable(tableConfig.table);
  if (Object.keys(derived).length === 0) {
    throw new Error(
      `Unable to derive columns for table "${tableName}". Provide an explicit columns map.`,
    );
  }

  return derived;
}

function deriveColumnsFromTable(table: object): DrizzleColumnMap<string> {
  const out: DrizzleColumnMap<string> = {};

  for (const [propertyKey, raw] of Object.entries(table as Record<string, unknown>)) {
    if (!looksLikeDrizzleColumn(raw)) {
      continue;
    }

    const column = raw as AnyColumn;
    out[propertyKey] = column;

    const dbName = readColumnName(column);
    if (dbName) {
      out[dbName] = column;
    }
  }

  return out;
}

function looksLikeDrizzleColumn(value: unknown): value is AnyColumn {
  return (
    !!value &&
    typeof value === "object" &&
    typeof (value as { name?: unknown }).name === "string"
  );
}

function readColumnName(column: AnyColumn): string | null {
  const maybeName = (column as unknown as { name?: unknown }).name;
  return typeof maybeName === "string" ? maybeName : null;
}

export function impossibleCondition(): SQL {
  return sql`0 = 1`;
}

function normalizeScope(scope: SQL | SQL[] | undefined): SQL[] {
  if (!scope) {
    return [];
  }
  return Array.isArray(scope) ? scope : [scope];
}

function buildSelection<TColumn extends string>(
  selectedColumns: TColumn[],
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): Record<TColumn, AnyColumn> {
  const out = {} as Record<TColumn, AnyColumn>;
  for (const column of selectedColumns) {
    const source = columns[column];
    if (!source) {
      throw new Error(`Unsupported column "${column}" for table "${tableName}".`);
    }
    out[column] = source;
  }
  return out;
}

function buildOrderBy<TColumn extends string>(
  orderBy: ScanOrderBy<TColumn>[] | undefined,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL[] {
  const out: SQL[] = [];
  for (const term of orderBy ?? []) {
    const source = columns[term.column];
    if (!source) {
      throw new Error(`Unsupported ORDER BY column "${term.column}" for table "${tableName}".`);
    }

    out.push(term.direction === "asc" ? asc(source) : desc(source));
  }
  return out;
}

function toSqlCondition<TColumn extends string>(
  clause: ScanFilterClause<TColumn>,
  columns: DrizzleColumnMap<TColumn>,
  tableName: string,
): SQL {
  const source = columns[clause.column as TColumn];
  if (!source) {
    throw new Error(`Unsupported filter column "${clause.column}" for table "${tableName}".`);
  }

  switch (clause.op) {
    case "eq":
      return eq(source, clause.value as never);
    case "neq":
      return ne(source, clause.value as never);
    case "gt":
      return gt(source, clause.value as never);
    case "gte":
      return gte(source, clause.value as never);
    case "lt":
      return lt(source, clause.value as never);
    case "lte":
      return lte(source, clause.value as never);
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return inArray(source, values as never[]);
    }
    case "is_null":
      return isNull(source);
    case "is_not_null":
      return isNotNull(source);
  }
}

function canCompileRel(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<any, string>>,
): boolean {
  switch (node.kind) {
    case "scan":
      return !!tableConfigs[node.table];
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return canCompileRel(node.input, tableConfigs);
    case "join":
      return canCompileRel(node.left, tableConfigs) && canCompileRel(node.right, tableConfigs);
    case "set_op":
    case "with":
    case "sql":
      return false;
  }
}

function hasSqlNode(node: RelNode): boolean {
  switch (node.kind) {
    case "sql":
      return true;
    case "scan":
      return false;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return hasSqlNode(node.input);
    case "join":
    case "set_op":
      return hasSqlNode(node.left) || hasSqlNode(node.right);
    case "with":
      return node.ctes.some((cte) => hasSqlNode(cte.query)) || hasSqlNode(node.body);
  }
}

type InternalRow = Record<string, unknown>;

class UnsupportedSingleQueryPlanError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedSingleQueryPlanError";
  }
}

interface RelPipeline {
  base: RelNode;
  project?: Extract<RelNode, { kind: "project" }>;
  aggregate?: Extract<RelNode, { kind: "aggregate" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

interface ScanBinding<TContext> {
  alias: string;
  scan: Extract<RelNode, { kind: "scan" }>;
  tableName: string;
  table: object;
  columns: DrizzleColumnMap<string>;
  scopeConditions: SQL[];
  tableConfig: DrizzleProviderTableConfig<TContext, string>;
}

interface JoinStep<TContext> {
  joinType: Extract<RelNode, { kind: "join" }>["joinType"];
  right: ScanBinding<TContext>;
  leftKey: { alias: string; column: string };
  rightKey: { alias: string; column: string };
}

interface JoinPlan<TContext> {
  root: ScanBinding<TContext>;
  joins: JoinStep<TContext>[];
  aliases: Map<string, ScanBinding<TContext>>;
}

interface SingleQueryPlan<TContext> {
  joinPlan: JoinPlan<TContext>;
  pipeline: RelPipeline;
}

async function executeDrizzleRelSingleQuery<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfigs = options.tables as Record<string, DrizzleProviderTableConfig<TContext, string>>;
  const plan = await buildSingleQueryPlan(rel, tableConfigs, context);
  const selection = buildSingleQuerySelection(plan);
  const preferDistinctSelection =
    !!plan.pipeline.aggregate &&
    plan.pipeline.aggregate.metrics.length === 0 &&
    plan.pipeline.aggregate.groupBy.length > 0;
  const db = options.db as {
    select: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
    selectDistinct?: (selection: Record<string, unknown>) => {
      from: (table: object) => {
        innerJoin: (table: object, on: SQL) => unknown;
        leftJoin: (table: object, on: SQL) => unknown;
        rightJoin: (table: object, on: SQL) => unknown;
        fullJoin: (table: object, on: SQL) => unknown;
        where: (condition: SQL) => unknown;
        groupBy: (...columns: AnyColumn[]) => unknown;
        orderBy: (...clauses: SQL[]) => unknown;
        limit: (value: number) => unknown;
        offset: (value: number) => unknown;
        execute: () => Promise<QueryRow[]>;
      };
    };
  };

  const selectFn =
    preferDistinctSelection && typeof db.selectDistinct === "function"
      ? db.selectDistinct.bind(db)
      : db.select.bind(db);

  let builder = selectFn(selection).from(plan.joinPlan.root.table) as {
    innerJoin: (table: object, on: SQL) => unknown;
    leftJoin: (table: object, on: SQL) => unknown;
    rightJoin: (table: object, on: SQL) => unknown;
    fullJoin: (table: object, on: SQL) => unknown;
    where: (condition: SQL) => unknown;
    groupBy: (...columns: AnyColumn[]) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    execute: () => Promise<QueryRow[]>;
  };

  ensureJoinMethodsAvailable(builder, plan.joinPlan.joins);

  for (const joinStep of plan.joinPlan.joins) {
    const leftColumn = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.leftKey.alias,
      column: joinStep.leftKey.column,
    });
    const rightColumn = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias: joinStep.rightKey.alias,
      column: joinStep.rightKey.column,
    });
    const onClause = eq(leftColumn, rightColumn);
    builder = (
      joinStep.joinType === "inner"
        ? builder.innerJoin(joinStep.right.table, onClause)
        : joinStep.joinType === "left"
          ? builder.leftJoin(joinStep.right.table, onClause)
          : joinStep.joinType === "right"
            ? builder.rightJoin(joinStep.right.table, onClause)
            : builder.fullJoin(joinStep.right.table, onClause)
    ) as typeof builder;
  }

  const whereClauses: SQL[] = [];
  for (const binding of plan.joinPlan.aliases.values()) {
    whereClauses.push(...binding.scopeConditions);
    for (const clause of binding.scan.where ?? []) {
      whereClauses.push(toSqlCondition(clause, binding.columns, binding.tableName));
    }
  }

  for (const filterNode of plan.pipeline.filters) {
    for (const clause of filterNode.where) {
      whereClauses.push(toSqlConditionFromRelFilterClause(clause, plan.joinPlan.aliases));
    }
  }

  const where = and(...whereClauses);
  if (where) {
    builder = builder.where(where) as typeof builder;
  }

  if (
    plan.pipeline.aggregate &&
    plan.pipeline.aggregate.groupBy.length > 0 &&
    !preferDistinctSelection
  ) {
    const groupByColumns = plan.pipeline.aggregate.groupBy.map((columnRef) =>
      resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(columnRef.alias ?? columnRef.table, columnRef.column),
      ));
    builder = builder.groupBy(...groupByColumns) as typeof builder;
  }

  if (plan.pipeline.sort) {
    const orderBy = plan.pipeline.sort.orderBy.map((term) => {
      const source = resolveSingleQuerySortSource(term, plan);
      return term.direction === "asc" ? asc(source) : desc(source);
    });
    if (orderBy.length > 0) {
      builder = builder.orderBy(...orderBy) as typeof builder;
    }
  }

  if (plan.pipeline.limitOffset?.limit != null) {
    builder = builder.limit(plan.pipeline.limitOffset.limit) as typeof builder;
  }
  if (plan.pipeline.limitOffset?.offset != null) {
    builder = builder.offset(plan.pipeline.limitOffset.offset) as typeof builder;
  }

  return builder.execute();
}

function resolveSingleQuerySortSource<TContext>(
  term: Extract<RelNode, { kind: "sort" }>["orderBy"][number],
  plan: SingleQueryPlan<TContext>,
): AnyColumn | SQL {
  const alias = term.source.alias ?? term.source.table;
  if (alias) {
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      alias,
      column: term.source.column,
    });
  }

  if (!plan.pipeline.aggregate) {
    return resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
      column: term.source.column,
    });
  }

  const metric = plan.pipeline.aggregate.metrics.find((entry) => entry.as === term.source.column);
  if (metric) {
    return buildAggregateMetricSql(metric, plan.joinPlan.aliases);
  }

  const groupBy = plan.pipeline.aggregate.groupBy.find((entry) => entry.column === term.source.column);
  if (groupBy) {
    return resolveColumnRefFromAliasMap(
      plan.joinPlan.aliases,
      toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
    );
  }

  throw new UnsupportedSingleQueryPlanError(
    `Unsupported ORDER BY reference "${term.source.column}" in aggregate rel fragment.`,
  );
}

function ensureJoinMethodsAvailable<TContext>(
  builder: {
    innerJoin?: unknown;
    leftJoin?: unknown;
    rightJoin?: unknown;
    fullJoin?: unknown;
  },
  joins: JoinStep<TContext>[],
): void {
  for (const join of joins) {
    const methodName =
      join.joinType === "inner"
        ? "innerJoin"
        : join.joinType === "left"
          ? "leftJoin"
          : join.joinType === "right"
            ? "rightJoin"
            : "fullJoin";

    if (typeof builder[methodName] !== "function") {
      throw new UnsupportedSingleQueryPlanError(
        `Drizzle query builder does not support ${methodName} for single-query pushdown.`,
      );
    }
  }
}

async function buildSingleQueryPlan<TContext>(
  rel: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext, string>>,
  context: TContext,
): Promise<SingleQueryPlan<TContext>> {
  const pipeline = extractRelPipeline(rel);
  const joinPlan = await buildJoinPlan(pipeline.base, tableConfigs, context);

  return {
    joinPlan,
    pipeline,
  };
}

function extractRelPipeline(node: RelNode): RelPipeline {
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let aggregate: Extract<RelNode, { kind: "aggregate" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          throw new UnsupportedSingleQueryPlanError("Multiple project nodes are not supported.");
        }
        project = current;
        current = current.input;
        continue;
      case "aggregate":
        if (aggregate) {
          throw new UnsupportedSingleQueryPlanError("Multiple aggregate nodes are not supported.");
        }
        aggregate = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          throw new UnsupportedSingleQueryPlanError("Multiple sort nodes are not supported.");
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          throw new UnsupportedSingleQueryPlanError("Multiple limit/offset nodes are not supported.");
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "scan":
      case "join":
        return {
          base: current,
          ...(project ? { project } : {}),
          ...(aggregate ? { aggregate } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          filters,
        };
      case "set_op":
      case "with":
      case "sql":
        throw new UnsupportedSingleQueryPlanError(
          `Rel node "${current.kind}" is not supported in single-query pushdown.`,
        );
    }
  }
}

async function buildJoinPlan<TContext>(
  node: RelNode,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext, string>>,
  context: TContext,
): Promise<JoinPlan<TContext>> {
  if (node.kind === "scan") {
    const root = await createScanBinding(node, tableConfigs, context);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedSingleQueryPlanError(`Expected scan/join base node, received "${node.kind}".`);
  }

  const left = await buildJoinPlan(node.left, tableConfigs, context);
  const right = await buildJoinPlan(node.right, tableConfigs, context);
  if (right.joins.length > 0) {
    throw new UnsupportedSingleQueryPlanError("Only left-deep join trees are supported.");
  }

  const rightRoot = right.root;
  if (left.aliases.has(rightRoot.alias)) {
    throw new UnsupportedSingleQueryPlanError(`Duplicate alias "${rightRoot.alias}" in join tree.`);
  }

  const seenTables = new Set([...left.aliases.values()].map((binding) => binding.tableName));
  if (seenTables.has(rightRoot.tableName)) {
    throw new UnsupportedSingleQueryPlanError(
      "Joining the same physical table more than once is not supported without aliases.",
    );
  }

  const leftAlias = node.leftKey.alias ?? node.leftKey.table;
  const rightAlias = node.rightKey.alias ?? node.rightKey.table;
  if (!leftAlias || !rightAlias) {
    throw new UnsupportedSingleQueryPlanError("Join keys must be qualified with table aliases.");
  }

  const aliases = new Map(left.aliases);
  aliases.set(rightRoot.alias, rightRoot);
  for (const [alias, binding] of right.aliases.entries()) {
    aliases.set(alias, binding);
  }

  return {
    root: left.root,
    joins: [
      ...left.joins,
      {
        joinType: node.joinType,
        right: rightRoot,
        leftKey: {
          alias: leftAlias,
          column: node.leftKey.column,
        },
        rightKey: {
          alias: rightAlias,
          column: node.rightKey.column,
        },
      },
    ],
    aliases,
  };
}

async function createScanBinding<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext, string>>,
  context: TContext,
): Promise<ScanBinding<TContext>> {
  const tableConfig = tableConfigs[scan.table];
  if (!tableConfig) {
    throw new UnsupportedSingleQueryPlanError(`Missing drizzle table config for "${scan.table}".`);
  }

  return {
    alias: scan.alias ?? scan.table,
    scan,
    tableName: scan.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, scan.table),
    scopeConditions: normalizeScope(
      tableConfig.scope ? await tableConfig.scope(context) : undefined,
    ),
    tableConfig,
  };
}

function buildSingleQuerySelection<TContext>(
  plan: SingleQueryPlan<TContext>,
): Record<string, AnyColumn | SQL> {
  const selection: Record<string, AnyColumn | SQL> = {};

  if (plan.pipeline.aggregate) {
    for (const groupBy of plan.pipeline.aggregate.groupBy) {
      const source = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(groupBy.alias ?? groupBy.table, groupBy.column),
      );
      selection[groupBy.column] = source;
    }

    for (const metric of plan.pipeline.aggregate.metrics) {
      selection[metric.as] = buildAggregateMetricSql(metric, plan.joinPlan.aliases);
    }
    return selection;
  }

  if (plan.pipeline.project) {
    for (const mapping of plan.pipeline.project.columns) {
      selection[mapping.output] = resolveColumnRefFromAliasMap(
        plan.joinPlan.aliases,
        toAliasColumnRef(mapping.source.alias ?? mapping.source.table, mapping.source.column),
      );
    }
    return selection;
  }

  for (const binding of plan.joinPlan.aliases.values()) {
    for (const column of binding.scan.select) {
      selection[`${binding.alias}.${column}`] = resolveColumnRefFromAliasMap(plan.joinPlan.aliases, {
        alias: binding.alias,
        column,
      });
    }
  }

  return selection;
}

function buildAggregateMetricSql<TContext>(
  metric: Extract<RelNode, { kind: "aggregate" }>["metrics"][number],
  aliases: Map<string, ScanBinding<TContext>>,
): SQL {
  if (metric.fn === "count" && !metric.column) {
    return sql`count(*)`;
  }

  if (!metric.column) {
    throw new UnsupportedSingleQueryPlanError(`Aggregate ${metric.fn} requires a column.`);
  }

  const source = resolveColumnRefFromAliasMap(aliases, {
    ...toAliasColumnRef(metric.column.alias ?? metric.column.table, metric.column.column),
  });

  switch (metric.fn) {
    case "count":
      return metric.distinct ? sql`count(distinct ${source})` : sql`count(${source})`;
    case "sum":
      return metric.distinct ? sql`sum(distinct ${source})` : sql`sum(${source})`;
    case "avg":
      return metric.distinct ? sql`avg(distinct ${source})` : sql`avg(${source})`;
    case "min":
      return sql`min(${source})`;
    case "max":
      return sql`max(${source})`;
  }
}

function toSqlConditionFromRelFilterClause<TContext>(
  clause: ScanFilterClause,
  aliases: Map<string, ScanBinding<TContext>>,
): SQL {
  const source = resolveColumnRefFromFilterColumn(aliases, clause.column);
  return toSqlConditionFromSource(clause, source);
}

function toSqlConditionFromSource(
  clause: ScanFilterClause,
  source: AnyColumn,
): SQL {
  switch (clause.op) {
    case "eq":
      return eq(source, clause.value as never);
    case "neq":
      return ne(source, clause.value as never);
    case "gt":
      return gt(source, clause.value as never);
    case "gte":
      return gte(source, clause.value as never);
    case "lt":
      return lt(source, clause.value as never);
    case "lte":
      return lte(source, clause.value as never);
    case "in": {
      const values = clause.values.filter((value) => value != null);
      if (values.length === 0) {
        return impossibleCondition();
      }
      return inArray(source, values as never[]);
    }
    case "is_null":
      return isNull(source);
    case "is_not_null":
      return isNotNull(source);
  }
}

function resolveColumnRefFromFilterColumn<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  column: string,
): AnyColumn {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    return resolveColumnRefFromAliasMap(aliases, { alias, column: name });
  }

  return resolveColumnRefFromAliasMap(aliases, { column });
}

function resolveColumnRefFromAliasMap<TContext>(
  aliases: Map<string, ScanBinding<TContext>>,
  ref: { alias?: string; column: string },
): AnyColumn {
  if (ref.alias) {
    const binding = aliases.get(ref.alias);
    if (!binding) {
      throw new UnsupportedSingleQueryPlanError(`Unknown alias "${ref.alias}" in rel fragment.`);
    }
    const source = binding.columns[ref.column];
    if (!source) {
      throw new UnsupportedSingleQueryPlanError(
        `Unknown column "${ref.column}" on alias "${ref.alias}" in rel fragment.`,
      );
    }
    return source;
  }

  let matched: AnyColumn | null = null;
  for (const binding of aliases.values()) {
    const source = binding.columns[ref.column];
    if (!source) {
      continue;
    }
    if (matched && matched !== source) {
      throw new UnsupportedSingleQueryPlanError(
        `Ambiguous unqualified column "${ref.column}" in rel fragment.`,
      );
    }
    matched = source;
  }

  if (!matched) {
    throw new UnsupportedSingleQueryPlanError(
      `Unknown unqualified column "${ref.column}" in rel fragment.`,
    );
  }

  return matched;
}

function toAliasColumnRef(
  alias: string | undefined,
  column: string,
): { alias?: string; column: string } {
  return alias ? { alias, column } : { column };
}

interface DrizzleRelExecutionContext<TContext> {
  options: CreateDrizzleProviderOptions<TContext>;
  tableConfigs: Record<string, DrizzleProviderTableConfig<TContext, string>>;
  context: TContext;
  cteRows: Map<string, QueryRow[]>;
}

async function executeDrizzleRel<TContext>(
  rel: RelNode,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const executionContext: DrizzleRelExecutionContext<TContext> = {
    options,
    tableConfigs: options.tables as Record<string, DrizzleProviderTableConfig<TContext, string>>,
    context,
    cteRows: new Map<string, QueryRow[]>(),
  };

  return executeDrizzleRelNode(rel, executionContext);
}

async function executeDrizzleRelNode<TContext>(
  node: RelNode,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  switch (node.kind) {
    case "scan":
      return executeDrizzleRelScan(node, context);
    case "filter":
      return executeDrizzleRelFilter(node, context);
    case "project":
      return executeDrizzleRelProject(node, context);
    case "join":
      return executeDrizzleRelJoin(node, context);
    case "aggregate":
      return executeDrizzleRelAggregate(node, context);
    case "sort":
      return executeDrizzleRelSort(node, context);
    case "limit_offset":
      return executeDrizzleRelLimitOffset(node, context);
    case "set_op":
      return executeDrizzleRelSetOp(node, context);
    case "with":
      return executeDrizzleRelWith(node, context);
    case "sql":
      throw new Error("Drizzle adapter does not execute rel sql nodes.");
  }
}

async function executeDrizzleRelScan<TContext>(
  scan: Extract<RelNode, { kind: "scan" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const alias = scan.alias ?? scan.table;

  const cteRows = context.cteRows.get(scan.table);
  if (cteRows) {
    const scanned = scanLocalRows(cteRows, {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    });
    return scanned.map((row) => prefixRow(row, alias));
  }

  const tableConfig = context.tableConfigs[scan.table];
  if (!tableConfig) {
    throw new Error(`Unknown drizzle table config: ${scan.table}`);
  }

  const scope = tableConfig.scope ? await tableConfig.scope(context.context) : undefined;
  const rows = await runDrizzleScan({
    db: context.options.db,
    tableName: scan.table,
    table: tableConfig.table,
    columns: resolveColumns(tableConfig, scan.table),
    request: {
      table: scan.table,
      ...(scan.alias ? { alias: scan.alias } : {}),
      select: scan.select,
      ...(scan.where ? { where: scan.where } : {}),
      ...(scan.orderBy ? { orderBy: scan.orderBy } : {}),
      ...(scan.limit != null ? { limit: scan.limit } : {}),
      ...(scan.offset != null ? { offset: scan.offset } : {}),
    },
    ...(scope ? { scope } : {}),
  });

  return rows.map((row) => prefixRow(row, alias));
}

async function executeDrizzleRelFilter<TContext>(
  filter: Extract<RelNode, { kind: "filter" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(filter.input, context)) as InternalRow[];
  let out = [...rows];

  for (const clause of filter.where) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  return out;
}

async function executeDrizzleRelProject<TContext>(
  project: Extract<RelNode, { kind: "project" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(project.input, context)) as InternalRow[];

  return rows.map((row) => {
    const out: QueryRow = {};
    for (const mapping of project.columns) {
      out[mapping.output] = readRowValue(row, toColumnKey(mapping.source)) ?? null;
    }
    return out;
  });
}

async function executeDrizzleRelJoin<TContext>(
  join: Extract<RelNode, { kind: "join" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const leftRows = (await executeDrizzleRelNode(join.left, context)) as InternalRow[];
  const rightRows = (await executeDrizzleRelNode(join.right, context)) as InternalRow[];
  return applyLocalHashJoin(join, leftRows, rightRows);
}

function applyLocalHashJoin(
  join: Extract<RelNode, { kind: "join" }>,
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

async function executeDrizzleRelAggregate<TContext>(
  aggregate: Extract<RelNode, { kind: "aggregate" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const rows = (await executeDrizzleRelNode(aggregate.input, context)) as InternalRow[];
  const groups = new Map<string, InternalRow[]>();

  for (const row of rows) {
    const key = JSON.stringify(
      aggregate.groupBy.map((ref) => readRowValue(row, toColumnKey(ref)) ?? null),
    );
    const bucket = groups.get(key) ?? [];
    bucket.push(row);
    groups.set(key, bucket);
  }

  if (groups.size === 0 && aggregate.groupBy.length === 0) {
    groups.set("__all__", []);
  }

  const out: QueryRow[] = [];

  for (const [groupKey, bucket] of groups.entries()) {
    const row: QueryRow = {};

    if (aggregate.groupBy.length > 0) {
      const values = JSON.parse(groupKey) as unknown[];
      aggregate.groupBy.forEach((ref, index) => {
        row[ref.column] = values[index] ?? null;
      });
    }

    for (const metric of aggregate.metrics) {
      const values = metric.column
        ? bucket.map((entry) => readRowValue(entry, toColumnKey(metric.column!)) ?? null)
        : bucket.map(() => 1);
      const metricValues = metric.distinct
        ? [...new Map(values.map((value) => [JSON.stringify(value), value])).values()]
        : values;

      row[metric.as] = evaluateAggregateMetric(metric.fn, metricValues, bucket.length, metric.column != null);
    }

    out.push(row);
  }

  return out;
}

async function executeDrizzleRelSort<TContext>(
  sort: Extract<RelNode, { kind: "sort" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<InternalRow[]> {
  const rows = (await executeDrizzleRelNode(sort.input, context)) as InternalRow[];
  const sorted = [...rows];

  sorted.sort((left, right) => {
    for (const term of sort.orderBy) {
      const comparison = compareNullableValues(
        readRowValue(left, toColumnKey(term.source)),
        readRowValue(right, toColumnKey(term.source)),
      );
      if (comparison !== 0) {
        return term.direction === "asc" ? comparison : -comparison;
      }
    }
    return 0;
  });

  return sorted;
}

async function executeDrizzleRelLimitOffset<TContext>(
  limitOffset: Extract<RelNode, { kind: "limit_offset" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  let rows = await executeDrizzleRelNode(limitOffset.input, context);

  if (limitOffset.offset != null) {
    rows = rows.slice(limitOffset.offset);
  }

  if (limitOffset.limit != null) {
    rows = rows.slice(0, limitOffset.limit);
  }

  return rows;
}

async function executeDrizzleRelSetOp<TContext>(
  setOp: Extract<RelNode, { kind: "set_op" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const leftRows = await executeDrizzleRelNode(setOp.left, context);
  const rightRows = await executeDrizzleRelNode(setOp.right, context);

  switch (setOp.op) {
    case "union_all":
      return [...leftRows, ...rightRows];
    case "union":
      return dedupeRows([...leftRows, ...rightRows]);
    case "intersect": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => rightKeys.has(stableRowKey(row))));
    }
    case "except": {
      const rightKeys = new Set(rightRows.map((row) => stableRowKey(row)));
      return dedupeRows(leftRows.filter((row) => !rightKeys.has(stableRowKey(row))));
    }
  }
}

async function executeDrizzleRelWith<TContext>(
  withNode: Extract<RelNode, { kind: "with" }>,
  context: DrizzleRelExecutionContext<TContext>,
): Promise<QueryRow[]> {
  const cteRows = new Map(context.cteRows);
  const nested: DrizzleRelExecutionContext<TContext> = {
    ...context,
    cteRows,
  };

  for (const cte of withNode.ctes) {
    const rows = await executeDrizzleRelNode(cte.query, nested);
    cteRows.set(cte.name, rows);
  }

  return executeDrizzleRelNode(withNode.body, nested);
}

function scanLocalRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = [...rows];
  for (const clause of request.where ?? []) {
    out = out.filter((row) => matchesClause(row, clause));
  }

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const comparison = compareNullableValues(readRowValue(left, term.column), readRowValue(right, term.column));
        if (comparison !== 0) {
          return term.direction === "asc" ? comparison : -comparison;
        }
      }
      return 0;
    });
  }

  if (request.offset != null) {
    out = out.slice(request.offset);
  }

  if (request.limit != null) {
    out = out.slice(0, request.limit);
  }

  return out.map((row) => {
    const projected: QueryRow = {};
    for (const column of request.select) {
      projected[column] = readRowValue(row, column) ?? null;
    }
    return projected;
  });
}

function prefixRow(row: QueryRow, alias: string): InternalRow {
  const out: InternalRow = {};
  for (const [column, value] of Object.entries(row)) {
    out[`${alias}.${column}`] = value;
  }
  return out;
}

function matchesClause(row: Record<string, unknown>, clause: ScanFilterClause): boolean {
  const value = readRowValue(row, clause.column);

  switch (clause.op) {
    case "eq":
      return value != null && value === clause.value;
    case "neq":
      return value != null && value !== clause.value;
    case "gt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) > 0;
    case "gte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) >= 0;
    case "lt":
      return value != null && clause.value != null && compareNonNull(value, clause.value) < 0;
    case "lte":
      return value != null && clause.value != null && compareNonNull(value, clause.value) <= 0;
    case "in": {
      const set = new Set(clause.values.filter((entry) => entry != null));
      return value != null && set.has(value);
    }
    case "is_null":
      return value == null;
    case "is_not_null":
      return value != null;
    default:
      return false;
  }
}

function evaluateAggregateMetric(
  fn: "count" | "sum" | "avg" | "min" | "max",
  values: unknown[],
  bucketSize: number,
  hasColumn: boolean,
): unknown {
  switch (fn) {
    case "count":
      return hasColumn ? values.filter((value) => value != null).length : bucketSize;
    case "sum": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "SUM"));
      return numeric.length > 0 ? numeric.reduce((sum, value) => sum + value, 0) : null;
    }
    case "avg": {
      const numeric = values.filter((value) => value != null).map((value) => toFiniteNumber(value, "AVG"));
      return numeric.length > 0
        ? numeric.reduce((sum, value) => sum + value, 0) / numeric.length
        : null;
    }
    case "min": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) <= 0 ? left : right,
          )
        : null;
    }
    case "max": {
      const candidates = values.filter((value) => value != null);
      return candidates.length > 0
        ? candidates.reduce((left, right) =>
            compareNullableValues(left, right) >= 0 ? left : right,
          )
        : null;
    }
  }
}

function dedupeRows(rows: QueryRow[]): QueryRow[] {
  const byKey = new Map<string, QueryRow>();
  for (const row of rows) {
    byKey.set(stableRowKey(row), row);
  }
  return [...byKey.values()];
}

function stableRowKey(row: QueryRow): string {
  const entries = Object.entries(row).sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(entries);
}

function readRowValue(row: Record<string, unknown>, column: string): unknown {
  if (column in row) {
    return row[column];
  }

  const suffix = `.${column}`;
  const candidates = Object.entries(row).filter(([key]) => key.endsWith(suffix));
  if (candidates.length === 1) {
    return candidates[0]?.[1];
  }

  return undefined;
}

function toColumnKey(ref: { alias?: string; table?: string; column: string }): string {
  const prefix = ref.alias ?? ref.table;
  return prefix ? `${prefix}.${ref.column}` : ref.column;
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

function compareNonNull(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    return leftNumber === rightNumber ? 0 : leftNumber < rightNumber ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  if (leftString === rightString) {
    return 0;
  }
  return leftString < rightString ? -1 : 1;
}

function toFiniteNumber(value: unknown, functionName: "SUM" | "AVG"): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${functionName} expects numeric values.`);
  }
  return parsed;
}
