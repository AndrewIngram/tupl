import {
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
import type { BetterSQLite3Database } from "drizzle-orm/better-sqlite3";
import type { AnySQLiteColumn } from "drizzle-orm/sqlite-core";
import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  QueryRow,
  ScanFilterClause,
  ScanOrderBy,
  TableScanRequest,
} from "sqlql";

export type DrizzleColumnMap<TColumn extends string = string> = Record<TColumn, AnySQLiteColumn>;

export interface DrizzleProviderTableConfig<
  TContext,
  TColumn extends string = string,
> {
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  scope?:
    | ((context: TContext) => SQL | SQL[] | undefined | Promise<SQL | SQL[] | undefined>)
    | undefined;
}

export interface CreateDrizzleProviderOptions<TContext> {
  db: BetterSQLite3Database;
  tables: Record<string, DrizzleProviderTableConfig<TContext, string>>;
}

export function createDrizzleProvider<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
): ProviderAdapter<TContext> {
  return {
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return !!options.tables[fragment.table];
        case "sql_query": {
          // Scope predicates are table-level closures; do not bypass them with raw SQL.
          const hasScopedTable = fragment.rel.kind === "sql"
            ? fragment.rel.tables.some((table) => !!options.tables[table]?.scope)
            : false;

          if (hasScopedTable) {
            return {
              supported: false,
              reason: "Raw SQL fragment pushdown is disabled for scoped tables.",
            };
          }

          return true;
        }
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      return {
        provider: "drizzle",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      return executeDrizzlePlan(plan, options, context);
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      return lookupManyWithDrizzle(options, request, context);
    },
  };
}

async function executeDrizzlePlan<TContext>(
  plan: ProviderCompiledPlan,
  options: CreateDrizzleProviderOptions<TContext>,
  context: TContext,
): Promise<QueryRow[]> {
  const fragment = plan.payload as ProviderFragment;

  switch (fragment.kind) {
    case "sql_query":
      return executeRawSql(options.db, fragment.sql);
    case "scan": {
      const tableConfig = options.tables[fragment.table];
      if (!tableConfig) {
        throw new Error(`Unknown drizzle table config: ${fragment.table}`);
      }

      const scope = tableConfig.scope ? await tableConfig.scope(context) : undefined;
      return runDrizzleScan({
        db: options.db,
        tableName: fragment.table,
        table: tableConfig.table,
        columns: tableConfig.columns,
        request: fragment.request,
        scope,
      });
    }
    default:
      throw new Error(`Unsupported drizzle compiled plan kind: ${fragment.kind}`);
  }
}

function executeRawSql(db: BetterSQLite3Database, sqlText: string): QueryRow[] {
  return db.$client.prepare(sqlText).all() as QueryRow[];
}

async function lookupManyWithDrizzle<TContext>(
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const tableConfig = options.tables[request.table];
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
    columns: tableConfig.columns,
    request: {
      table: request.table,
      select: request.select,
      where,
    },
    scope,
  });
}

export interface RunDrizzleScanOptions<TTable extends string, TColumn extends string> {
  db: BetterSQLite3Database;
  tableName: TTable;
  table: object;
  columns: DrizzleColumnMap<TColumn>;
  request: TableScanRequest<TTable, TColumn>;
  scope?: SQL | SQL[];
}

export function runDrizzleScan<TTable extends string, TColumn extends string>(
  options: RunDrizzleScanOptions<TTable, TColumn>,
): QueryRow[] {
  const selection = buildSelection(options.request.select, options.columns, options.tableName);
  const filterConditions = (options.request.where ?? []).map((clause) =>
    toSqlCondition(clause, options.columns, options.tableName),
  );
  const scopeConditions = normalizeScope(options.scope);
  const whereConditions = [...scopeConditions, ...filterConditions];

  let builder = options.db.select(selection).from(options.table as never) as {
    where: (condition: SQL) => unknown;
    orderBy: (...clauses: SQL[]) => unknown;
    limit: (value: number) => unknown;
    offset: (value: number) => unknown;
    all: () => QueryRow[];
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

  return builder.all();
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
): Record<TColumn, AnySQLiteColumn> {
  const out = {} as Record<TColumn, AnySQLiteColumn>;
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
