import type {
  ProviderAdapter,
  ProviderCapabilityReport,
  ProviderCompiledPlan,
  ProviderFragment,
  ProviderLookupManyRequest,
  QueryRow,
  ScanFilterClause,
  TableScanRequest,
} from "sqlql";

export interface KnexLike {
  table(name: string): any;
  raw(sql: string, params?: unknown[]): Promise<unknown>;
}

export interface ObjectionProviderTableConfig<TContext> {
  scope?: (query: any, context: TContext, table: string) => void | Promise<void>;
}

export interface CreateObjectionProviderOptions<TContext> {
  knex: KnexLike;
  tables?: Record<string, ObjectionProviderTableConfig<TContext>>;
}

export function createObjectionProvider<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
): ProviderAdapter<TContext> {
  return {
    canExecute(fragment): boolean | ProviderCapabilityReport {
      return fragment.kind === "scan" || fragment.kind === "sql_query";
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      return {
        provider: "objection",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      const fragment = plan.payload as ProviderFragment;

      switch (fragment.kind) {
        case "sql_query":
          return executeRawSql(options.knex, fragment.sql);
        case "scan":
          return executeScan(options, fragment.request, context);
        default:
          throw new Error(`Unsupported objection compiled plan kind: ${fragment.kind}`);
      }
    },
    async lookupMany(request, context): Promise<QueryRow[]> {
      const scanRequest: TableScanRequest = {
        table: request.table,
        select: request.select,
        where: [
          ...(request.where ?? []),
          {
            op: "in",
            column: request.key,
            values: request.keys,
          } as ScanFilterClause,
        ],
      };

      return executeScan(options, scanRequest, context);
    },
  };
}

async function executeScan<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  let qb = options.knex.table(request.table).select(request.select);

  const tableScope = options.tables?.[request.table]?.scope;
  if (tableScope) {
    await tableScope(qb, context, request.table);
  }

  for (const clause of request.where ?? []) {
    qb = applyFilter(qb, clause);
  }

  for (const term of request.orderBy ?? []) {
    qb = qb.orderBy(term.column, term.direction);
  }

  if (request.limit != null) {
    qb = qb.limit(request.limit);
  }

  if (request.offset != null) {
    qb = qb.offset(request.offset);
  }

  const rows = await qb;
  return Array.isArray(rows) ? rows : [];
}

async function executeRawSql(knex: KnexLike, sql: string): Promise<QueryRow[]> {
  const result = await knex.raw(sql);

  if (Array.isArray(result)) {
    const first = result[0];
    return Array.isArray(first) ? (first as QueryRow[]) : [];
  }

  if (result && typeof result === "object" && "rows" in result) {
    const rows = (result as { rows?: unknown }).rows;
    return Array.isArray(rows) ? (rows as QueryRow[]) : [];
  }

  return [];
}

function applyFilter(qb: any, clause: ScanFilterClause): any {
  switch (clause.op) {
    case "eq":
      return qb.where(clause.column, "=", clause.value);
    case "neq":
      return qb.where(clause.column, "!=", clause.value);
    case "gt":
      return qb.where(clause.column, ">", clause.value);
    case "gte":
      return qb.where(clause.column, ">=", clause.value);
    case "lt":
      return qb.where(clause.column, "<", clause.value);
    case "lte":
      return qb.where(clause.column, "<=", clause.value);
    case "in":
      return qb.whereIn(clause.column, clause.values);
    case "is_null":
      return qb.whereNull(clause.column);
    case "is_not_null":
      return qb.whereNotNull(clause.column);
  }
}

