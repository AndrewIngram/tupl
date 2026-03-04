import {
  createDataEntityHandle,
  type DataEntityHandle,
  type ProviderAdapter,
  type ProviderCapabilityReport,
  type ProviderCompiledPlan,
  type ProviderFragment,
  type ProviderLookupManyRequest,
  type QueryRow,
  type ScanFilterClause,
  type TableScanRequest,
} from "sqlql";

export interface KnexLike {
  table(name: string): any;
  raw(sql: string, params?: unknown[]): Promise<unknown>;
}

export interface ObjectionProviderTableConfig<TContext> {
  scope?: (query: any, context: TContext, table: string) => void | Promise<void>;
}

export interface ObjectionProviderEntityConfig<TContext> {
  /**
   * Builds the mandatory scoped root query for this entity.
   */
  base: (context: TContext) => any | Promise<any>;
}

export interface CreateObjectionProviderOptions<TContext> {
  name?: string;
  knex: KnexLike;
  /**
   * Source-neutral entity registry for the new lens model.
   */
  entities?: Record<string, ObjectionProviderEntityConfig<TContext>>;
  /**
   * @deprecated Legacy table-level scope callbacks.
   */
  tables?: Record<string, ObjectionProviderTableConfig<TContext>>;
}

export function createObjectionProvider<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
): ProviderAdapter<TContext> & {
  entities: Record<string, DataEntityHandle>;
  tables: Record<string, DataEntityHandle>;
} {
  const providerName = options.name ?? "objection";

  const handles: Record<string, DataEntityHandle> = Object.fromEntries(
    Object.keys(options.entities ?? {}).map((entityName) => [
      entityName,
      createDataEntityHandle({
        entity: entityName,
        provider: providerName,
      }),
    ]),
  );

  const adapter: ProviderAdapter<TContext> & {
    entities: Record<string, DataEntityHandle>;
    tables: Record<string, DataEntityHandle>;
  } = {
    entities: handles,
    tables: handles,
    canExecute(fragment): boolean | ProviderCapabilityReport {
      switch (fragment.kind) {
        case "scan":
          return true;
        case "sql_query":
          return true;
        case "rel":
          return {
            supported: false,
            reason: "Objection provider currently executes rel nodes through scan-based planning.",
          };
        default:
          return false;
      }
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      if (fragment.kind === "rel") {
        throw new Error("Objection provider does not directly compile rel fragments in this version.");
      }

      return {
        provider: providerName,
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      switch (plan.kind) {
        case "sql_query": {
          const fragment = plan.payload as Extract<ProviderFragment, { kind: "sql_query" }>;
          return executeRawSql(options.knex, fragment.sql);
        }
        case "scan": {
          const fragment = plan.payload as Extract<ProviderFragment, { kind: "scan" }>;
          return executeScan(options, fragment.request, context);
        }
        default:
          throw new Error(`Unsupported objection compiled plan kind: ${plan.kind}`);
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

  return adapter;
}

async function executeScan<TContext>(
  options: CreateObjectionProviderOptions<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const entityConfig = options.entities?.[request.table];

  let qb: any;
  if (entityConfig) {
    qb = await entityConfig.base(context);
    if (qb?.clone && typeof qb.clone === "function") {
      qb = qb.clone();
    }
    qb = qb.select(request.select);
  } else {
    qb = options.knex.table(request.table).select(request.select);

    const tableScope = options.tables?.[request.table]?.scope;
    if (tableScope) {
      await tableScope(qb, context, request.table);
    }
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
