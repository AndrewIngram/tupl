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

export interface KyselySqlExecutor<TContext> {
  executeSql(args: {
    sql: string;
    params: unknown[];
    context: TContext;
  }): Promise<QueryRow[]>;
}

export interface KyselyProviderTableConfig<TContext> {
  scopeSql?: (context: TContext, table: string) =>
    | { sql: string; params?: unknown[] }
    | undefined
    | Promise<{ sql: string; params?: unknown[] } | undefined>;
}

export interface CreateKyselyProviderOptions<TContext> {
  executor: KyselySqlExecutor<TContext>;
  tables?: Record<string, KyselyProviderTableConfig<TContext>>;
}

export function createKyselyProvider<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
): ProviderAdapter<TContext> {
  return {
    canExecute(fragment): boolean | ProviderCapabilityReport {
      return fragment.kind === "scan" || fragment.kind === "sql_query";
    },
    async compile(fragment): Promise<ProviderCompiledPlan> {
      return {
        provider: "kysely",
        kind: fragment.kind,
        payload: fragment,
      };
    },
    async execute(plan, context): Promise<QueryRow[]> {
      const fragment = plan.payload as ProviderFragment;

      switch (fragment.kind) {
        case "sql_query":
          return options.executor.executeSql({
            sql: fragment.sql,
            params: [],
            context,
          });
        case "scan": {
          const compiled = await compileScanSql(options, fragment.request, context);
          return options.executor.executeSql({
            sql: compiled.sql,
            params: compiled.params,
            context,
          });
        }
        default:
          throw new Error(`Unsupported kysely compiled plan kind: ${fragment.kind}`);
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

      const compiled = await compileScanSql(options, scanRequest, context);
      return options.executor.executeSql({
        sql: compiled.sql,
        params: compiled.params,
        context,
      });
    },
  };
}

async function compileScanSql<TContext>(
  options: CreateKyselyProviderOptions<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<{ sql: string; params: unknown[] }> {
  const params: unknown[] = [];
  const whereParts: string[] = [];

  for (const clause of request.where ?? []) {
    switch (clause.op) {
      case "eq":
      case "neq":
      case "gt":
      case "gte":
      case "lt":
      case "lte": {
        const operator =
          clause.op === "eq"
            ? "="
            : clause.op === "neq"
              ? "!="
              : clause.op === "gt"
                ? ">"
                : clause.op === "gte"
                  ? ">="
                  : clause.op === "lt"
                    ? "<"
                    : "<=";
        whereParts.push(`${escapeIdentifier(clause.column)} ${operator} ?`);
        params.push(clause.value);
        break;
      }
      case "in": {
        if (clause.values.length === 0) {
          whereParts.push("1 = 0");
          break;
        }

        const placeholders = clause.values.map(() => "?").join(", ");
        whereParts.push(`${escapeIdentifier(clause.column)} IN (${placeholders})`);
        params.push(...clause.values);
        break;
      }
      case "is_null":
        whereParts.push(`${escapeIdentifier(clause.column)} IS NULL`);
        break;
      case "is_not_null":
        whereParts.push(`${escapeIdentifier(clause.column)} IS NOT NULL`);
        break;
    }
  }

  const scope = await options.tables?.[request.table]?.scopeSql?.(context, request.table);
  if (scope?.sql) {
    whereParts.push(`(${scope.sql})`);
    params.push(...(scope.params ?? []));
  }

  const selectSql = request.select.map(escapeIdentifier).join(", ");
  let sql = `SELECT ${selectSql} FROM ${escapeIdentifier(request.table)}`;

  if (whereParts.length > 0) {
    sql += ` WHERE ${whereParts.join(" AND ")}`;
  }

  if (request.orderBy && request.orderBy.length > 0) {
    sql += ` ORDER BY ${request.orderBy
      .map((term) => `${escapeIdentifier(term.column)} ${term.direction.toUpperCase()}`)
      .join(", ")}`;
  }

  if (request.limit != null) {
    sql += ` LIMIT ${request.limit}`;
  }

  if (request.offset != null) {
    sql += ` OFFSET ${request.offset}`;
  }

  return {
    sql,
    params,
  };
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
