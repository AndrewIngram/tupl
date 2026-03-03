import type {
  QueryRow,
  ScanFilterClause,
  SchemaDefinition,
  TableAggregateRequest,
  TableScanRequest,
} from "./schema";
import type { RelNode } from "./rel";

export type MaybePromise<T> = T | Promise<T>;

export interface ProviderCapabilityReport {
  supported: boolean;
  reason?: string;
  notes?: string[];
}

export interface ProviderEstimate {
  rows: number;
  cost: number;
}

export interface ProviderCompiledPlan {
  provider: string;
  kind: string;
  payload: unknown;
}

export interface ProviderLookupManyRequest {
  table: string;
  key: string;
  keys: unknown[];
  select: string[];
  where?: ScanFilterClause[];
}

export interface SqlQueryProviderFragment {
  kind: "sql_query";
  provider: string;
  sql: string;
  rel: RelNode;
}

export interface RelProviderFragment {
  kind: "rel";
  provider: string;
  rel: RelNode;
}

export interface ScanProviderFragment {
  kind: "scan";
  provider: string;
  table: string;
  request: TableScanRequest;
}

export interface AggregateProviderFragment {
  kind: "aggregate";
  provider: string;
  table: string;
  request: TableAggregateRequest;
}

export type ProviderFragment =
  | SqlQueryProviderFragment
  | RelProviderFragment
  | ScanProviderFragment
  | AggregateProviderFragment;

export interface ProviderAdapter<TContext = unknown> {
  canExecute(fragment: ProviderFragment, context: TContext): MaybePromise<boolean | ProviderCapabilityReport>;
  compile(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderCompiledPlan>;
  execute(plan: ProviderCompiledPlan, context: TContext): Promise<QueryRow[]>;
  lookupMany?(request: ProviderLookupManyRequest, context: TContext): Promise<QueryRow[]>;
  estimate?(fragment: ProviderFragment, context: TContext): MaybePromise<ProviderEstimate>;
}

export type ProvidersMap<TContext = unknown> = Record<string, ProviderAdapter<TContext>>;

export function defineProviders<TContext, TProviders extends ProvidersMap<TContext>>(
  providers: TProviders,
): TProviders {
  return providers;
}

export function normalizeCapability(
  capability: boolean | ProviderCapabilityReport,
): ProviderCapabilityReport {
  if (typeof capability === "boolean") {
    return capability ? { supported: true } : { supported: false };
  }

  return capability;
}

export function resolveTableProvider(schema: SchemaDefinition, table: string): string {
  const tableDefinition = schema.tables[table];
  if (!tableDefinition) {
    throw new Error(`Unknown table: ${table}`);
  }

  if (!tableDefinition.provider || tableDefinition.provider.length === 0) {
    throw new Error(`Table ${table} is missing required provider mapping.`);
  }

  return tableDefinition.provider;
}

export function validateProviderBindings<TContext>(
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): void {
  for (const tableName of Object.keys(schema.tables)) {
    const providerName = resolveTableProvider(schema, tableName);
    if (!providers[providerName]) {
      throw new Error(
        `Table ${tableName} is bound to provider ${providerName}, but no such provider is registered.`,
      );
    }
  }
}
