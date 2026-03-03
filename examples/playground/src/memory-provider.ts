import {
  defineProviders,
  type ProviderAdapter,
  type ProviderFragment,
  type ProvidersMap,
  type QueryRow,
  type ScanFilterClause,
  type SchemaDefinition,
  type TableScanRequest,
} from "sqlql";

function applyFilters(row: QueryRow, filters: ScanFilterClause[]): boolean {
  for (const clause of filters) {
    const value = row[clause.column];
    switch (clause.op) {
      case "eq":
        if (value !== clause.value) {
          return false;
        }
        break;
      case "neq":
        if (value === clause.value) {
          return false;
        }
        break;
      case "gt":
        if (value == null || clause.value == null || Number(value) <= Number(clause.value)) {
          return false;
        }
        break;
      case "gte":
        if (value == null || clause.value == null || Number(value) < Number(clause.value)) {
          return false;
        }
        break;
      case "lt":
        if (value == null || clause.value == null || Number(value) >= Number(clause.value)) {
          return false;
        }
        break;
      case "lte":
        if (value == null || clause.value == null || Number(value) > Number(clause.value)) {
          return false;
        }
        break;
      case "in":
        if (!clause.values.includes(value)) {
          return false;
        }
        break;
      case "is_null":
        if (value != null) {
          return false;
        }
        break;
      case "is_not_null":
        if (value == null) {
          return false;
        }
        break;
    }
  }
  return true;
}

function scanRows(rows: QueryRow[], request: TableScanRequest): QueryRow[] {
  let out = rows.filter((row) => applyFilters(row, request.where ?? []));

  if (request.orderBy && request.orderBy.length > 0) {
    out = [...out].sort((left, right) => {
      for (const term of request.orderBy ?? []) {
        const leftValue = left[term.column] ?? null;
        const rightValue = right[term.column] ?? null;
        if (leftValue === rightValue) {
          continue;
        }
        const direction = term.direction === "asc" ? 1 : -1;
        if (leftValue == null) {
          return -1 * direction;
        }
        if (rightValue == null) {
          return 1 * direction;
        }
        if (leftValue < rightValue) {
          return -1 * direction;
        }
        if (leftValue > rightValue) {
          return 1 * direction;
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
      projected[column] = row[column] ?? null;
    }
    return projected;
  });
}

export function createPlaygroundProviders(
  schema: SchemaDefinition,
  rowsByTable: Record<string, QueryRow[]>,
): ProvidersMap<object> {
  const providerNames = new Set(
    Object.values(schema.tables).map((table) => table.provider),
  );
  const providers = Object.fromEntries(
    [...providerNames].map((providerName) => {
      const adapter: ProviderAdapter<object> = {
        canExecute(fragment: ProviderFragment) {
          return fragment.kind === "scan";
        },
        async compile(fragment: ProviderFragment) {
          return {
            provider: providerName,
            kind: fragment.kind,
            payload: fragment,
          };
        },
        async execute(plan) {
          const fragment = plan.payload as ProviderFragment;
          if (fragment.kind !== "scan") {
            return [];
          }
          return scanRows(rowsByTable[fragment.table] ?? [], fragment.request);
        },
        async lookupMany(request) {
          const tableRows = rowsByTable[request.table] ?? [];
          const keySet = new Set(request.keys);
          return tableRows
            .filter((row) => keySet.has(row[request.key]))
            .map((row) => {
              const projected: QueryRow = {};
              for (const column of request.select) {
                projected[column] = row[column] ?? null;
              }
              return projected;
            });
        },
      };

      return [providerName, adapter] as const;
    }),
  );

  return defineProviders(providers);
}
