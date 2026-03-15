import type { QueryRow, ScanFilterClause } from "@tupl/provider-kit";
import type { ProviderLookupManyRequest } from "@tupl/provider-kit/shapes";

import type { CreateDrizzleProviderOptions } from "../types";
import type { DrizzleQueryExecutor } from "../types";
import { executeScan } from "./scan-execution";

export async function executeLookupMany<TContext>(
  db: DrizzleQueryExecutor,
  options: CreateDrizzleProviderOptions<TContext>,
  request: ProviderLookupManyRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const where: ScanFilterClause[] = [
    ...(request.where ?? []),
    {
      op: "in",
      column: request.key,
      values: request.keys,
    } as ScanFilterClause,
  ];

  return executeScan(
    db,
    options,
    {
      table: request.table,
      select: request.select,
      where,
    },
    context,
  );
}
