import type { SchemaDefinition } from "@sqlql/core";
import type { PlannedQuery } from "@sqlql/sql";

export type Row = Record<string, unknown>;
export type TableData = Record<string, Row[]>;

export interface ExecuteInput {
  schema: SchemaDefinition;
  data: TableData;
  plan: PlannedQuery;
}

export function executeInMemory(input: ExecuteInput): Row[] {
  const tableExists = Object.prototype.hasOwnProperty.call(input.schema.tables, input.plan.source);
  if (!tableExists) {
    throw new Error(`Unknown table: ${input.plan.source}`);
  }

  return input.data[input.plan.source] ?? [];
}
