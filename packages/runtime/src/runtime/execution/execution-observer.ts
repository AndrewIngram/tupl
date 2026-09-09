import type { RelNode } from "@tupl/foundation";
import type { QueryRow } from "@tupl/schema-model";

import type { QueryStepKind, QueryStepRoute } from "../session/contracts";

export interface RelExecutionDescriptor {
  kind: QueryStepKind;
  summary: string;
  routeUsed: QueryStepRoute;
  inputRowCount?: number;
  computations?: Array<{ id: string; label: string; invocations: number }>;
}

export interface RelExecutionObservation {
  updateDescriptor(descriptor: RelExecutionDescriptor): void;
  complete(rows: QueryRow[]): void;
  fail(error: unknown): void;
}

export interface RelExecutionObserver {
  start(node: RelNode, descriptor: RelExecutionDescriptor): RelExecutionObservation;
  close(error?: unknown): void;
}

export function describeRelExecution(node: RelNode): RelExecutionDescriptor {
  switch (node.kind) {
    case "scan":
      return {
        kind: "scan",
        summary: `Scan ${node.alias ?? node.table} (${node.table})`,
        routeUsed: "scan",
      };
    case "cte_ref":
      return {
        kind: "scan",
        summary: `Read CTE ${node.alias ?? node.name} (${node.name})`,
        routeUsed: "local",
      };
    case "values":
      return {
        kind: "projection",
        summary: "Materialize literal rows",
        routeUsed: "local",
      };
    case "filter":
      return { kind: "filter", summary: "Apply WHERE filter", routeUsed: "local" };
    case "project":
      return { kind: "projection", summary: "Project result rows", routeUsed: "local" };
    case "aggregate":
      return {
        kind: "aggregate",
        summary: "Compute grouped aggregates",
        routeUsed: "local",
      };
    case "window":
      return { kind: "window", summary: "Compute window functions", routeUsed: "local" };
    case "sort":
      return { kind: "order", summary: "Order result rows", routeUsed: "local" };
    case "limit_offset":
      return { kind: "limit_offset", summary: "Apply LIMIT/OFFSET", routeUsed: "local" };
    case "join":
      return {
        kind: "join",
        summary: `${node.joinType.toUpperCase()} join`,
        routeUsed: "local",
      };
    case "set_op":
      return {
        kind: "projection",
        summary: `Apply set operation (${node.op})`,
        routeUsed: "local",
      };
    case "with":
      return { kind: "projection", summary: "Finalize WITH query", routeUsed: "local" };
    case "repeat_union":
      return {
        kind: "projection",
        summary: `Recursive CTE (${node.cteName})`,
        routeUsed: "local",
      };
    case "correlate":
      return {
        kind: "projection",
        summary: "Correlated subquery rewrite",
        routeUsed: "local",
      };
  }
}

export function describeProviderFragmentExecution(providerName: string): RelExecutionDescriptor {
  return {
    kind: "remote_fragment",
    summary: `Execute provider fragment (${providerName})`,
    routeUsed: "provider_fragment",
  };
}

export function describeLookupJoinExecution(input: {
  leftTable: string;
  leftKey: string;
  rightTable: string;
  rightKey: string;
}): RelExecutionDescriptor {
  return {
    kind: "lookup_join",
    summary: `Lookup join ${input.leftTable}.${input.leftKey} -> ${input.rightTable}.${input.rightKey}`,
    routeUsed: "lookup_join",
  };
}
