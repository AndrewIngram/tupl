import type { AggregateFunction, ScanFilterClause, ScanOrderBy, SchemaDefinition } from "./schema";

export type RelConvention = `provider:${string}` | "local";

export interface RelColumnRef {
  table?: string;
  alias?: string;
  column: string;
}

export interface RelLiteralExpr {
  kind: "literal";
  value: string | number | boolean | null;
}

export interface RelColumnExpr {
  kind: "column";
  ref: RelColumnRef;
}

export interface RelFunctionExpr {
  kind: "function";
  name: string;
  args: RelExpr[];
}

export type RelExpr = RelLiteralExpr | RelColumnExpr | RelFunctionExpr;

export interface RelOutputColumn {
  name: string;
  type?: string;
  nullable?: boolean;
}

export interface RelNodeBase {
  id: string;
  convention: RelConvention;
  output: RelOutputColumn[];
}

export interface RelScanNode extends RelNodeBase {
  kind: "scan";
  table: string;
  alias?: string;
  select: string[];
  where?: ScanFilterClause[];
  orderBy?: ScanOrderBy[];
  limit?: number;
  offset?: number;
}

export interface RelFilterNode extends RelNodeBase {
  kind: "filter";
  input: RelNode;
  where: ScanFilterClause[];
}

export interface RelProjectNode extends RelNodeBase {
  kind: "project";
  input: RelNode;
  select: string[];
}

export interface RelJoinNode extends RelNodeBase {
  kind: "join";
  joinType: "inner" | "left" | "right" | "full";
  left: RelNode;
  right: RelNode;
  leftKey: RelColumnRef;
  rightKey: RelColumnRef;
}

export interface RelAggregateMetric {
  fn: AggregateFunction;
  as: string;
  column?: RelColumnRef;
  distinct?: boolean;
}

export interface RelAggregateNode extends RelNodeBase {
  kind: "aggregate";
  input: RelNode;
  groupBy: RelColumnRef[];
  metrics: RelAggregateMetric[];
}

export interface RelSortNode extends RelNodeBase {
  kind: "sort";
  input: RelNode;
  orderBy: ScanOrderBy[];
}

export interface RelLimitOffsetNode extends RelNodeBase {
  kind: "limit_offset";
  input: RelNode;
  limit?: number;
  offset?: number;
}

export interface RelSetOpNode extends RelNodeBase {
  kind: "set_op";
  op: "union" | "union_all" | "intersect" | "except";
  left: RelNode;
  right: RelNode;
}

export interface RelWithNode extends RelNodeBase {
  kind: "with";
  ctes: Array<{
    name: string;
    query: RelNode;
  }>;
  body: RelNode;
}

export interface RelSqlNode extends RelNodeBase {
  kind: "sql";
  sql: string;
  tables: string[];
}

export type RelNode =
  | RelScanNode
  | RelFilterNode
  | RelProjectNode
  | RelJoinNode
  | RelAggregateNode
  | RelSortNode
  | RelLimitOffsetNode
  | RelSetOpNode
  | RelWithNode
  | RelSqlNode;

let relIdCounter = 0;

function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

export function createSqlRel(sql: string, tables: string[]): RelSqlNode {
  return {
    id: nextRelId("sql"),
    kind: "sql",
    convention: "local",
    sql,
    tables,
    output: [],
  };
}

export function countRelNodes(node: RelNode): number {
  switch (node.kind) {
    case "scan":
    case "sql":
      return 1;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return 1 + countRelNodes(node.input);
    case "join":
    case "set_op":
      return 1 + countRelNodes(node.left) + countRelNodes(node.right);
    case "with":
      return (
        1 +
        node.ctes.reduce((sum, cte) => sum + countRelNodes(cte.query), 0) +
        countRelNodes(node.body)
      );
  }
}

export function collectRelTables(node: RelNode): string[] {
  const out = new Set<string>();

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan":
        out.add(current.table);
        return;
      case "sql":
        for (const table of current.tables) {
          out.add(table);
        }
        return;
      case "filter":
      case "project":
      case "aggregate":
      case "sort":
      case "limit_offset":
        visit(current.input);
        return;
      case "join":
      case "set_op":
        visit(current.left);
        visit(current.right);
        return;
      case "with":
        for (const cte of current.ctes) {
          visit(cte.query);
        }
        visit(current.body);
        return;
    }
  };

  visit(node);
  return [...out];
}

export function validateRelAgainstSchema(node: RelNode, schema: SchemaDefinition): void {
  const tables = collectRelTables(node);
  for (const table of tables) {
    if (!schema.tables[table]) {
      throw new Error(`Unknown table in relational plan: ${table}`);
    }
  }
}
