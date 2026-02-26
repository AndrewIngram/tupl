import {
  getTable,
  resolveTableQueryBehavior,
  type QueryRow,
  type ScanFilterClause,
  type ScanOrderBy,
  type SchemaDefinition,
  type TableMethods,
  type TableMethodsMap,
  type TableScanRequest,
} from "@sqlql/core";
import nodeSqlParser from "node-sql-parser";

export interface SqlQuery {
  text: string;
}

export interface PlannedQuery {
  source: string;
  selectAll: boolean;
}

export interface QueryInput<TContext> {
  schema: SchemaDefinition;
  methods: TableMethodsMap<TContext>;
  context: TContext;
  sql: string;
}

interface TableBinding {
  table: string;
  alias: string;
  index: number;
}

interface JoinCondition {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface ParsedJoin {
  alias: string;
  join: "inner";
  condition: JoinCondition;
}

interface SelectColumn {
  alias: string;
  column: string;
  output: string;
}

interface OrderColumn {
  alias: string;
  column: string;
  direction: "asc" | "desc";
}

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

interface ParsedSelectQuery {
  bindings: TableBinding[];
  joins: ParsedJoin[];
  joinEdges: JoinCondition[];
  filters: LiteralFilter[];
  selectAll: boolean;
  selectColumns: SelectColumn[];
  orderBy: OrderColumn[];
  limit?: number;
}

const { Parser } = nodeSqlParser as { Parser: new () => { astify: (sql: string) => unknown } };
const parser = new Parser();

export function parseSql(query: SqlQuery, schema: SchemaDefinition): PlannedQuery {
  const parsed = parseSelectAst(query.text, schema);
  const source = parsed.bindings[0];
  if (!source) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  return {
    source: source.table,
    selectAll: parsed.selectAll,
  };
}

export async function query<TContext>(input: QueryInput<TContext>): Promise<QueryRow[]> {
  const parsed = parseSelectAst(input.sql, input.schema);
  const rootBinding = parsed.bindings[0];
  if (!rootBinding) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  for (const binding of parsed.bindings) {
    getTable(input.schema, binding.table);
    if (!input.methods[binding.table]) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
    }
  }

  const projectionByAlias = buildProjection(parsed, input.schema);
  const filtersByAlias = groupFiltersByAlias(parsed.filters);
  const executionOrder = buildExecutionOrder(parsed.bindings, parsed.joinEdges, filtersByAlias);
  const rowsByAlias = new Map<string, QueryRow[]>();

  for (const alias of executionOrder) {
    const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
    if (!binding) {
      throw new Error(`Unknown alias in execution order: ${alias}`);
    }

    const dependencyFilters = buildDependencyFilters(alias, parsed.joinEdges, rowsByAlias);
    const localFilters = filtersByAlias.get(alias) ?? [];

    if (dependencyFilters.some((filter) => filter.op === "in" && filter.values.length === 0)) {
      rowsByAlias.set(alias, []);
      continue;
    }

    const tableBehavior = resolveTableQueryBehavior(input.schema, binding.table);
    const defaultMaxRows = tableBehavior.maxRows;
    const requestWhere: ScanFilterClause[] = [...localFilters, ...dependencyFilters];

    const canPushFinalSort =
      parsed.bindings.length === 1 && parsed.orderBy.every((term) => term.alias === alias);
    const requestOrderBy: ScanOrderBy[] | undefined = canPushFinalSort
      ? parsed.orderBy.map((term) => ({
          column: term.column,
          direction: term.direction,
        }))
      : undefined;

    const canPushFinalLimit = parsed.bindings.length === 1;
    let requestLimit = canPushFinalLimit ? parsed.limit : undefined;
    if (requestLimit == null && defaultMaxRows != null) {
      requestLimit = defaultMaxRows;
    }
    if (requestLimit != null && defaultMaxRows != null && requestLimit > defaultMaxRows) {
      throw new Error(
        `Requested limit ${requestLimit} exceeds maxRows ${defaultMaxRows} for table ${binding.table}`,
      );
    }

    const method = input.methods[binding.table];
    if (!method) {
      throw new Error(`No table methods registered for table: ${binding.table}`);
    }

    const projection = projectionByAlias.get(alias);
    if (!projection) {
      throw new Error(`Unable to resolve projection columns for alias: ${alias}`);
    }

    const request: TableScanRequest = {
      table: binding.table,
      alias,
      select: [...projection],
    };
    if (requestWhere.length > 0) {
      request.where = requestWhere;
    }
    if (requestOrderBy && requestOrderBy.length > 0) {
      request.orderBy = requestOrderBy;
    }
    if (requestLimit != null) {
      request.limit = requestLimit;
    }

    const rows = await runScan(method, request, input.context);
    rowsByAlias.set(alias, rows);
  }

  let joinedRows = initializeJoinedRows(rowsByAlias, rootBinding.alias);
  for (const join of parsed.joins) {
    joinedRows = applyInnerJoin(joinedRows, join, rowsByAlias);
  }

  if (parsed.orderBy.length > 0) {
    joinedRows = applyFinalSort(joinedRows, parsed.orderBy);
  }

  if (parsed.limit != null && parsed.bindings.length > 1) {
    joinedRows = joinedRows.slice(0, parsed.limit);
  }

  return projectResultRows(joinedRows, parsed);
}

async function runScan<TContext>(
  method: TableMethods<TContext>,
  request: TableScanRequest,
  context: TContext,
): Promise<QueryRow[]> {
  const dependencyFilters = request.where?.filter((clause) => clause.op === "in") ?? [];

  if (
    dependencyFilters.length === 1 &&
    method.lookup &&
    dependencyFilters[0] &&
    dependencyFilters[0].values.length > 0 &&
    request.orderBy == null &&
    request.limit == null
  ) {
    const lookup = dependencyFilters[0];
    if (!lookup) {
      return method.scan(request, context);
    }

    const nonDependencyFilters = request.where?.filter((clause) => clause !== lookup);
    const lookupRequest = {
      table: request.table,
      key: lookup.column,
      values: lookup.values,
      select: request.select,
    } as const;
    const fullLookupRequest: Parameters<NonNullable<typeof method.lookup>>[0] = {
      ...lookupRequest,
    };
    if (request.alias) {
      fullLookupRequest.alias = request.alias;
    }
    if (nonDependencyFilters && nonDependencyFilters.length > 0) {
      fullLookupRequest.where = nonDependencyFilters;
    }
    return method.lookup(fullLookupRequest, context);
  }

  return method.scan(request, context);
}

function parseSelectAst(sql: string, _schema: SchemaDefinition): ParsedSelectQuery {
  const astRaw = parser.astify(sql);
  if (Array.isArray(astRaw)) {
    throw new Error("Only a single SQL statement is supported.");
  }

  const ast = astRaw as {
    type?: unknown;
    from?: unknown;
    where?: unknown;
    columns?: unknown;
    orderby?: unknown;
    limit?: unknown;
  };

  if (ast.type !== "select") {
    throw new Error("Only SELECT statements are currently supported.");
  }

  const rawFrom: unknown[] = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (rawFrom.length === 0) {
    throw new Error("SELECT queries must include a FROM clause.");
  }

  const bindings = rawFrom.map((entry: unknown, index: number) => {
    if (!entry || typeof entry !== "object" || !("table" in entry)) {
      throw new Error("Unsupported FROM clause entry.");
    }

    const table = (entry as { table?: unknown }).table;
    const alias = (entry as { as?: unknown }).as;
    if (typeof table !== "string" || table.length === 0) {
      throw new Error("Unable to resolve table name from query.");
    }

    return {
      table,
      alias: typeof alias === "string" && alias.length > 0 ? alias : table,
      index,
    };
  });

  const aliasToTable = new Map(
    bindings.map((binding: TableBinding) => [binding.alias, binding.table]),
  );

  const joins: ParsedJoin[] = [];
  const joinEdges: JoinCondition[] = [];

  for (let i = 1; i < rawFrom.length; i += 1) {
    const entry = rawFrom[i] as { join?: unknown; on?: unknown; as?: unknown; table?: unknown };
    const joinType = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    if (joinType !== "INNER JOIN" && joinType !== "JOIN") {
      throw new Error(`Unsupported join type: ${String(entry.join ?? "unknown")}`);
    }

    const parsedJoin = parseJoinCondition(entry.on);
    if (!aliasToTable.has(parsedJoin.leftAlias) || !aliasToTable.has(parsedJoin.rightAlias)) {
      throw new Error("JOIN condition references an unknown table alias.");
    }

    const joinedAlias =
      typeof entry.as === "string" && entry.as.length > 0 ? entry.as : String(entry.table);
    joins.push({
      alias: joinedAlias,
      join: "inner",
      condition: parsedJoin,
    });
    joinEdges.push(parsedJoin);
  }

  const whereParts = flattenAndConditions(ast.where);
  const filters: LiteralFilter[] = [];

  for (const part of whereParts) {
    if (!part || typeof part !== "object") {
      throw new Error("Unsupported WHERE clause.");
    }

    const binary = part as { type?: unknown; operator?: unknown; left?: unknown; right?: unknown };
    if (binary.type !== "binary_expr") {
      throw new Error("Only binary predicates are supported in WHERE clauses.");
    }

    const operator = normalizeBinaryOperator(binary.operator);
    if (operator === "in") {
      const colRef = toColumnRef(binary.left);
      if (!colRef) {
        throw new Error("IN predicates must use a column on the left-hand side.");
      }

      const values = parseExpressionList(binary.right);
      filters.push({
        alias: colRef.alias,
        clause: {
          op: "in",
          column: colRef.column,
          values,
        },
      });
      continue;
    }

    const leftCol = toColumnRef(binary.left);
    const rightCol = toColumnRef(binary.right);

    if (operator === "eq" && leftCol && rightCol) {
      joinEdges.push({
        leftAlias: leftCol.alias,
        leftColumn: leftCol.column,
        rightAlias: rightCol.alias,
        rightColumn: rightCol.column,
      });
      continue;
    }

    const leftLiteral = parseLiteral(binary.left);
    const rightLiteral = parseLiteral(binary.right);

    if (leftCol && rightLiteral !== undefined) {
      filters.push({
        alias: leftCol.alias,
        clause: {
          op: operator,
          column: leftCol.column,
          value: rightLiteral,
        },
      });
      continue;
    }

    if (rightCol && leftLiteral !== undefined) {
      filters.push({
        alias: rightCol.alias,
        clause: {
          op: invertOperator(operator),
          column: rightCol.column,
          value: leftLiteral,
        },
      });
      continue;
    }

    throw new Error(
      "WHERE predicates must compare columns to literals (or column equality joins).",
    );
  }

  const selectColumnsRaw: unknown = ast.columns;
  const selectAll =
    selectColumnsRaw === "*" ||
    (Array.isArray(selectColumnsRaw) &&
      selectColumnsRaw.length === 1 &&
      isStarColumn(selectColumnsRaw[0] as { expr?: unknown }));

  const selectColumns: SelectColumn[] = [];
  if (!selectAll) {
    if (!Array.isArray(selectColumnsRaw)) {
      throw new Error("Unsupported SELECT clause.");
    }

    for (const item of selectColumnsRaw) {
      if (!item || typeof item !== "object") {
        throw new Error("Unsupported SELECT item.");
      }

      const expr = (item as { expr?: unknown }).expr;
      const colRef = toColumnRef(expr);
      if (!colRef) {
        throw new Error("Only direct column references are currently supported in SELECT.");
      }

      const as = (item as { as?: unknown }).as;
      selectColumns.push({
        alias: colRef.alias,
        column: colRef.column,
        output:
          typeof as === "string" && as.length > 0
            ? as
            : selectColumns.some((existing) => existing.column === colRef.column)
              ? `${colRef.alias}.${colRef.column}`
              : colRef.column,
      });
    }
  }

  const orderBy: OrderColumn[] = [];
  if (Array.isArray(ast.orderby)) {
    for (const item of ast.orderby) {
      const colRef = toColumnRef((item as { expr?: unknown }).expr);
      if (!colRef) {
        throw new Error("Only column references are currently supported in ORDER BY.");
      }

      const rawType = (item as { type?: unknown }).type;
      orderBy.push({
        alias: colRef.alias,
        column: colRef.column,
        direction: rawType === "DESC" ? "desc" : "asc",
      });
    }
  }

  let limit: number | undefined;
  const rawLimit = ast.limit as { value?: Array<{ value?: unknown }> } | null;
  if (rawLimit && Array.isArray(rawLimit.value) && rawLimit.value.length > 0) {
    const first = rawLimit.value[0]?.value;
    if (typeof first === "number") {
      limit = first;
    } else if (typeof first === "string") {
      const parsed = Number(first);
      if (Number.isFinite(parsed)) {
        limit = parsed;
      }
    }
    if (limit == null) {
      throw new Error("Unable to parse LIMIT value.");
    }
  }

  if (selectAll && bindings.length > 1) {
    // Ambiguous wildcard expansion is easy to misuse across joins.
    throw new Error("SELECT * is only supported for single-table queries.");
  }

  const parsedQuery: ParsedSelectQuery = {
    bindings,
    joins,
    joinEdges: uniqueJoinEdges(joinEdges),
    filters,
    selectAll,
    selectColumns,
    orderBy,
  };
  if (limit != null) {
    parsedQuery.limit = limit;
  }

  return parsedQuery;
}

function buildProjection(
  parsed: ParsedSelectQuery,
  schema: SchemaDefinition,
): Map<string, Set<string>> {
  const projections = new Map<string, Set<string>>();
  for (const binding of parsed.bindings) {
    projections.set(binding.alias, new Set());
  }

  if (parsed.selectAll) {
    const base = parsed.bindings[0];
    if (!base) {
      throw new Error("SELECT queries must include a FROM clause.");
    }

    const allColumns = Object.keys(getTable(schema, base.table).columns);
    for (const column of allColumns) {
      projections.get(base.alias)?.add(column);
    }
  } else {
    for (const item of parsed.selectColumns) {
      projections.get(item.alias)?.add(item.column);
    }
  }

  for (const join of parsed.joinEdges) {
    projections.get(join.leftAlias)?.add(join.leftColumn);
    projections.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of parsed.filters) {
    projections.get(filter.alias)?.add(filter.clause.column);
  }

  for (const term of parsed.orderBy) {
    projections.get(term.alias)?.add(term.column);
  }

  for (const [alias, cols] of projections) {
    if (cols.size === 0) {
      const binding = parsed.bindings.find((candidate) => candidate.alias === alias);
      if (binding) {
        const firstColumn = Object.keys(getTable(schema, binding.table).columns)[0];
        if (!firstColumn) {
          throw new Error(`Table ${binding.table} has no columns.`);
        }
        cols.add(firstColumn);
      }
    }
  }

  return projections;
}

function groupFiltersByAlias(filters: LiteralFilter[]): Map<string, ScanFilterClause[]> {
  const grouped = new Map<string, ScanFilterClause[]>();
  for (const filter of filters) {
    const existing = grouped.get(filter.alias) ?? [];
    existing.push(filter.clause);
    grouped.set(filter.alias, existing);
  }
  return grouped;
}

function buildExecutionOrder(
  bindings: TableBinding[],
  joinEdges: JoinCondition[],
  filtersByAlias: Map<string, ScanFilterClause[]>,
): string[] {
  const score = new Map<string, number>();
  for (const binding of bindings) {
    score.set(binding.alias, filtersByAlias.get(binding.alias)?.length ?? 0);
  }

  const unvisited = new Set(bindings.map((binding) => binding.alias));
  const visited = new Set<string>();
  const order: string[] = [];

  while (unvisited.size > 0) {
    const candidates = [...unvisited].filter((alias) => {
      if (visited.size === 0) {
        return true;
      }
      return joinEdges.some(
        (edge) =>
          (edge.leftAlias === alias && visited.has(edge.rightAlias)) ||
          (edge.rightAlias === alias && visited.has(edge.leftAlias)),
      );
    });

    const pool = candidates.length > 0 ? candidates : [...unvisited];
    pool.sort((a, b) => {
      const aScore = score.get(a) ?? 0;
      const bScore = score.get(b) ?? 0;
      if (aScore !== bScore) {
        return bScore - aScore;
      }

      const aIndex = bindings.find((binding) => binding.alias === a)?.index ?? 0;
      const bIndex = bindings.find((binding) => binding.alias === b)?.index ?? 0;
      return bIndex - aIndex;
    });

    const next = pool[0];
    if (!next) {
      break;
    }

    order.push(next);
    visited.add(next);
    unvisited.delete(next);
  }

  return order;
}

function buildDependencyFilters(
  alias: string,
  joinEdges: JoinCondition[],
  rowsByAlias: Map<string, QueryRow[]>,
): ScanFilterClause[] {
  const clauses: ScanFilterClause[] = [];
  for (const edge of joinEdges) {
    if (edge.leftAlias === alias && rowsByAlias.has(edge.rightAlias)) {
      clauses.push({
        op: "in",
        column: edge.leftColumn,
        values: uniqueValues(rowsByAlias.get(edge.rightAlias) ?? [], edge.rightColumn),
      });
      continue;
    }

    if (edge.rightAlias === alias && rowsByAlias.has(edge.leftAlias)) {
      clauses.push({
        op: "in",
        column: edge.rightColumn,
        values: uniqueValues(rowsByAlias.get(edge.leftAlias) ?? [], edge.leftColumn),
      });
    }
  }

  return dedupeInClauses(clauses);
}

function initializeJoinedRows(
  rowsByAlias: Map<string, QueryRow[]>,
  baseAlias: string,
): Array<Record<string, QueryRow>> {
  const baseRows = rowsByAlias.get(baseAlias) ?? [];
  return baseRows.map((row) => ({
    [baseAlias]: row,
  }));
}

function applyInnerJoin(
  existing: Array<Record<string, QueryRow>>,
  join: ParsedJoin,
  rowsByAlias: Map<string, QueryRow[]>,
): Array<Record<string, QueryRow>> {
  const rightRows = rowsByAlias.get(join.alias) ?? [];
  const isJoinAliasLeft = join.condition.leftAlias === join.alias;
  const joinAliasColumn = isJoinAliasLeft ? join.condition.leftColumn : join.condition.rightColumn;
  const existingAlias = isJoinAliasLeft ? join.condition.rightAlias : join.condition.leftAlias;
  const existingColumn = isJoinAliasLeft ? join.condition.rightColumn : join.condition.leftColumn;

  const index = new Map<unknown, QueryRow[]>();
  for (const row of rightRows) {
    const key = row[joinAliasColumn];
    const bucket = index.get(key) ?? [];
    bucket.push(row);
    index.set(key, bucket);
  }

  const joined: Array<Record<string, QueryRow>> = [];
  for (const bundle of existing) {
    const leftRow = bundle[existingAlias];
    if (!leftRow) {
      continue;
    }

    const key = leftRow[existingColumn];
    const matches = index.get(key) ?? [];
    for (const match of matches) {
      joined.push({
        ...bundle,
        [join.alias]: match,
      });
    }
  }

  return joined;
}

function applyFinalSort(
  rows: Array<Record<string, QueryRow>>,
  orderBy: OrderColumn[],
): Array<Record<string, QueryRow>> {
  const sorted = [...rows];
  sorted.sort((left, right) => {
    for (const term of orderBy) {
      const leftValue = left[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      const rightValue = right[term.alias]?.[term.column] as
        | string
        | number
        | boolean
        | null
        | undefined;
      if (leftValue === rightValue) {
        continue;
      }

      const leftNorm = leftValue ?? null;
      const rightNorm = rightValue ?? null;

      const comparison = compareNullableValues(leftNorm, rightNorm);
      return term.direction === "asc" ? comparison : -comparison;
    }

    return 0;
  });

  return sorted;
}

function projectResultRows(
  rows: Array<Record<string, QueryRow>>,
  parsed: ParsedSelectQuery,
): QueryRow[] {
  if (parsed.selectAll) {
    const baseAlias = parsed.bindings[0]?.alias;
    if (!baseAlias) {
      return [];
    }

    return rows.map((row) => {
      const baseRow = row[baseAlias];
      return baseRow ? { ...baseRow } : {};
    });
  }

  return rows.map((bundle) => {
    const out: QueryRow = {};
    for (const item of parsed.selectColumns) {
      out[item.output] = bundle[item.alias]?.[item.column] ?? null;
    }
    return out;
  });
}

function parseJoinCondition(raw: unknown): JoinCondition {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    throw new Error("Only equality join conditions are currently supported.");
  }

  const left = toColumnRef(expr.left);
  const right = toColumnRef(expr.right);
  if (!left || !right) {
    throw new Error("JOIN conditions must compare two columns.");
  }

  return {
    leftAlias: left.alias,
    leftColumn: left.column,
    rightAlias: right.alias,
    rightColumn: right.column,
  };
}

function flattenAndConditions(where: unknown): unknown[] {
  if (!where) {
    return [];
  }

  const expr = where as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type === "binary_expr" && expr.operator === "AND") {
    return [...flattenAndConditions(expr.left), ...flattenAndConditions(expr.right)];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    throw new Error("OR predicates are not yet supported.");
  }

  return [where];
}

function normalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> {
  switch (raw) {
    case "=":
      return "eq";
    case "!=":
    case "<>":
      return "neq";
    case ">":
      return "gt";
    case ">=":
      return "gte";
    case "<":
      return "lt";
    case "<=":
      return "lte";
    case "IN":
      return "in";
    default:
      throw new Error(`Unsupported operator: ${String(raw)}`);
  }
}

function invertOperator(
  op: Exclude<ScanFilterClause["op"], "in">,
): Exclude<ScanFilterClause["op"], "in"> {
  switch (op) {
    case "eq":
      return "eq";
    case "neq":
      return "neq";
    case "gt":
      return "lt";
    case "gte":
      return "lte";
    case "lt":
      return "gt";
    case "lte":
      return "gte";
  }
}

function toColumnRef(raw: unknown): { alias: string; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0) {
    return undefined;
  }

  if (typeof expr.table !== "string" || expr.table.length === 0) {
    throw new Error(`Ambiguous unqualified column reference: ${expr.column}`);
  }

  return {
    alias: expr.table,
    column: expr.column,
  };
}

function isStarColumn(raw: { expr?: unknown }): boolean {
  const expr = raw.expr as { type?: unknown; column?: unknown } | undefined;
  return expr?.type === "column_ref" && expr.column === "*";
}

function parseLiteral(raw: unknown): unknown | undefined {
  const expr = raw as { type?: unknown; value?: unknown };

  switch (expr?.type) {
    case "single_quote_string":
    case "double_quote_string":
    case "string":
      return String(expr.value ?? "");
    case "number": {
      const value = expr.value;
      if (typeof value === "number") {
        return value;
      }
      if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
      }
      return undefined;
    }
    case "bool":
      return Boolean(expr.value);
    case "null":
      return null;
    default:
      return undefined;
  }
}

function parseExpressionList(raw: unknown): unknown[] {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    throw new Error("IN predicates must use literal lists.");
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    throw new Error("IN predicates must contain only literal values.");
  }

  return values;
}

function uniqueJoinEdges(edges: JoinCondition[]): JoinCondition[] {
  const seen = new Set<string>();
  const out: JoinCondition[] = [];

  for (const edge of edges) {
    const key = `${edge.leftAlias}.${edge.leftColumn}=${edge.rightAlias}.${edge.rightColumn}`;
    const reverseKey = `${edge.rightAlias}.${edge.rightColumn}=${edge.leftAlias}.${edge.leftColumn}`;
    if (seen.has(key) || seen.has(reverseKey)) {
      continue;
    }
    seen.add(key);
    out.push(edge);
  }

  return out;
}

function uniqueValues(rows: QueryRow[], column: string): unknown[] {
  const seen = new Set<unknown>();
  const out: unknown[] = [];
  for (const row of rows) {
    const value = row[column] ?? null;
    if (seen.has(value)) {
      continue;
    }

    seen.add(value);
    out.push(value);
  }
  return out;
}

function compareNullableValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }

  if (typeof left === "number" && typeof right === "number") {
    return left < right ? -1 : 1;
  }

  if (typeof left === "boolean" && typeof right === "boolean") {
    return Number(left) < Number(right) ? -1 : 1;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString < rightString ? -1 : 1;
}

function dedupeInClauses(clauses: ScanFilterClause[]): ScanFilterClause[] {
  const out: ScanFilterClause[] = [];
  const seen = new Set<string>();

  for (const clause of clauses) {
    if (clause.op !== "in") {
      out.push(clause);
      continue;
    }

    const key = `${clause.column}:${JSON.stringify(clause.values)}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    out.push(clause);
  }

  return out;
}
