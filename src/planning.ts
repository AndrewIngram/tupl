import { defaultSqlAstParser } from "./parser";
import type {
  ExpressionAst,
  FromEntryAst,
  OrderByTermAst,
  SelectAst,
  SelectColumnAst,
} from "./sqlite-parser/ast";
import type { PhysicalPlan, PhysicalStep } from "./physical";
import {
  collectRelTables,
  createSqlRel,
  type RelColumnRef,
  type RelJoinNode,
  type RelNode,
  type RelProjectNode,
  type RelScanNode,
  validateRelAgainstSchema,
} from "./rel";
import {
  normalizeCapability,
  resolveTableProvider,
  type ProviderFragment,
  type ProvidersMap,
} from "./provider";
import type { ScanFilterClause, SchemaDefinition } from "./schema";
import {
  getNormalizedColumnSourceMap,
  getNormalizedTableBinding,
  resolveNormalizedColumnSource,
  type ColumnDefinition,
} from "./schema";

export interface RelLoweringResult {
  rel: RelNode;
  tables: string[];
}

interface Binding {
  table: string;
  alias: string;
  index: number;
}

interface ParsedJoin {
  alias: string;
  joinType: "inner" | "left" | "right" | "full";
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
}

interface SelectProjection {
  source: {
    alias: string;
    column: string;
  };
  output: string;
}

interface ParsedOrderTerm {
  source: {
    alias?: string;
    column: string;
  };
  direction: "asc" | "desc";
}

interface ParsedAggregateProjection {
  kind: "group" | "metric";
  output: string;
  source?: {
    alias: string;
    column: string;
  };
  metric?: {
    fn: "count" | "sum" | "avg" | "min" | "max";
    as: string;
    column?: RelColumnRef;
    distinct?: boolean;
  };
}

interface LiteralFilter {
  alias: string;
  clause: ScanFilterClause;
}

let physicalStepIdCounter = 0;
let relIdCounter = 0;

function nextPhysicalStepId(prefix: string): string {
  physicalStepIdCounter += 1;
  return `${prefix}_${physicalStepIdCounter}`;
}

function nextRelId(prefix: string): string {
  relIdCounter += 1;
  return `${prefix}_${relIdCounter}`;
}

export function lowerSqlToRel(sql: string, schema: SchemaDefinition): RelLoweringResult {
  const ast = defaultSqlAstParser.astify(sql) as SelectAst;

  const simple = tryLowerSimpleSelect(ast, schema);
  if (simple) {
    validateRelAgainstSchema(simple, schema);
    return {
      rel: simple,
      tables: collectRelTables(simple),
    };
  }

  const tables = collectTablesFromSelectAst(ast);
  const rel = createSqlRel(sql, tables);
  validateRelAgainstSchema(rel, schema);
  return {
    rel,
    tables,
  };
}

export async function planPhysicalQuery<TContext>(
  rel: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  sql: string,
): Promise<PhysicalPlan> {
  const plannedRel = assignConventions(rel, schema);
  const state: { steps: PhysicalStep[] } = { steps: [] };

  const rootStepId = await planPhysicalNode(
    plannedRel,
    schema,
    providers,
    context,
    sql,
    state,
  );

  return {
    rel: plannedRel,
    rootStepId,
    steps: state.steps,
  };
}

async function planPhysicalNode<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  sql: string,
  state: { steps: PhysicalStep[] },
): Promise<string> {
  const remoteStepId = await tryPlanRemoteFragment(node, schema, providers, context, state);
  if (remoteStepId) {
    return remoteStepId;
  }

  switch (node.kind) {
    case "scan": {
      const step: PhysicalStep = {
        id: nextPhysicalStepId("local_project"),
        kind: "local_project",
        dependsOn: [],
        summary: `Local fallback scan for ${node.table}`,
      };
      state.steps.push(step);
      return step.id;
    }
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset": {
      const input = await planPhysicalNode(node.input, schema, providers, context, sql, state);
      const kind =
        node.kind === "filter"
          ? "local_filter"
          : node.kind === "project"
            ? "local_project"
            : node.kind === "aggregate"
              ? "local_aggregate"
              : node.kind === "sort"
                ? "local_sort"
                : "local_limit_offset";

      const step: PhysicalStep = {
        id: nextPhysicalStepId(kind),
        kind,
        dependsOn: [input],
        summary: `Local ${node.kind} execution`,
      };
      state.steps.push(step);
      return step.id;
    }
    case "join": {
      const lookup = resolveLookupJoinCandidate(node, schema, providers);
      if (lookup) {
        const left = await planPhysicalNode(node.left, schema, providers, context, sql, state);
        const step: PhysicalStep = {
          id: nextPhysicalStepId("lookup_join"),
          kind: "lookup_join",
          dependsOn: [left],
          summary: `Lookup join ${lookup.leftScan.table}.${lookup.leftKey} -> ${lookup.rightScan.table}.${lookup.rightKey}`,
          leftProvider: lookup.leftProvider,
          rightProvider: lookup.rightProvider,
          leftTable: lookup.leftScan.table,
          rightTable: lookup.rightScan.table,
          leftKey: lookup.leftKey,
          rightKey: lookup.rightKey,
          joinType: lookup.joinType,
        };
        state.steps.push(step);
        return step.id;
      }

      const left = await planPhysicalNode(node.left, schema, providers, context, sql, state);
      const right = await planPhysicalNode(node.right, schema, providers, context, sql, state);
      const step: PhysicalStep = {
        id: nextPhysicalStepId("local_hash_join"),
        kind: "local_hash_join",
        dependsOn: [left, right],
        summary: `Local ${node.joinType} join execution`,
      };
      state.steps.push(step);
      return step.id;
    }
    case "set_op": {
      const left = await planPhysicalNode(node.left, schema, providers, context, sql, state);
      const right = await planPhysicalNode(node.right, schema, providers, context, sql, state);
      const step: PhysicalStep = {
        id: nextPhysicalStepId("local_set_op"),
        kind: "local_set_op",
        dependsOn: [left, right],
        summary: `Local ${node.op} execution`,
      };
      state.steps.push(step);
      return step.id;
    }
    case "with": {
      const dependencies: string[] = [];
      for (const cte of node.ctes) {
        dependencies.push(
          await planPhysicalNode(cte.query, schema, providers, context, sql, state),
        );
      }
      dependencies.push(
        await planPhysicalNode(node.body, schema, providers, context, sql, state),
      );

      const step: PhysicalStep = {
        id: nextPhysicalStepId("local_with"),
        kind: "local_with",
        dependsOn: dependencies,
        summary: "Local WITH materialization",
      };
      state.steps.push(step);
      return step.id;
    }
    case "sql": {
      const step: PhysicalStep = {
        id: nextPhysicalStepId("local_project"),
        kind: "local_project",
        dependsOn: [],
        summary: "Local SQL fallback execution",
      };
      state.steps.push(step);
      return step.id;
    }
  }
}

async function tryPlanRemoteFragment<TContext>(
  node: RelNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
  context: TContext,
  state: { steps: PhysicalStep[] },
): Promise<string | null> {
  const provider = resolveSingleProvider(node, schema);
  if (!provider) {
    return null;
  }

  const adapter = providers[provider];
  if (!adapter) {
    throw new Error(`Missing provider adapter: ${provider}`);
  }

  const fragment = buildProviderFragmentForNode(node, schema, provider);
  const capability = normalizeCapability(await adapter.canExecute(fragment, context));
  if (!capability.supported) {
    return null;
  }

  const step: PhysicalStep = {
    id: nextPhysicalStepId("remote_fragment"),
    kind: "remote_fragment",
    dependsOn: [],
    summary: `Execute provider fragment (${provider})`,
    provider,
    fragment,
  };

  state.steps.push(step);
  return step.id;
}

export function buildProviderFragmentForRel(
  node: RelNode,
  schema: SchemaDefinition,
): ProviderFragment | null {
  const provider = resolveSingleProvider(node, schema);
  if (!provider) {
    return null;
  }

  return buildProviderFragmentForNode(node, schema, provider);
}

function buildProviderFragmentForNode(
  node: RelNode,
  schema: SchemaDefinition,
  provider: string,
): ProviderFragment {
  if (node.kind === "scan") {
    const normalizedScan = normalizeScanForProvider(node, schema);
    return {
      kind: "scan",
      provider,
      table: normalizedScan.table,
      request: {
        table: normalizedScan.table,
        ...(normalizedScan.alias ? { alias: normalizedScan.alias } : {}),
        select: normalizedScan.select,
        ...(normalizedScan.where ? { where: normalizedScan.where } : {}),
        ...(normalizedScan.orderBy ? { orderBy: normalizedScan.orderBy } : {}),
        ...(normalizedScan.limit != null ? { limit: normalizedScan.limit } : {}),
        ...(normalizedScan.offset != null ? { offset: normalizedScan.offset } : {}),
      },
    };
  }

  return {
    kind: "rel",
    provider,
    rel: normalizeRelForProvider(node, schema),
  };
}

type AliasToSourceMap = Map<string, Record<string, string>>;

function normalizeRelForProvider(node: RelNode, schema: SchemaDefinition): RelNode {
  const aliasToSource = collectAliasToSourceMappings(node, schema);

  const visit = (current: RelNode): RelNode => {
    switch (current.kind) {
      case "scan":
        return normalizeScanForProvider(current, schema);
      case "filter":
        return {
          ...current,
          input: visit(current.input),
          where: current.where.map((clause) => ({
            ...clause,
            column: mapColumnNameForAlias(clause.column, aliasToSource),
          })),
        };
      case "project":
        return {
          ...current,
          input: visit(current.input),
          columns: current.columns.map((column) => ({
            ...column,
            source: mapColumnRefForAlias(column.source, aliasToSource),
          })),
        };
      case "join":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
          leftKey: mapColumnRefForAlias(current.leftKey, aliasToSource),
          rightKey: mapColumnRefForAlias(current.rightKey, aliasToSource),
        };
      case "aggregate":
        return {
          ...current,
          input: visit(current.input),
          groupBy: current.groupBy.map((column) => mapColumnRefForAlias(column, aliasToSource)),
          metrics: current.metrics.map((metric) => ({
            ...metric,
            ...(metric.column
              ? { column: mapColumnRefForAlias(metric.column, aliasToSource) }
              : {}),
          })),
        };
      case "sort":
        return {
          ...current,
          input: visit(current.input),
          orderBy: current.orderBy.map((term) => ({
            ...term,
            source: mapColumnRefForAlias(term.source, aliasToSource),
          })),
        };
      case "limit_offset":
        return {
          ...current,
          input: visit(current.input),
        };
      case "set_op":
        return {
          ...current,
          left: visit(current.left),
          right: visit(current.right),
        };
      case "with":
        return {
          ...current,
          ctes: current.ctes.map((cte) => ({
            ...cte,
            query: visit(cte.query),
          })),
          body: visit(current.body),
        };
      case "sql":
        return current;
    }
  };

  return visit(node);
}

function normalizeScanForProvider(node: RelScanNode, schema: SchemaDefinition): RelScanNode {
  const binding = getNormalizedTableBinding(schema, node.table);
  if (!binding || binding.kind !== "physical") {
    return node;
  }
  const table = schema.tables[node.table];

  const mapColumn = (column: string): string => resolveNormalizedColumnSource(binding, column);
  const mapClause = (clause: ScanFilterClause): ScanFilterClause => {
    const mapped = mapEnumFilterForProvider(table?.columns[clause.column], clause);
    return {
      ...mapped,
      column: mapColumn(mapped.column),
    };
  };

  return {
    ...node,
    table: binding.entity,
    select: node.select.map(mapColumn),
    ...(node.where
      ? {
          where: node.where.map(mapClause),
        }
      : {}),
    ...(node.orderBy
      ? {
          orderBy: node.orderBy.map((term) => ({
            ...term,
            column: mapColumn(term.column),
          })),
        }
      : {}),
  };
}

function mapEnumFilterForProvider(
  definition: unknown,
  clause: ScanFilterClause,
): ScanFilterClause {
  if (!definition || typeof definition === "string") {
    return clause;
  }

  const column = definition as ColumnDefinition;
  if (!column.enumMap || Object.keys(column.enumMap).length === 0) {
    return clause;
  }

  const mapFacadeValueToSource = (value: unknown): string[] => {
    if (typeof value !== "string") {
      return [];
    }
    const out: string[] = [];
    for (const [sourceValue, facadeValue] of Object.entries(column.enumMap ?? {})) {
      if (facadeValue === value) {
        out.push(sourceValue);
      }
    }
    return out;
  };

  if (clause.op === "eq") {
    const mappedValues = mapFacadeValueToSource(clause.value);
    if (mappedValues.length === 0) {
      throw new Error(
        `No upstream enum mapping for value ${JSON.stringify(clause.value)} on ${clause.column}.`,
      );
    }
    if (mappedValues.length === 1) {
      return {
        ...clause,
        value: mappedValues[0],
      };
    }
    return {
      op: "in",
      column: clause.column,
      values: mappedValues,
    };
  }

  if (clause.op === "in") {
    const mapped = [...new Set(clause.values.flatMap((value) => mapFacadeValueToSource(value)))];
    if (mapped.length === 0) {
      throw new Error(
        `No upstream enum mappings for IN predicate on ${clause.column}.`,
      );
    }
    return {
      ...clause,
      values: mapped,
    };
  }

  return clause;
}

function collectAliasToSourceMappings(node: RelNode, schema: SchemaDefinition): AliasToSourceMap {
  const mappings: AliasToSourceMap = new Map();

  const visit = (current: RelNode): void => {
    switch (current.kind) {
      case "scan": {
        const binding = getNormalizedTableBinding(schema, current.table);
        if (binding?.kind !== "physical") {
          return;
        }
        const alias = current.alias ?? current.table;
        mappings.set(alias, getNormalizedColumnSourceMap(binding));
        return;
      }
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
      case "sql":
        return;
    }
  };

  visit(node);
  return mappings;
}

function mapColumnRefForAlias(ref: RelColumnRef, aliasToSource: AliasToSourceMap): RelColumnRef {
  const alias = ref.alias ?? ref.table;
  if (alias) {
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return ref;
    }
    return {
      ...ref,
      column: mapping[ref.column] ?? ref.column,
    };
  }

  return {
    ...ref,
    column: mapColumnNameForAlias(ref.column, aliasToSource),
  };
}

function mapColumnNameForAlias(column: string, aliasToSource: AliasToSourceMap): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    const alias = column.slice(0, idx);
    const name = column.slice(idx + 1);
    const mapping = aliasToSource.get(alias);
    if (!mapping) {
      return column;
    }
    const mapped = mapping[name] ?? name;
    return `${alias}.${mapped}`;
  }

  let mappedColumn: string | null = null;
  for (const mapping of aliasToSource.values()) {
    if (!(column in mapping)) {
      continue;
    }
    const candidate = mapping[column] ?? column;
    if (mappedColumn && mappedColumn !== candidate) {
      return column;
    }
    mappedColumn = candidate;
  }

  return mappedColumn ?? column;
}

function resolveSingleProvider(node: RelNode, schema: SchemaDefinition): string | null {
  const tables = collectRelTables(node);
  if (tables.length === 0) {
    return null;
  }

  const providers = new Set<string>();
  for (const table of tables) {
    const normalized = getNormalizedTableBinding(schema, table);
    if (normalized?.kind === "view") {
      return null;
    }
    providers.add(resolveTableProvider(schema, table));
  }
  if (providers.size !== 1) {
    return null;
  }

  return [...providers][0] ?? null;
}

function assignConventions(node: RelNode, schema: SchemaDefinition): RelNode {
  switch (node.kind) {
    case "scan": {
      const normalized = getNormalizedTableBinding(schema, node.table);
      if (normalized?.kind === "view") {
        return {
          ...node,
          convention: "local",
        };
      }
      const provider = resolveTableProvider(schema, node.table);
      return {
        ...node,
        convention: `provider:${provider}`,
      };
    }
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset": {
      const input = assignConventions(node.input, schema);
      const provider = resolveSingleProvider(input, schema);
      return {
        ...node,
        input,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "join":
    case "set_op": {
      const left = assignConventions(node.left, schema);
      const right = assignConventions(node.right, schema);
      const provider = resolveSingleProvider(
        {
          ...node,
          left,
          right,
        } as RelNode,
        schema,
      );
      return {
        ...node,
        left,
        right,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "with": {
      const ctes = node.ctes.map((cte) => ({
        ...cte,
        query: assignConventions(cte.query, schema),
      }));
      const body = assignConventions(node.body, schema);
      const provider = resolveSingleProvider(body, schema);
      return {
        ...node,
        ctes,
        body,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
    case "sql": {
      const provider = resolveSingleProvider(node, schema);
      return {
        ...node,
        convention: provider ? (`provider:${provider}` as const) : "local",
      };
    }
  }
}

function tryLowerSimpleSelect(ast: SelectAst, schema: SchemaDefinition): RelNode | null {
  if (ast.type !== "select") {
    return null;
  }

  if (ast.with || ast.set_op || ast._next || ast.having || ast.window) {
    return null;
  }

  const from = Array.isArray(ast.from) ? ast.from : ast.from ? [ast.from] : [];
  if (from.length === 0) {
    return null;
  }

  if (from.some((entry) => typeof (entry as FromEntryAst).table !== "string" || (entry as FromEntryAst).stmt)) {
    return null;
  }

  const bindings: Binding[] = from.map((entry, index) => {
    const table = (entry as FromEntryAst).table;
    if (typeof table !== "string" || !schema.tables[table]) {
      throw new Error(`Unknown table: ${String(table)}`);
    }

    const alias =
      typeof (entry as FromEntryAst).as === "string" && (entry as FromEntryAst).as
        ? ((entry as FromEntryAst).as as string)
        : table;

    return {
      table,
      alias,
      index,
    };
  });

  const aliasToBinding = new Map(bindings.map((binding) => [binding.alias, binding]));

  const joins = parseJoins(from, bindings, aliasToBinding);
  if (joins == null) {
    return null;
  }

  const whereFilters = parseWhereFilters(ast.where, bindings, aliasToBinding);
  if (whereFilters == null) {
    return null;
  }

  const groupBy = parseGroupBy(ast.groupby, bindings, aliasToBinding);
  if (groupBy == null) {
    return null;
  }

  const distinctMode = ast.distinct === "DISTINCT";
  const aggregateMode = groupBy.length > 0 || hasAggregateProjection(ast.columns) || distinctMode;

  const projections = aggregateMode
    ? null
    : parseProjection(ast.columns, bindings, aliasToBinding);
  if (!aggregateMode && projections == null) {
    return null;
  }

  const aggregateProjections = aggregateMode
    ? parseAggregateProjections(ast.columns, bindings, aliasToBinding)
    : null;
  if (aggregateMode && aggregateProjections == null) {
    return null;
  }

  const safeAggregateProjections = aggregateMode ? (aggregateProjections ?? []) : [];
  const safeProjections = aggregateMode ? [] : (projections ?? []);
  let effectiveGroupBy = groupBy;

  if (distinctMode && effectiveGroupBy.length === 0) {
    const distinctGroupBy: RelColumnRef[] = [];
    for (const projection of safeAggregateProjections) {
      if (projection.kind !== "group" || !projection.source) {
        // Defer DISTINCT+aggregate/function cases to the generic fallback path.
        return null;
      }
      distinctGroupBy.push({
        alias: projection.source.alias,
        column: projection.source.column,
      });
    }

    if (distinctGroupBy.length === 0) {
      return null;
    }
    effectiveGroupBy = distinctGroupBy;
  }

  if (aggregateMode && !validateAggregateProjectionGroupBy(safeAggregateProjections, effectiveGroupBy)) {
    return null;
  }

  const outputAliases = new Set<string>(
    aggregateMode
      ? safeAggregateProjections.map((projection) => projection.output)
      : safeProjections.map((projection) => projection.output),
  );

  const orderBy = parseOrderBy(ast.orderby, bindings, aliasToBinding, outputAliases);
  if (orderBy == null) {
    return null;
  }

  const { limit, offset } = parseLimitAndOffset(ast.limit);

  const columnsByAlias = new Map<string, Set<string>>();
  for (const binding of bindings) {
    columnsByAlias.set(binding.alias, new Set<string>());
  }

  if (aggregateMode) {
    for (const projection of safeAggregateProjections) {
      if (projection.kind === "group" && projection.source) {
        columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
      }

      if (projection.kind === "metric" && projection.metric?.column) {
        const metricSource = projection.metric.column;
        const alias = metricSource.alias ?? metricSource.table;
        if (alias) {
          columnsByAlias.get(alias)?.add(metricSource.column);
        }
      }
    }

    for (const ref of effectiveGroupBy) {
      if (ref.alias) {
        columnsByAlias.get(ref.alias)?.add(ref.column);
      }
    }
  } else {
    for (const projection of safeProjections) {
      columnsByAlias.get(projection.source.alias)?.add(projection.source.column);
    }
  }

  for (const join of joins) {
    columnsByAlias.get(join.leftAlias)?.add(join.leftColumn);
    columnsByAlias.get(join.rightAlias)?.add(join.rightColumn);
  }

  for (const filter of whereFilters) {
    columnsByAlias.get(filter.alias)?.add(filter.clause.column);
  }

  for (const term of orderBy) {
    if (term.source.alias) {
      columnsByAlias.get(term.source.alias)?.add(term.source.column);
    }
  }

  for (const binding of bindings) {
    const columns = columnsByAlias.get(binding.alias);
    if (!columns || columns.size > 0) {
      continue;
    }

    const schemaColumns = Object.keys(schema.tables[binding.table]?.columns ?? {});
    const first = schemaColumns[0];
    if (first) {
      columns.add(first);
    }
  }

  const filtersByAlias = new Map<string, ScanFilterClause[]>();
  for (const filter of whereFilters) {
    const current = filtersByAlias.get(filter.alias) ?? [];
    current.push(filter.clause);
    filtersByAlias.set(filter.alias, current);
  }

  const scansByAlias = new Map<string, RelScanNode>();
  for (const binding of bindings) {
    const select = [...(columnsByAlias.get(binding.alias) ?? new Set<string>())];
    const scanWhere = filtersByAlias.get(binding.alias);

    scansByAlias.set(binding.alias, {
      id: nextRelId("scan"),
      kind: "scan",
      convention: "local",
      table: binding.table,
      alias: binding.alias,
      select,
      ...(scanWhere && scanWhere.length > 0 ? { where: scanWhere } : {}),
      output: select.map((column) => ({
        name: `${binding.alias}.${column}`,
      })),
    });
  }

  const root = bindings[0];
  if (!root) {
    return null;
  }

  let current: RelNode = scansByAlias.get(root.alias)!;

  for (const join of joins) {
    const right = scansByAlias.get(join.alias);
    if (!right) {
      return null;
    }

    const joinLeftOnCurrent = appearsInRel(current, join.leftAlias);
    const leftKey: RelColumnRef = joinLeftOnCurrent
      ? {
          alias: join.leftAlias,
          column: join.leftColumn,
        }
      : {
          alias: join.rightAlias,
          column: join.rightColumn,
        };

    const rightKey: RelColumnRef = joinLeftOnCurrent
      ? {
          alias: join.rightAlias,
          column: join.rightColumn,
        }
      : {
          alias: join.leftAlias,
          column: join.leftColumn,
        };

    current = {
      id: nextRelId("join"),
      kind: "join",
      convention: "local",
      joinType: join.joinType,
      left: current,
      right,
      leftKey,
      rightKey,
      output: [...current.output, ...right.output],
    };
  }

  if (aggregateMode) {
    current = {
      id: nextRelId("aggregate"),
      kind: "aggregate",
      convention: "local",
      input: current,
      groupBy: effectiveGroupBy,
      metrics: safeAggregateProjections
        .filter((projection): projection is ParsedAggregateProjection & { metric: NonNullable<ParsedAggregateProjection["metric"]> } =>
          projection.kind === "metric" && !!projection.metric
        )
        .map((projection) => projection.metric),
      output: [
        ...effectiveGroupBy.map((ref) => ({
          name: ref.column,
        })),
        ...safeAggregateProjections
          .filter((projection): projection is ParsedAggregateProjection & { metric: NonNullable<ParsedAggregateProjection["metric"]> } =>
            projection.kind === "metric" && !!projection.metric
          )
          .map((projection) => ({
            name: projection.metric.as,
          })),
      ],
    };
  }

  if (orderBy.length > 0) {
    current = {
      id: nextRelId("sort"),
      kind: "sort",
      convention: "local",
      input: current,
      orderBy: orderBy.map((term) => ({
        source: term.source.alias
          ? {
              alias: term.source.alias,
              column: term.source.column,
            }
          : {
              column: term.source.column,
            },
        direction: term.direction,
      })),
      output: current.output,
    };
  }

  if (limit != null || offset != null) {
    current = {
      id: nextRelId("limit_offset"),
      kind: "limit_offset",
      convention: "local",
      input: current,
      ...(limit != null ? { limit } : {}),
      ...(offset != null ? { offset } : {}),
      output: current.output,
    };
  }

  const projectNode: RelProjectNode = {
    id: nextRelId("project"),
    kind: "project",
    convention: "local",
    input: current,
    columns: aggregateMode
      ? safeAggregateProjections.map((projection) =>
          projection.kind === "group" && projection.source
            ? {
                source: {
                  column: projection.source.column,
                },
                output: projection.output,
              }
            : {
                source: {
                  column: projection.metric!.as,
                },
                output: projection.output,
              })
      : safeProjections.map((projection) => ({
          source: {
            alias: projection.source.alias,
            column: projection.source.column,
          },
          output: projection.output,
        })),
    output: (aggregateMode ? safeAggregateProjections : safeProjections).map((projection) => ({
      name: projection.output,
    })),
  };

  return projectNode;
}

function parseJoins(
  from: FromEntryAst[],
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedJoin[] | null {
  const joins: ParsedJoin[] = [];

  for (let index = 1; index < from.length; index += 1) {
    const entry = from[index];
    if (!entry) {
      return null;
    }

    const joinRaw = typeof entry.join === "string" ? entry.join.toUpperCase() : "";
    const joinType =
      joinRaw === "JOIN" || joinRaw === "INNER JOIN"
        ? "inner"
        : joinRaw === "LEFT JOIN" || joinRaw === "LEFT OUTER JOIN"
          ? "left"
          : joinRaw === "RIGHT JOIN" || joinRaw === "RIGHT OUTER JOIN"
            ? "right"
            : joinRaw === "FULL JOIN" || joinRaw === "FULL OUTER JOIN"
              ? "full"
              : null;

    if (!joinType) {
      return null;
    }

    const binding = bindings[index];
    if (!binding || !entry.on) {
      return null;
    }

    const condition = parseJoinCondition(entry.on, bindings, aliasToBinding);
    if (!condition) {
      return null;
    }

    joins.push({
      alias: binding.alias,
      joinType,
      leftAlias: condition.leftAlias,
      leftColumn: condition.leftColumn,
      rightAlias: condition.rightAlias,
      rightColumn: condition.rightColumn,
    });
  }

  return joins;
}

function parseJoinCondition(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): {
  leftAlias: string;
  leftColumn: string;
  rightAlias: string;
  rightColumn: string;
} | null {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr?.type !== "binary_expr" || expr.operator !== "=") {
    return null;
  }

  const left = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const right = resolveColumnRef(expr.right, bindings, aliasToBinding);
  if (!left || !right) {
    return null;
  }

  return {
    leftAlias: left.alias,
    leftColumn: left.column,
    rightAlias: right.alias,
    rightColumn: right.column,
  };
}

function parseProjection(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): SelectProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: SelectProjection[] = [];

  for (const entry of columns) {
    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    if (!column) {
      return null;
    }

    out.push({
      source: {
        alias: column.alias,
        column: column.column,
      },
      output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : column.column,
    });
  }

  return out;
}

function hasAggregateProjection(rawColumns: unknown): boolean {
  if (rawColumns === "*") {
    return false;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  return columns.some((entry) => (entry.expr as { type?: unknown })?.type === "aggr_func");
}

function parseGroupBy(
  rawGroupBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): RelColumnRef[] | null {
  if (!rawGroupBy || typeof rawGroupBy !== "object") {
    return [];
  }

  const groupBy = rawGroupBy as { columns?: unknown };
  const columns = Array.isArray(groupBy.columns) ? groupBy.columns : [];
  const refs: RelColumnRef[] = [];

  for (const entry of columns) {
    const resolved = resolveColumnRef(entry, bindings, aliasToBinding);
    if (!resolved) {
      return null;
    }

    refs.push({
      alias: resolved.alias,
      column: resolved.column,
    });
  }

  return refs;
}

function parseAggregateProjections(
  rawColumns: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedAggregateProjection[] | null {
  if (rawColumns === "*") {
    return null;
  }

  const columns = Array.isArray(rawColumns) ? (rawColumns as SelectColumnAst[]) : [];
  if (columns.length === 0) {
    return null;
  }

  const out: ParsedAggregateProjection[] = [];

  for (const entry of columns) {
    const exprType = (entry.expr as { type?: unknown })?.type;
    if (exprType === "aggr_func") {
      const output =
        typeof entry.as === "string" && entry.as.length > 0
          ? entry.as
          : deriveDefaultAggregateOutputName(entry.expr);
      const metric = parseAggregateMetric(entry.expr, output, bindings, aliasToBinding);
      if (!metric) {
        return null;
      }

      out.push({
        kind: "metric",
        output,
        metric,
      });
      continue;
    }

    const column = resolveColumnRef(entry.expr, bindings, aliasToBinding);
    if (!column) {
      return null;
    }

    out.push({
      kind: "group",
      source: {
        alias: column.alias,
        column: column.column,
      },
      output: typeof entry.as === "string" && entry.as.length > 0 ? entry.as : column.column,
    });
  }

  return out;
}

function parseAggregateMetric(
  raw: unknown,
  output: string,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): ParsedAggregateProjection["metric"] | null {
  const expr = raw as {
    type?: unknown;
    name?: unknown;
    args?: {
      expr?: unknown;
      distinct?: unknown;
    };
  };

  if (expr.type !== "aggr_func" || typeof expr.name !== "string") {
    return null;
  }

  const fn = expr.name.toLowerCase();
  if (fn !== "count" && fn !== "sum" && fn !== "avg" && fn !== "min" && fn !== "max") {
    return null;
  }

  const distinct = expr.args?.distinct === "DISTINCT";
  const arg = expr.args?.expr;
  const column = parseAggregateMetricColumn(arg, bindings, aliasToBinding);

  if (fn === "count") {
    if (column === null) {
      return null;
    }
    if (!column && distinct) {
      return null;
    }

    return {
      fn,
      as: output,
      ...(column ? { column } : {}),
      ...(distinct ? { distinct: true } : {}),
    };
  }

  if (!column) {
    return null;
  }

  return {
    fn,
    as: output,
    column,
    ...(distinct ? { distinct: true } : {}),
  };
}

function parseAggregateMetricColumn(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): RelColumnRef | null | undefined {
  if (!raw) {
    return undefined;
  }

  const maybeStar = raw as { type?: unknown; value?: unknown };
  if (maybeStar.type === "star" || maybeStar.value === "*") {
    return undefined;
  }

  const resolved = resolveColumnRef(raw, bindings, aliasToBinding);
  if (!resolved) {
    return null;
  }

  return {
    alias: resolved.alias,
    column: resolved.column,
  };
}

function deriveDefaultAggregateOutputName(raw: unknown): string {
  const expr = raw as { name?: unknown };
  const fn = typeof expr.name === "string" ? expr.name.toLowerCase() : "agg";
  return `${fn}_value`;
}

function validateAggregateProjectionGroupBy(
  projections: ParsedAggregateProjection[],
  groupBy: RelColumnRef[],
): boolean {
  const groupBySet = new Set(
    groupBy.map((ref) => `${ref.alias ?? ""}.${ref.column}`),
  );

  for (const projection of projections) {
    if (projection.kind !== "group" || !projection.source) {
      continue;
    }

    const key = `${projection.source.alias}.${projection.source.column}`;
    if (!groupBySet.has(key)) {
      return false;
    }
  }

  return true;
}

function parseOrderBy(
  rawOrderBy: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
  outputAliases: Set<string> = new Set<string>(),
): ParsedOrderTerm[] | null {
  const orderBy = Array.isArray(rawOrderBy) ? (rawOrderBy as OrderByTermAst[]) : [];
  const out: ParsedOrderTerm[] = [];

  for (const term of orderBy) {
    const resolved = resolveColumnRef(term.expr, bindings, aliasToBinding);
    if (!resolved) {
      const rawRef = toRawColumnRef(term.expr);
      if (!rawRef || rawRef.table || !outputAliases.has(rawRef.column)) {
        return null;
      }

      out.push({
        source: {
          column: rawRef.column,
        },
        direction: term.type === "DESC" ? "desc" : "asc",
      });
      continue;
    }

    if (!resolved.alias) {
      return null;
    }

    out.push({
      source: {
        alias: resolved.alias,
        column: resolved.column,
      },
      direction: term.type === "DESC" ? "desc" : "asc",
    });
  }

  return out;
}

function parseWhereFilters(
  where: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): LiteralFilter[] | null {
  const parts = flattenConjunctiveWhere(where);
  if (parts == null) {
    return null;
  }

  const out: LiteralFilter[] = [];
  for (const part of parts) {
    const parsed = parseLiteralFilter(part, bindings, aliasToBinding);
    if (!parsed) {
      return null;
    }
    out.push(parsed);
  }

  return out;
}

function parseLiteralFilter(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): LiteralFilter | null {
  const expr = raw as {
    type?: unknown;
    operator?: unknown;
    left?: unknown;
    right?: unknown;
  };

  if (expr.type !== "binary_expr") {
    return null;
  }

  const operator = tryNormalizeBinaryOperator(expr.operator);
  if (!operator) {
    return null;
  }

  if (operator === "in") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const values = tryParseLiteralExpressionList(expr.right);
    if (!col || !values) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: "in",
        column: col.column,
        values,
      },
    };
  }

  if (operator === "is_null" || operator === "is_not_null") {
    const col = resolveColumnRef(expr.left, bindings, aliasToBinding);
    const value = parseLiteral(expr.right);
    if (!col || value !== null) {
      return null;
    }

    return {
      alias: col.alias,
      clause: {
        op: operator,
        column: col.column,
      },
    };
  }

  const leftCol = resolveColumnRef(expr.left, bindings, aliasToBinding);
  const rightCol = resolveColumnRef(expr.right, bindings, aliasToBinding);

  if (leftCol && rightCol) {
    // This becomes join/relational filter territory; this simple lowering handles literal filters only.
    return null;
  }

  if (leftCol) {
    const value = parseLiteral(expr.right);
    if (value === undefined) {
      return null;
    }

    return {
      alias: leftCol.alias,
      clause: {
        op: operator,
        column: leftCol.column,
        value,
      },
    };
  }

  if (rightCol) {
    const value = parseLiteral(expr.left);
    if (value === undefined) {
      return null;
    }

    return {
      alias: rightCol.alias,
      clause: {
        op: invertOperator(operator),
        column: rightCol.column,
        value,
      },
    };
  }

  return null;
}

function flattenConjunctiveWhere(where: unknown): unknown[] | null {
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
    const left = flattenConjunctiveWhere(expr.left);
    const right = flattenConjunctiveWhere(expr.right);
    if (!left || !right) {
      return null;
    }

    return [...left, ...right];
  }

  if (expr.type === "binary_expr" && expr.operator === "OR") {
    return null;
  }

  if (expr.type === "function") {
    return null;
  }

  return [expr];
}

function tryNormalizeBinaryOperator(raw: unknown): Exclude<ScanFilterClause["op"], never> | null {
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
    case "IS":
      return "is_null";
    case "IS NOT":
      return "is_not_null";
    default:
      return null;
  }
}

function invertOperator(
  op: Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null">,
): Exclude<ScanFilterClause["op"], "in" | "is_null" | "is_not_null"> {
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

function toRawColumnRef(raw: unknown): { table: string | null; column: string } | undefined {
  const expr = raw as { type?: unknown; table?: unknown; column?: unknown };
  if (expr?.type !== "column_ref") {
    return undefined;
  }

  if (typeof expr.column !== "string" || expr.column.length === 0 || expr.column === "*") {
    return undefined;
  }

  const table = typeof expr.table === "string" && expr.table.length > 0 ? expr.table : null;
  return {
    table,
    column: expr.column,
  };
}

function resolveColumnRef(
  raw: unknown,
  bindings: Binding[],
  aliasToBinding: Map<string, Binding>,
): { alias: string; column: string } | undefined {
  const rawRef = toRawColumnRef(raw);
  if (!rawRef) {
    return undefined;
  }

  if (rawRef.table) {
    if (!aliasToBinding.has(rawRef.table)) {
      return undefined;
    }

    return {
      alias: rawRef.table,
      column: rawRef.column,
    };
  }

  if (bindings.length === 1) {
    return {
      alias: bindings[0]?.alias ?? "",
      column: rawRef.column,
    };
  }

  return undefined;
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

function tryParseLiteralExpressionList(raw: unknown): unknown[] | undefined {
  const expr = raw as { type?: unknown; value?: unknown };
  if (expr?.type !== "expr_list" || !Array.isArray(expr.value)) {
    return undefined;
  }

  const values = expr.value.map((entry) => parseLiteral(entry));
  if (values.some((value) => value === undefined)) {
    return undefined;
  }

  return values;
}

function parseLimitAndOffset(rawLimit: unknown): { limit?: number; offset?: number } {
  if (!rawLimit || typeof rawLimit !== "object") {
    return {};
  }

  const limitNode = rawLimit as {
    value?: Array<{ value?: unknown }>;
    seperator?: unknown;
  };

  if (!Array.isArray(limitNode.value) || limitNode.value.length === 0) {
    return {};
  }

  const first = parseNumericLiteral(limitNode.value[0]?.value);
  const second = parseNumericLiteral(limitNode.value[1]?.value);
  const separator = limitNode.seperator;

  if (first == null) {
    throw new Error("Unable to parse LIMIT value.");
  }

  if (separator === "offset") {
    return {
      limit: first,
      ...(second != null ? { offset: second } : {}),
    };
  }

  if (separator === ",") {
    return {
      ...(second != null ? { limit: second } : {}),
      offset: first,
    };
  }

  return {
    limit: first,
  };
}

function parseNumericLiteral(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

function appearsInRel(node: RelNode, alias: string): boolean {
  switch (node.kind) {
    case "scan":
      return node.alias === alias;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return appearsInRel(node.input, alias);
    case "join":
    case "set_op":
      return appearsInRel(node.left, alias) || appearsInRel(node.right, alias);
    case "with":
      return appearsInRel(node.body, alias);
    case "sql":
      return false;
  }
}

function resolveLookupJoinCandidate<TContext>(
  join: RelJoinNode,
  schema: SchemaDefinition,
  providers: ProvidersMap<TContext>,
): {
  leftProvider: string;
  rightProvider: string;
  leftScan: RelScanNode;
  rightScan: RelScanNode;
  leftKey: string;
  rightKey: string;
  joinType: "inner" | "left";
} | null {
  if (join.joinType !== "inner" && join.joinType !== "left") {
    return null;
  }

  const leftScan = findFirstScanNode(join.left);
  const rightScan = findFirstScanNode(join.right);
  if (!leftScan || !rightScan) {
    return null;
  }

  const leftBinding = getNormalizedTableBinding(schema, leftScan.table);
  const rightBinding = getNormalizedTableBinding(schema, rightScan.table);
  if (leftBinding?.kind === "view" || rightBinding?.kind === "view") {
    return null;
  }

  const leftProvider = resolveTableProvider(schema, leftScan.table);
  const rightProvider = resolveTableProvider(schema, rightScan.table);
  if (leftProvider === rightProvider) {
    return null;
  }

  const rightAdapter = providers[rightProvider];
  if (!rightAdapter?.lookupMany) {
    return null;
  }

  return {
    leftProvider,
    rightProvider,
    leftScan,
    rightScan,
    leftKey: join.leftKey.column,
    rightKey: join.rightKey.column,
    joinType: join.joinType,
  };
}

function findFirstScanNode(node: RelNode): RelScanNode | null {
  switch (node.kind) {
    case "scan":
      return node;
    case "filter":
    case "project":
    case "aggregate":
    case "sort":
    case "limit_offset":
      return findFirstScanNode(node.input);
    case "join":
    case "set_op":
      return findFirstScanNode(node.left) ?? findFirstScanNode(node.right);
    case "with":
      return findFirstScanNode(node.body);
    case "sql":
      return null;
  }
}

function collectTablesFromSelectAst(ast: SelectAst): string[] {
  const tables = new Set<string>();
  const cteNames = new Set<string>();

  const visit = (value: unknown): void => {
    if (!value || typeof value !== "object") {
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item);
      }
      return;
    }

    const record = value as Record<string, unknown>;
    const rawName = record.name;
    if (typeof rawName === "string") {
      cteNames.add(rawName);
    } else if (
      rawName &&
      typeof rawName === "object" &&
      typeof (rawName as { value?: unknown }).value === "string"
    ) {
      cteNames.add((rawName as { value: string }).value);
    }

    const from = record.from;
    if (Array.isArray(from)) {
      for (const entry of from) {
        if (entry && typeof entry === "object") {
          const table = (entry as { table?: unknown }).table;
          if (typeof table === "string" && !cteNames.has(table)) {
            tables.add(table);
          }
        }
      }
    }

    for (const nested of Object.values(record)) {
      visit(nested);
    }
  };

  visit(ast.with);
  visit(ast);

  return [...tables];
}
