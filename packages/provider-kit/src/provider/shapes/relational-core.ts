import { isRelProjectColumnMapping, type RelNode } from "@tupl/foundation";

/**
 * Relational-core owns the backend-neutral shapes that SQL-like providers reason about:
 * single-query pipelines, set-op wrappers, and WITH-body wrappers. Providers may rely on these
 * derived shapes, but not on planner/runtime ownership or execution policy.
 */
export class UnsupportedRelationalPlanError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedRelationalPlanError";
  }
}

export interface RelationalPipeline {
  base: RelNode;
  project?: Extract<RelNode, { kind: "project" }>;
  aggregate?: Extract<RelNode, { kind: "aggregate" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

export interface RelationalSetOpWrapper {
  setOp: Extract<RelNode, { kind: "set_op" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
}

export interface RelationalWithBodyWrapper {
  cteRef: Extract<RelNode, { kind: "cte_ref" }>;
  project?: Extract<RelNode, { kind: "project" }>;
  sort?: Extract<RelNode, { kind: "sort" }>;
  limitOffset?: Extract<RelNode, { kind: "limit_offset" }>;
  window?: Extract<RelNode, { kind: "window" }>;
  filters: Extract<RelNode, { kind: "filter" }>[];
}

export interface RelationalScanBindingBase {
  alias: string;
  scan: Extract<RelNode, { kind: "scan" }>;
}

export interface RelationalRegularJoinStep<TBinding extends RelationalScanBindingBase> {
  joinType: Exclude<Extract<RelNode, { kind: "join" }>["joinType"], "semi">;
  right: TBinding;
  leftKey: { alias: string; column: string };
  rightKey: { alias: string; column: string };
}

export interface RelationalSemiJoinStep {
  joinType: "semi";
  right: RelNode;
  leftKey: { alias: string; column: string };
  rightKey: { alias?: string; column: string };
}

export type RelationalJoinStep<TBinding extends RelationalScanBindingBase> =
  | RelationalRegularJoinStep<TBinding>
  | RelationalSemiJoinStep;

export interface RelationalJoinPlan<TBinding extends RelationalScanBindingBase> {
  root: TBinding;
  joins: RelationalJoinStep<TBinding>[];
  aliases: Map<string, TBinding>;
}

export interface RelationalSingleQueryPlan<TBinding extends RelationalScanBindingBase> {
  pipeline: RelationalPipeline;
  joinPlan: RelationalJoinPlan<TBinding>;
}

export function canCompileBasicRel(
  node: RelNode,
  isKnownScan: (table: string) => boolean,
  options?: { requireColumnProjectMappings?: boolean },
): boolean {
  const requireColumnProjectMappings = options?.requireColumnProjectMappings ?? false;

  switch (node.kind) {
    case "values":
    case "cte_ref":
      return false;
    case "scan":
      return (
        isKnownScan(node.table) &&
        !node.orderBy?.length &&
        node.limit == null &&
        node.offset == null
      );
    case "filter":
      return !node.expr && canCompileBasicRel(node.input, isKnownScan, options);
    case "project":
      return (
        (!requireColumnProjectMappings ||
          node.columns.every((column) => isRelProjectColumnMapping(column))) &&
        canCompileBasicRel(node.input, isKnownScan, options)
      );
    case "aggregate":
    case "sort":
    case "limit_offset":
      return canCompileBasicRel(node.input, isKnownScan, options);
    case "correlate":
      return false;
    case "join":
      return (
        (node.joinType !== "semi" || node.right.output.length === 1) &&
        canCompileBasicRel(node.left, isKnownScan, options) &&
        canCompileBasicRel(node.right, isKnownScan, options)
      );
    case "window":
    case "set_op":
    case "with":
    case "repeat_union":
      return false;
  }
}

export function resolveRelationalStrategy<TStrategy extends string>(
  node: RelNode,
  input: {
    basicStrategy: TStrategy;
    setOpStrategy: TStrategy;
    withStrategy: TStrategy;
    canCompileBasic: (node: RelNode) => boolean;
    validateBasic: (node: RelNode) => boolean;
    canCompileSetOp: (node: RelNode) => boolean;
    canCompileWith: (node: RelNode) => boolean;
  },
): TStrategy | null {
  if (input.canCompileBasic(node) && input.validateBasic(node)) {
    return input.basicStrategy;
  }

  if (input.canCompileSetOp(node)) {
    return input.setOpStrategy;
  }

  if (input.canCompileWith(node)) {
    return input.withStrategy;
  }

  return null;
}

export function isSupportedRelationalPlan(validate: () => void): boolean {
  try {
    validate();
    return true;
  } catch (error) {
    if (error instanceof UnsupportedRelationalPlanError) {
      return false;
    }

    throw error;
  }
}

export function canCompileSetOpRel<TStrategy extends string>(
  node: RelNode,
  resolveBranchStrategy: (node: RelNode) => TStrategy | null,
  requireProjectColumnMapping: (
    mapping: Extract<RelNode, { kind: "project" }>["columns"][number],
  ) => { source: { alias?: string; table?: string; column: string }; output: string },
): boolean {
  const wrapper = unwrapSetOpRel(node);
  if (!wrapper) {
    return false;
  }

  if (!resolveBranchStrategy(wrapper.setOp.left) || !resolveBranchStrategy(wrapper.setOp.right)) {
    return false;
  }

  const topProject = wrapper.project;
  if (topProject) {
    for (const rawColumn of topProject.columns) {
      const column = requireProjectColumnMapping(rawColumn);
      if (column.source.alias || column.source.table) {
        return false;
      }
      if (column.source.column !== column.output) {
        return false;
      }
    }
  }

  for (const term of wrapper.sort?.orderBy ?? []) {
    if (term.source.alias || term.source.table) {
      return false;
    }
  }

  return true;
}

export function canCompileWithRel<TStrategy extends string>(
  node: RelNode,
  resolveBranchStrategy: (node: RelNode) => TStrategy | null,
): boolean {
  if (node.kind !== "with" || node.ctes.length === 0) {
    return false;
  }

  for (const cte of node.ctes) {
    if (!resolveBranchStrategy(cte.query)) {
      return false;
    }
  }

  const body = unwrapWithBodyRel(node.body);
  if (!body) {
    return false;
  }
  if (!node.ctes.some((cte) => cte.name === body.cteRef.name)) {
    return false;
  }

  for (const fn of body.window?.functions ?? []) {
    if (fn.fn !== "dense_rank" && fn.fn !== "rank" && fn.fn !== "row_number") {
      return false;
    }
  }

  return true;
}

export function buildSingleQueryPlan<TBinding extends RelationalScanBindingBase>(
  rel: RelNode,
  createScanBinding: (scan: Extract<RelNode, { kind: "scan" }>) => TBinding,
): RelationalSingleQueryPlan<TBinding> {
  const pipeline = extractRelPipeline(rel);
  const joinPlan = buildJoinPlan(pipeline.base, createScanBinding);

  return {
    pipeline,
    joinPlan,
  };
}

// Flattening must preserve the order of operations that do not commute. Unsupported
// compositions remain separate fragments so the runtime can execute their boundary.
function canFlattenPipeline(node: RelNode) {
  const outer = new Set<RelNode["kind"]>();
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let current = node;
  while ("input" in current) {
    const operation = current;
    if (
      (operation.kind === "limit_offset" && [...outer].some((kind) => kind !== "project")) ||
      (operation.kind === "project" &&
        (outer.has("aggregate") ||
          outer.has("window") ||
          (outer.has("filter") &&
            !filters.every(
              (filter) =>
                !filter.expr &&
                (filter.where ?? []).every((clause) => {
                  const mapping = operation.columns.find(
                    (column) => column.output === clause.column,
                  );
                  // Expression-capable backends substitute these computed values into WHERE.
                  // Column renames require explicit rebinding before a pipeline can be flattened.
                  return mapping !== undefined && !isRelProjectColumnMapping(mapping);
                }),
            )))) ||
      (operation.kind === "aggregate" && outer.has("filter")) ||
      (operation.kind === "sort" && (outer.has("aggregate") || outer.has("window"))) ||
      (operation.kind === "window" && outer.has("filter"))
    )
      return false;
    if (operation.kind === "filter") filters.push(operation);
    outer.add(operation.kind);
    current = operation.input;
  }
  return true;
}

export function isProjectionInInput(input: RelNode, project: RelationalPipeline["project"]) {
  if (!project) return false;
  let current = input;
  while ("input" in current && current !== project) current = current.input;
  return current === project;
}

/** Bind ordering in the sort's original input scope, before flattening projections. */
export function resolveRelationalOrderBy(
  sort: RelationalPipeline["sort"],
  project: RelationalPipeline["project"],
): Array<Extract<RelNode, { kind: "sort" }>["orderBy"][number] & { projectedOutput?: string }> {
  if (!sort || !project) return sort?.orderBy ?? [];
  if (!isProjectionInInput(sort.input, project)) return sort.orderBy;
  return sort.orderBy.map((term) => {
    const alias = term.source.alias ?? term.source.table;
    const name = alias ? `${alias}.${term.source.column}` : term.source.column;
    const mapping =
      project.columns.find((column) => column.output === name) ??
      project.columns.find((column) => column.output === term.source.column);
    if (!mapping) {
      throw new UnsupportedRelationalPlanError(
        `ORDER BY output "${name}" cannot be resolved through this projection.`,
      );
    }
    if (!isRelProjectColumnMapping(mapping)) {
      return { ...term, projectedOutput: mapping.output };
    }
    return { ...term, source: mapping.source };
  });
}

export function extractRelPipeline(node: RelNode): RelationalPipeline {
  if (!canFlattenPipeline(node)) {
    throw new UnsupportedRelationalPlanError(
      "Relational operation order requires separate query fragments.",
    );
  }
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let aggregate: Extract<RelNode, { kind: "aggregate" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          throw new UnsupportedRelationalPlanError("Multiple project nodes are not supported.");
        }
        project = current;
        current = current.input;
        continue;
      case "aggregate":
        if (aggregate) {
          throw new UnsupportedRelationalPlanError("Multiple aggregate nodes are not supported.");
        }
        aggregate = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          throw new UnsupportedRelationalPlanError("Multiple sort nodes are not supported.");
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          throw new UnsupportedRelationalPlanError(
            "Multiple limit/offset nodes are not supported.",
          );
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "correlate":
        throw new UnsupportedRelationalPlanError(
          'Rel node "correlate" is not supported in single-query pushdown.',
        );
      case "scan":
      case "join":
        resolveRelationalOrderBy(sort, project);
        return {
          base: current,
          ...(project ? { project } : {}),
          ...(aggregate ? { aggregate } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          filters,
        };
      case "cte_ref":
      case "set_op":
      case "with":
      case "window":
        throw new UnsupportedRelationalPlanError(
          `Rel node "${current.kind}" is not supported in single-query pushdown.`,
        );
    }
  }
}

export function unwrapSetOpRel(node: RelNode): RelationalSetOpWrapper | null {
  if (!canFlattenPipeline(node)) return null;
  let current = node;
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "correlate":
        return null;
      case "set_op":
        return {
          setOp: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
        };
      default:
        return null;
    }
  }
}

export function unwrapWithBodyRel(node: RelNode): RelationalWithBodyWrapper | null {
  if (!canFlattenPipeline(node)) return null;
  let current = node;
  const filters: Extract<RelNode, { kind: "filter" }>[] = [];
  let project: Extract<RelNode, { kind: "project" }> | undefined;
  let sort: Extract<RelNode, { kind: "sort" }> | undefined;
  let limitOffset: Extract<RelNode, { kind: "limit_offset" }> | undefined;
  let window: Extract<RelNode, { kind: "window" }> | undefined;

  while (true) {
    switch (current.kind) {
      case "filter":
        filters.push(current);
        current = current.input;
        continue;
      case "project":
        if (project) {
          return null;
        }
        project = current;
        current = current.input;
        continue;
      case "sort":
        if (sort) {
          return null;
        }
        sort = current;
        current = current.input;
        continue;
      case "limit_offset":
        if (limitOffset) {
          return null;
        }
        limitOffset = current;
        current = current.input;
        continue;
      case "window":
        if (window) {
          return null;
        }
        window = current;
        current = current.input;
        continue;
      case "correlate":
        return null;
      case "cte_ref":
        if (
          !isSupportedRelationalPlan(() => {
            resolveRelationalOrderBy(sort, project);
          })
        )
          return null;
        return {
          cteRef: current,
          ...(project ? { project } : {}),
          ...(sort ? { sort } : {}),
          ...(limitOffset ? { limitOffset } : {}),
          ...(window ? { window } : {}),
          filters,
        };
      default:
        return null;
    }
  }
}

export function buildJoinPlan<TBinding extends RelationalScanBindingBase>(
  node: RelNode,
  createScanBinding: (scan: Extract<RelNode, { kind: "scan" }>) => TBinding,
): RelationalJoinPlan<TBinding> {
  if (node.kind === "scan") {
    const root = createScanBinding(node);
    return {
      root,
      joins: [],
      aliases: new Map([[root.alias, root]]),
    };
  }

  if (node.kind !== "join") {
    throw new UnsupportedRelationalPlanError(
      `Expected scan/join base node, received "${node.kind}".`,
    );
  }

  const left = buildJoinPlan(node.left, createScanBinding);

  if (node.joinType === "semi") {
    const leftAlias = node.leftKey.alias ?? node.leftKey.table;
    const rightAlias = node.rightKey.alias ?? node.rightKey.table;
    if (!leftAlias) {
      throw new UnsupportedRelationalPlanError("Join keys must be alias-qualified.");
    }

    return {
      root: left.root,
      joins: [
        ...left.joins,
        {
          joinType: "semi",
          right: node.right,
          leftKey: {
            alias: leftAlias,
            column: node.leftKey.column,
          },
          rightKey: {
            ...(rightAlias ? { alias: rightAlias } : {}),
            column: node.rightKey.column,
          },
        },
      ],
      aliases: new Map(left.aliases),
    };
  }

  const right = buildJoinPlan(node.right, createScanBinding);
  if (right.joins.length > 0) {
    throw new UnsupportedRelationalPlanError("Only left-deep join trees are supported.");
  }

  const rightRoot = right.root;
  if (left.aliases.has(rightRoot.alias)) {
    throw new UnsupportedRelationalPlanError(`Duplicate alias "${rightRoot.alias}" in join tree.`);
  }

  const leftAlias = node.leftKey.alias ?? node.leftKey.table;
  const rightAlias = node.rightKey.alias ?? node.rightKey.table;
  if (!leftAlias || !rightAlias) {
    throw new UnsupportedRelationalPlanError("Join keys must be alias-qualified.");
  }

  const aliases = new Map(left.aliases);
  aliases.set(rightRoot.alias, rightRoot);
  for (const [alias, binding] of right.aliases.entries()) {
    aliases.set(alias, binding);
  }

  return {
    root: left.root,
    joins: [
      ...left.joins,
      {
        joinType: node.joinType,
        right: rightRoot,
        leftKey: {
          alias: leftAlias,
          column: node.leftKey.column,
        },
        rightKey: {
          alias: rightAlias,
          column: node.rightKey.column,
        },
      },
    ],
    aliases,
  };
}

export function resolveColumnFromFilterColumn<TBinding extends RelationalScanBindingBase>(
  aliases: Map<string, TBinding>,
  column: string,
): string {
  const idx = column.lastIndexOf(".");
  if (idx > 0) {
    return column;
  }

  return resolveColumnRef(aliases, { column });
}

export function resolveColumnRef<TBinding extends RelationalScanBindingBase>(
  aliases: Map<string, TBinding>,
  ref: { alias?: string; column: string },
): string {
  if (ref.alias) {
    const binding = aliases.get(ref.alias);
    if (!binding) {
      throw new UnsupportedRelationalPlanError(`Unknown alias "${ref.alias}" in rel fragment.`);
    }
    return `${binding.alias}.${ref.column}`;
  }

  let matched: string | null = null;
  for (const binding of aliases.values()) {
    const available = new Set(binding.scan.select);
    const hasInFilter = (binding.scan.where ?? []).some((entry) => entry.column === ref.column);
    if (!available.has(ref.column) && !hasInFilter) {
      continue;
    }

    const candidate = `${binding.alias}.${ref.column}`;
    if (matched && matched !== candidate) {
      throw new UnsupportedRelationalPlanError(
        `Ambiguous unqualified column "${ref.column}" in rel fragment.`,
      );
    }
    matched = candidate;
  }

  if (!matched) {
    throw new UnsupportedRelationalPlanError(
      `Unknown unqualified column "${ref.column}" in rel fragment.`,
    );
  }

  return matched;
}
