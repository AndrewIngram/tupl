import type { RelColumnRef, RelExpr, RelNode, RelProjectNode } from "@tupl/foundation";
import { containsLocalExpression } from "./provider/provider-ownership";

const key = (ref: RelColumnRef) =>
  (ref.alias ?? ref.table) ? `${ref.alias ?? ref.table}.${ref.column}` : ref.column;
const needed = (names: Set<string>, name: string) =>
  names.has(name) || names.has(name.slice(name.lastIndexOf(".") + 1));

function refs(expr: RelExpr): string[] {
  if (expr.kind === "column") return [key(expr.ref)];
  if (expr.kind === "local" || expr.kind === "function") return expr.args.flatMap(refs);
  return [];
}

function inputs(project: RelProjectNode, names: Set<string>) {
  return project.columns.filter((column) => needed(names, column.output));
}

/** Demand flows through relational operators, never through application callback bodies. */
function prune(node: RelNode, required: Set<string>): RelNode {
  switch (node.kind) {
    case "project": {
      const columns = inputs(node, required);
      if (columns.length === 0) return prune(node.input, new Set());
      const dependencies = new Set(
        columns.flatMap((column) => ("expr" in column ? refs(column.expr) : [key(column.source)])),
      );
      return {
        ...node,
        columns,
        output: node.output.filter((c) => columns.some((m) => m.output === c.name)),
        input: prune(node.input, dependencies),
      };
    }
    case "scan":
    case "cte_ref": {
      const alias = node.alias ?? (node.kind === "scan" ? node.table : node.name);
      const names = new Set([
        ...required,
        ...(node.where ?? []).map((c) => c.column),
        ...(node.orderBy ?? []).map((c) => c.column),
      ]);
      let select = node.select.filter(
        (column) => needed(names, `${alias}.${column}`) || names.has(column),
      );
      // Providers still need a concrete projection to preserve one result per source row.
      if (select.length === 0 && node.kind === "scan") {
        const metadata = node.kind === "scan" ? node.entity?.columns : undefined;
        const candidate = metadata
          ? Object.keys(metadata).find((name) => !name.startsWith("__derive_input"))
          : undefined;
        const column = candidate ?? node.select[0];
        if (column) select = [column];
      }
      return {
        ...node,
        select,
        output: select.map(
          (name) =>
            node.output.find((c) => c.name === name || c.name === `${alias}.${name}`) ?? {
              name: `${alias}.${name}`,
            },
        ),
      };
    }
    case "filter":
      return {
        ...node,
        input: prune(
          node.input,
          new Set([
            ...required,
            ...(node.where ?? []).map((c) => c.column),
            ...(node.expr ? refs(node.expr) : []),
          ]),
        ),
      };
    case "sort":
      if (required.size === 0) return prune(node.input, required);
      return {
        ...node,
        input: prune(node.input, new Set([...required, ...node.orderBy.map((c) => key(c.source))])),
      };
    case "limit_offset":
      return { ...node, input: prune(node.input, required) };
    case "join": {
      const names = new Set([...required, key(node.leftKey), key(node.rightKey)]);
      return { ...node, left: prune(node.left, names), right: prune(node.right, names) };
    }
    case "aggregate": {
      let metrics = node.metrics.filter((m) => needed(required, m.as));
      if (!metrics.length && !node.groupBy.length)
        metrics = [{ fn: "count", as: "__tupl_cardinality" }];
      const output = [
        ...node.output.slice(0, node.groupBy.length),
        ...metrics.map((m) => ({ name: m.as })),
      ];
      return {
        ...node,
        metrics,
        output,
        input: prune(
          node.input,
          new Set([
            ...node.groupBy.map(key),
            ...metrics.flatMap((m) => (m.column ? [key(m.column)] : [])),
          ]),
        ),
      };
    }
    case "window": {
      const functions = node.functions.filter((f) => needed(required, f.as));
      const dependencies = functions.flatMap((f) => [
        ...f.partitionBy.map(key),
        ...f.orderBy.map((c) => key(c.source)),
        ...("column" in f && f.column ? [key(f.column)] : []),
        ...("value" in f ? refs(f.value) : []),
        ...("defaultExpr" in f && f.defaultExpr ? refs(f.defaultExpr) : []),
      ]);
      const input = prune(node.input, new Set([...required, ...dependencies]));
      return functions.length ? { ...node, functions, input } : input;
    }
    case "set_op": {
      // Distinct set operations depend on complete row values, even if the caller drops columns.
      const names = node.op === "union_all" ? required : new Set(node.output.map((c) => c.name));
      return { ...node, left: prune(node.left, names), right: prune(node.right, names) };
    }
    case "with": {
      const body = prune(node.body, required);
      const demands = new Map<string, Set<string>>();
      const collect = (rel: RelNode) => {
        if (rel.kind === "cte_ref") {
          const names = demands.get(rel.name) ?? new Set<string>();
          rel.select.forEach((c) => names.add(c));
          demands.set(rel.name, names);
        }
        children(rel).forEach(collect);
      };
      collect(body);
      const ctes = [...node.ctes];
      for (let i = ctes.length - 1; i >= 0; i--) {
        const cte = ctes[i]!;
        const requiredColumns = demands.get(cte.name);
        if (!requiredColumns) {
          ctes.splice(i, 1);
          continue;
        }
        const query = prune(cte.query, requiredColumns);
        ctes[i] = { ...cte, query };
        collect(query);
      }
      return ctes.length ? { ...node, body, ctes } : body;
    }
    case "repeat_union":
    case "correlate":
    case "values":
      return node;
  }
}

function expressionSubqueries(expr: RelExpr): RelNode[] {
  if (expr.kind === "subquery") return [expr.rel];
  if (expr.kind === "function" || expr.kind === "local")
    return expr.args.flatMap(expressionSubqueries);
  return [];
}

function children(node: RelNode): RelNode[] {
  if (node.kind === "project")
    return [
      node.input,
      ...node.columns.flatMap((c) => ("expr" in c ? expressionSubqueries(c.expr) : [])),
    ];
  if (node.kind === "filter")
    return [node.input, ...(node.expr ? expressionSubqueries(node.expr) : [])];
  if (node.kind === "window")
    return [
      node.input,
      ...node.functions.flatMap((fn) =>
        "value" in fn
          ? [
              ...expressionSubqueries(fn.value),
              ...(fn.defaultExpr ? expressionSubqueries(fn.defaultExpr) : []),
            ]
          : [],
      ),
    ];
  if ("input" in node) return [node.input];
  if ("left" in node) return [node.left, node.right];
  if (node.kind === "with") return [...node.ctes.map((c) => c.query), node.body];
  if (node.kind === "repeat_union") return [node.seed, node.iterative];
  return [];
}

function mapChildren(node: RelNode): RelNode {
  if ("input" in node) return { ...node, input: move(node.input) };
  if ("left" in node) return { ...node, left: move(node.left), right: move(node.right) };
  if (node.kind === "with")
    return {
      ...node,
      body: move(node.body),
      ctes: node.ctes.map((c) => ({ ...c, query: move(c.query) })),
    };
  if (node.kind === "repeat_union")
    return { ...node, seed: move(node.seed), iterative: move(node.iterative) };
  return node;
}

function sourceRef(project: RelProjectNode, name: string): RelColumnRef | undefined {
  const exact = project.columns.find((c) => c.output === name);
  const candidates = exact
    ? [exact]
    : project.columns.filter((c) => c.output.slice(c.output.lastIndexOf(".") + 1) === name);
  const mapping = candidates.length === 1 ? candidates[0] : undefined;
  return mapping && !("expr" in mapping) ? mapping.source : undefined;
}

function mapPredicate(expr: RelExpr, project: RelProjectNode): RelExpr | undefined {
  if (expr.kind === "literal") return expr;
  if (expr.kind === "column") {
    const ref = sourceRef(project, key(expr.ref));
    return ref ? { ...expr, ref } : undefined;
  }
  if (expr.kind !== "function") return undefined;
  const args = expr.args.map((arg) => mapPredicate(arg, project));
  if (args.some((arg) => !arg)) return undefined;
  return { ...expr, args: args.filter((arg) => arg !== undefined) };
}

function conjunction(terms: RelExpr[]): RelExpr | undefined {
  return terms.length === 0
    ? undefined
    : terms.length === 1
      ? terms[0]
      : { kind: "function", name: "and", args: terms };
}

/** Commute only across a cardinality-preserving projection, never across a join or aggregate. */
function move(original: RelNode): RelNode {
  const node = mapChildren(original);
  if (!("input" in node) || node.input.kind !== "project") return node;
  const project = node.input;
  if (node.kind === "limit_offset") {
    return {
      ...project,
      input: move({ ...node, input: project.input, output: project.input.output }),
    };
  }
  if (node.kind === "sort") {
    const orderBy = node.orderBy.map((term) => ({
      ...term,
      source: sourceRef(project, key(term.source)),
    }));
    if (orderBy.some((term) => !term.source)) return node;
    return {
      ...project,
      input: move({
        ...node,
        input: project.input,
        output: project.input.output,
        orderBy: orderBy.flatMap((term) =>
          term.source ? [{ source: term.source, direction: term.direction }] : [],
        ),
      }),
    };
  }
  if (node.kind !== "filter") return node;
  const nativeWhere = (node.where ?? []).flatMap((clause) => {
    const ref = sourceRef(project, clause.column);
    return ref ? [{ ...clause, column: key(ref) }] : [];
  });
  const residualWhere = (node.where ?? []).filter((clause) => !sourceRef(project, clause.column));
  const terms =
    node.expr?.kind === "function" && node.expr.name === "and"
      ? node.expr.args
      : node.expr
        ? [node.expr]
        : [];
  const nativeTerms = terms.flatMap((term) => {
    const mapped = mapPredicate(term, project);
    return mapped ? [mapped] : [];
  });
  const residualTerms = terms.filter((term) => !mapPredicate(term, project));
  if (!nativeWhere.length && !nativeTerms.length) return node;
  const { expr: _originalExpr, ...filter } = node;
  const nativeExpr = conjunction(nativeTerms);
  const residualExpr = conjunction(residualTerms);
  const lowered: RelNode = {
    ...project,
    input: move({
      ...filter,
      id: `${node.id}_native`,
      input: project.input,
      output: project.input.output,
      where: nativeWhere,
      ...(nativeExpr ? { expr: nativeExpr } : {}),
    }),
  };
  return residualWhere.length || residualTerms.length
    ? {
        ...filter,
        input: lowered,
        where: residualWhere,
        ...(residualExpr ? { expr: residualExpr } : {}),
      }
    : lowered;
}

/** Materialize the dependency graph once, so relational operators can move between computations. */
function materialize(node: RelNode, early = new Set<string>()): RelNode {
  if (node.kind === "project") {
    const priority = node.columns.filter((c) => needed(early, c.output));
    const later = node.columns.filter((c) => !needed(early, c.output));
    const earlyInputs = new Set(
      priority.flatMap((c) => ("expr" in c ? refs(c.expr) : [key(c.source)])),
    );
    let input = materialize(node.input, earlyInputs);
    const computed = new Map<import("@tupl/foundation").RelLocalOperation, RelColumnRef>();
    const lower = (expr: RelExpr): RelExpr => {
      if (expr.kind === "function") return { ...expr, args: expr.args.map(lower) };
      if (expr.kind !== "local") return expr;
      const existing = computed.get(expr.operation);
      if (existing) return { kind: "column", ref: existing };
      const args = expr.args.map(lower);
      let name = `__tupl_${node.id}_${expr.operation.id}`;
      while (input.output.some((c) => c.name === name)) name += "_";
      const ref = { column: name };
      input = {
        kind: "project",
        convention: "local",
        id: `${node.id}_${expr.operation.id}`,
        input,
        columns: [
          ...input.output.map((c) => ({
            kind: "column" as const,
            source: parseRef(c.name),
            output: c.name,
          })),
          { kind: "expr", output: name, expr: { ...expr, args } },
        ],
        output: [...input.output, { name }],
      };
      computed.set(expr.operation, ref);
      return { kind: "column", ref };
    };
    const mappings = new Map<string, RelProjectNode["columns"][number]>();
    for (const column of [...priority, ...later]) {
      if (!("expr" in column)) {
        mappings.set(column.output, column);
        continue;
      }
      const expr = lower(column.expr);
      mappings.set(
        column.output,
        expr.kind === "column"
          ? { kind: "column", source: expr.ref, output: column.output }
          : { ...column, expr },
      );
    }
    return { ...node, input, columns: node.columns.map((column) => mappings.get(column.output)!) };
  }
  if (node.kind === "filter")
    return {
      ...node,
      input: materialize(
        node.input,
        new Set([
          ...early,
          ...(node.where ?? []).map((c) => c.column),
          ...(node.expr ? refs(node.expr) : []),
        ]),
      ),
    };
  if (node.kind === "sort")
    return {
      ...node,
      input: materialize(
        node.input,
        new Set([...early, ...node.orderBy.map((c) => key(c.source))]),
      ),
    };
  if ("input" in node) return { ...node, input: materialize(node.input, early) };
  if ("left" in node)
    return { ...node, left: materialize(node.left), right: materialize(node.right) };
  if (node.kind === "with")
    return {
      ...node,
      body: materialize(node.body, early),
      ctes: node.ctes.map((c) => ({ ...c, query: materialize(c.query) })),
    };
  if (node.kind === "repeat_union")
    return { ...node, seed: materialize(node.seed), iterative: materialize(node.iterative) };
  return node;
}

function parseRef(name: string): RelColumnRef {
  const dot = name.lastIndexOf(".");
  return dot < 0 ? { column: name } : { alias: name.slice(0, dot), column: name.slice(dot + 1) };
}

function hasComputation(node: RelNode): boolean {
  return (
    (node.kind === "project" &&
      node.columns.some((c) => "expr" in c && containsLocalExpression(c.expr))) ||
    children(node).some(hasComputation)
  );
}

export function planLocalComputations(
  node: RelNode,
  outputDemand: "values" | "existence" = "values",
): RelNode {
  if (outputDemand === "values" && !hasComputation(node)) return node;
  const required = new Set(outputDemand === "values" ? node.output.map((c) => c.name) : []);
  return prune(move(materialize(prune(node, required))), required);
}
