import type { RelNode } from "@tupl/foundation";
import type { ProviderAdapter, ProvidersMap } from "./contracts";
import { getDataEntityProvider } from "./entity-handles";

/**
 * Resolve a named provider from the registry or an entity owned by the relation.
 * Private view inputs carry their provider on the entity, without a public table registration.
 */
export function resolveRelProviderAdapter<TContext>(
  node: RelNode,
  providerName: string,
  providers: ProvidersMap<TContext>,
): ProviderAdapter<TContext> | undefined {
  const registered = providers[providerName];
  if (registered) return registered;

  const visit = (current: RelNode): ProviderAdapter<TContext> | undefined => {
    switch (current.kind) {
      case "scan":
        return current.entity?.provider === providerName
          ? getDataEntityProvider(current.entity)
          : undefined;
      case "filter":
      case "project":
      case "aggregate":
      case "window":
      case "sort":
      case "limit_offset":
        return visit(current.input);
      case "join":
      case "set_op":
      case "correlate":
        return visit(current.left) ?? visit(current.right);
      case "with":
        for (const cte of current.ctes) {
          const provider = visit(cte.query);
          if (provider) return provider;
        }
        return visit(current.body);
      case "repeat_union":
        return visit(current.seed) ?? visit(current.iterative);
      case "values":
      case "cte_ref":
        return undefined;
    }
  };
  return visit(node);
}
