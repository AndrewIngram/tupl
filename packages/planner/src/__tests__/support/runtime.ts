import type { RelNode } from "@tupl/foundation";
import { bindProviderEntities, type ProviderAdapter } from "@tupl/provider-kit";
import type { LookupManyCapableProviderAdapter } from "@tupl/provider-kit/shapes";

type ProviderInput<TContext> = {
  name?: string;
  entities?: Record<string, unknown>;
  fallbackPolicy?: unknown;
  canExecute(rel: RelNode, context: TContext): unknown;
  estimate?(rel: RelNode, context: TContext): unknown;
  compile?(rel: RelNode, context: TContext): unknown;
  describeCompiledPlan?(plan: unknown, context: TContext): unknown;
  execute?(plan: unknown, context: TContext): unknown;
} & Partial<LookupManyCapableProviderAdapter<TContext>>;

export function finalizeProviders<TContext>(
  providers: Record<string, ProviderInput<TContext>>,
): Record<string, ProviderAdapter<TContext>> {
  for (const [providerName, adapter] of Object.entries(providers)) {
    const boundAdapter = adapter as ProviderAdapter<TContext>;
    if (!boundAdapter.name) {
      boundAdapter.name = providerName;
    }
    bindProviderEntities(boundAdapter);
  }

  return providers as Record<string, ProviderAdapter<TContext>>;
}
