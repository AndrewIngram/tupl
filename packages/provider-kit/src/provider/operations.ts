import { Result, type Result as BetterResult } from "better-result";

import type {
  FragmentProvider,
  LookupProvider,
  Provider,
  ProviderOperationResult,
} from "./contracts";

/**
 * Provider operations centralize Result handling and capability predicates for provider contracts.
 */
export const AdapterResult = Result;
export type AdapterResult<T, E = Error> = BetterResult<T, E>;
export type MaybePromise<T> = T | PromiseLike<T>;
export type ProviderRuntimeBinding<TContext, TValue> =
  | TValue
  | ((context: TContext) => MaybePromise<TValue>);

export function unwrapProviderOperationResult<T, E>(outcome: ProviderOperationResult<T, E>): T {
  if (Result.isError(outcome)) {
    throw outcome.error;
  }

  return outcome.value;
}

export function supportsFragmentExecution<TContext>(
  provider: Provider<TContext>,
): provider is FragmentProvider<TContext> {
  return (
    "compile" in provider &&
    typeof provider.compile === "function" &&
    "execute" in provider &&
    typeof provider.execute === "function"
  );
}

export function supportsLookupMany<TContext>(
  provider: Provider<TContext>,
): provider is LookupProvider<TContext> {
  return "lookupMany" in provider && typeof provider.lookupMany === "function";
}
