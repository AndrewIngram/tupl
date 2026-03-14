import { Result, type Result as BetterResult } from "better-result";

import type { RelNode, TuplError } from "@tupl/foundation";
import {
  normalizeCapability,
  type Provider,
  type ProviderCapabilityReport,
  type ProviderFragment,
} from "@tupl/provider-kit";
import { buildProviderFragmentForRelResult } from "@tupl/planner";

import type { QueryInput, TuplDiagnostic } from "../contracts";
import { buildCapabilityDiagnostics, tryQueryStep, tryQueryStepAsync } from "../diagnostics";
import { isPromiseLike } from "../policy";

/**
 * Provider capability owns fragment construction and provider capability resolution.
 */
export interface QueryCapabilityResolution<TContext> {
  fragment: ProviderFragment | null;
  provider: Provider<TContext> | null;
  report: ProviderCapabilityReport | null;
  diagnostics: TuplDiagnostic[];
}

function emptyCapabilityResolution<TContext>(): QueryCapabilityResolution<TContext> {
  return {
    fragment: null,
    provider: null,
    report: null,
    diagnostics: [],
  };
}

function resolveCapabilityTargetResult<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<
  {
    fragment: ProviderFragment | null;
    provider: ProviderAdapter<TContext> | null;
  },
  TuplError
> {
  const fragmentResult = buildProviderFragmentForRelResult(rel, input.schema, input.context);
  if (Result.isError(fragmentResult)) {
    return fragmentResult;
  }

  const fragment = fragmentResult.value;
  if (!fragment) {
    return Result.ok({
      fragment: null,
      provider: null,
    });
  }

  return Result.ok({
    fragment,
    provider: input.providers[fragment.provider] ?? null,
  });
}

function buildCapabilityResolution<TContext>(
  input: QueryInput<TContext>,
  inputResolution: {
    fragment: ProviderFragment;
    provider: ProviderAdapter<TContext>;
    report: ProviderCapabilityReport;
  },
): QueryCapabilityResolution<TContext> {
  return {
    fragment: inputResolution.fragment,
    provider: inputResolution.provider,
    report: inputResolution.report,
    diagnostics: buildCapabilityDiagnostics(
      inputResolution.provider,
      inputResolution.fragment,
      inputResolution.report,
      input.fallbackPolicy,
    ),
  };
}

export async function resolveProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): Promise<BetterResult<QueryCapabilityResolution<TContext>, TuplError>> {
  const targetResult = resolveCapabilityTargetResult(input, rel);
  if (Result.isError(targetResult)) {
    return targetResult;
  }

  const target = targetResult.value;
  if (!target.fragment || !target.provider) {
    return Result.ok(
      target.fragment
        ? {
            fragment: target.fragment,
            provider: null,
            report: null,
            diagnostics: [],
          }
        : emptyCapabilityResolution<TContext>(),
    );
  }

  const { fragment, provider } = target;

  const capabilityResult = await tryQueryStepAsync("resolve provider capability", () =>
    Promise.resolve(provider.canExecute(fragment, input.context)),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  return Result.ok(
    buildCapabilityResolution(input, {
      fragment,
      provider,
      report: normalizeCapability(capabilityResult.value),
    }),
  );
}

export function resolveSyncProviderCapabilityForRel<TContext>(
  input: QueryInput<TContext>,
  rel: RelNode,
): BetterResult<QueryCapabilityResolution<TContext> | null, TuplError> {
  const targetResult = resolveCapabilityTargetResult(input, rel);
  if (Result.isError(targetResult)) {
    return targetResult;
  }

  const target = targetResult.value;
  if (!target.fragment || !target.provider) {
    return Result.ok(
      target.fragment
        ? {
            fragment: target.fragment,
            provider: null,
            report: null,
            diagnostics: [],
          }
        : emptyCapabilityResolution<TContext>(),
    );
  }

  const { fragment, provider } = target;

  const capabilityResult = tryQueryStep("resolve provider capability", () =>
    provider.canExecute(fragment, input.context),
  );
  if (Result.isError(capabilityResult)) {
    return capabilityResult;
  }

  const capability = capabilityResult.value;
  if (isPromiseLike(capability)) {
    return Result.ok(null);
  }

  return Result.ok(
    buildCapabilityResolution(input, {
      fragment,
      provider,
      report: normalizeCapability(capability),
    }),
  );
}
