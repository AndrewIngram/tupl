import { Result, type Result as BetterResult } from "better-result";

import type { TuplError } from "@tupl/foundation";

import type { QuerySession, QuerySessionInput } from "./contracts";
import { tryQueryStep, unwrapQueryResult } from "./diagnostics";
import { createRelExecutionSession } from "./rel-execution-session";
import { resolveSessionPreparationResult } from "./provider-fragment-session";

/**
 * Query-session factory owns end-to-end session preparation and chooses the concrete session kind.
 */
export function createQuerySessionResult<TContext>(
  input: QuerySessionInput<TContext>,
): BetterResult<QuerySession, TuplError> {
  const preparationResult = resolveSessionPreparationResult(input);
  if (Result.isError(preparationResult as any)) {
    return preparationResult as BetterResult<QuerySession, TuplError>;
  }

  const prepared = (preparationResult as any).value as {
    resolvedInput: QuerySessionInput<TContext>;
    guardrails: import("./contracts").QueryGuardrails;
    providerSession: QuerySession | null;
    executableRel: import("@tupl/foundation").RelNode;
    diagnostics: import("./contracts").TuplDiagnostic[];
  };
  if (prepared.providerSession) {
    return Result.ok(prepared.providerSession);
  }

  return tryQueryStep("create relational execution session", () =>
    createRelExecutionSession(
      prepared.resolvedInput,
      prepared.guardrails,
      prepared.executableRel,
      prepared.diagnostics,
    ),
  );
}

export function createQuerySessionInternal<TContext>(
  input: QuerySessionInput<TContext>,
): QuerySession {
  return unwrapQueryResult(createQuerySessionResult(input));
}
