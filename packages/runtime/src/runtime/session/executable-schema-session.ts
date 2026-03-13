import { Result } from "better-result";

import type { TuplResult } from "@tupl/foundation";
import type { ExecutableSchema } from "../contracts";
import { readExecutableSchemaSessionAccessResult } from "../executable-schema-runtime";

import type { ExecutableSchemaSessionInput, QuerySession } from "./contracts";

/**
 * Executable-schema session helpers are the advanced session-observer entrypoint for callers that
 * need step-by-step execution state without widening the main executable-schema facade.
 */
export function createExecutableSchemaSession<TContext>(
  executableSchema: ExecutableSchema<TContext>,
  input: ExecutableSchemaSessionInput<TContext>,
): TuplResult<QuerySession> {
  const accessResult = readExecutableSchemaSessionAccessResult(executableSchema);
  if (Result.isError(accessResult)) {
    return accessResult;
  }

  return accessResult.value.createSession(input);
}
