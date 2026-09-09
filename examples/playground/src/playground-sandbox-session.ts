import type {
  QueryExecutionPlan,
  QuerySession,
  QueryStepEvent,
  QueryStepState,
} from "@tupl/runtime/session";
import type { QueryRow } from "@tupl/schema";

import { requestSandboxWorker } from "./playground-sandbox-client";

const SANDBOX_SESSION_PROXY = Symbol("playgroundSandboxSessionProxy");

interface SandboxSessionState {
  sessionId: string;
  plan: QueryExecutionPlan;
  stepStates: Map<string, QueryStepState>;
  result: QueryRow[] | null;
  done: boolean;
  error: string | null;
  events: QueryStepEvent[];
}

type SandboxQuerySession = QuerySession & {
  [SANDBOX_SESSION_PROXY]: SandboxSessionState;
};

function createInitialStepStates(plan: QueryExecutionPlan): Map<string, QueryStepState> {
  return new Map(
    plan.steps.map((step) => [
      step.id,
      {
        id: step.id,
        kind: step.kind,
        status: "ready",
        summary: step.summary,
        dependsOn: step.dependsOn,
        ...(step.relNodeId ? { relNodeId: step.relNodeId } : {}),
        ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
      } satisfies QueryStepState,
    ]),
  );
}

function applyStepEvent(state: SandboxSessionState, event: QueryStepEvent): void {
  state.events.push(event);
  if (!event.stepId) {
    return;
  }

  const current = state.stepStates.get(event.stepId);
  if (current && current.status !== "ready" && current.occurrence > event.occurrence) {
    return;
  }

  state.stepStates.set(event.stepId, { ...event, id: event.stepId });
}

export function isSandboxQuerySession(session: QuerySession): session is SandboxQuerySession {
  return SANDBOX_SESSION_PROXY in session;
}

export function createSandboxQuerySession(
  sessionId: string,
  plan: QueryExecutionPlan,
  initialEvents: QueryStepEvent[] = [],
  initialResult: QueryRow[] | null = null,
  initialDone = false,
  initialError: string | null = null,
): QuerySession {
  const state: SandboxSessionState = {
    sessionId,
    plan,
    stepStates: createInitialStepStates(plan),
    result: initialResult,
    done: initialDone,
    error: initialError,
    events: [],
  };

  for (const event of initialEvents) {
    applyStepEvent(state, event);
  }

  const session: SandboxQuerySession = {
    [SANDBOX_SESSION_PROXY]: state,
    getPlan() {
      return state.plan;
    },
    async next() {
      if (state.done) {
        if (state.error) {
          throw new Error(state.error);
        }
        return {
          done: true,
          result: state.result ?? [],
        };
      }

      const next = await requestSandboxWorker("session_next", {
        sessionId: state.sessionId,
      });
      if ("done" in next) {
        state.done = true;
        state.result = next.result;
        state.error = null;
        return next;
      }

      applyStepEvent(state, next);
      return next;
    },
    async runToCompletion() {
      if (state.done) {
        if (state.error) {
          throw new Error(state.error);
        }
        return state.result ?? [];
      }

      const snapshot = await requestSandboxWorker("session_run_to_completion", {
        sessionId: state.sessionId,
      });
      for (const event of snapshot.events) {
        applyStepEvent(state, event);
      }
      state.done = snapshot.done;
      state.result = snapshot.result;
      state.error = snapshot.error;
      if (snapshot.error) {
        throw new Error(snapshot.error);
      }
      return snapshot.result ?? [];
    },
    getResult() {
      return state.result;
    },
    getStepState(stepId: string) {
      return state.stepStates.get(stepId);
    },
  };

  return session;
}

export function readSandboxSessionId(session: QuerySession): string {
  return (session as SandboxQuerySession)[SANDBOX_SESSION_PROXY].sessionId;
}

export function applySandboxCompletionSnapshot(
  session: QuerySession,
  snapshot: {
    events: QueryStepEvent[];
    result: QueryRow[] | null;
    done: boolean;
    error: string | null;
  },
): void {
  const state = (session as SandboxQuerySession)[SANDBOX_SESSION_PROXY];
  for (const event of snapshot.events) {
    applyStepEvent(state, event);
  }
  state.done = snapshot.done;
  state.result = snapshot.result;
  state.error = snapshot.error;
}
