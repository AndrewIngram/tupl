import type { QueryRow } from "@tupl/schema-model";

import type {
  RelExecutionDescriptor,
  RelExecutionObservation,
  RelExecutionObserver,
} from "../execution/execution-observer";
import type {
  QueryExecutionPlan,
  QueryExecutionPlanStep,
  QueryStepEvent,
  QueryStepState,
} from "./contracts";

export function createSessionExecutionObserver(input: {
  plan: QueryExecutionPlan;
  states: Map<string, QueryStepState>;
  rootRelNodeId: string;
  captureRows: "full" | undefined;
  publish: (event: QueryStepEvent) => void;
}): RelExecutionObserver {
  const stepByRelNodeId = new Map<string, QueryExecutionPlanStep>();
  for (const step of input.plan.steps) {
    if (step.relNodeId && !stepByRelNodeId.has(step.relNodeId)) {
      stepByRelNodeId.set(step.relNodeId, step);
    }
  }

  const occurrences = new Map<string, number>();
  let nextExecutionId = 1;
  let nextCompletionIndex = 1;
  let closed = false;
  const active = new Set<{ fail(error: unknown): void }>();

  const ignoredObservation: RelExecutionObservation = {
    updateDescriptor() {},
    complete() {},
    fail() {},
  };

  return {
    start(node, initialDescriptor) {
      if (closed) {
        return ignoredObservation;
      }
      const step = stepByRelNodeId.get(node.id);
      const occurrence = (occurrences.get(node.id) ?? 0) + 1;
      occurrences.set(node.id, occurrence);
      const executionId = `execution_${nextExecutionId}`;
      nextExecutionId += 1;
      const startedAt = Date.now();
      let descriptor = initialDescriptor;
      let settled = false;

      updateRunningState(step, {
        descriptor,
        executionId,
        occurrence,
        relNodeId: node.id,
        startedAt,
      });

      const activeExecution = {
        fail(error: unknown) {
          settle("failed", [], error);
        },
      };
      active.add(activeExecution);

      const settle = (status: "done" | "failed", rows: QueryRow[], error?: unknown) => {
        if (settled || closed) {
          return;
        }
        settled = true;
        active.delete(activeExecution);

        const endedAt = Date.now();
        const executionIndex = nextCompletionIndex;
        nextCompletionIndex += 1;
        const isRoot = node.id === input.rootRelNodeId;
        const eventBase = {
          id: step?.id ?? executionId,
          executionId,
          ...(step ? { stepId: step.id } : {}),
          relNodeId: node.id,
          occurrence,
          kind: descriptor.kind,
          status,
          summary: descriptor.summary,
          dependsOn: step?.dependsOn ?? [],
          executionIndex,
          startedAt,
          endedAt,
          durationMs: Math.max(endedAt - startedAt, 0),
          routeUsed: descriptor.routeUsed,
          ...(step?.diagnostics ? { diagnostics: step.diagnostics } : {}),
        };
        const event: QueryStepEvent =
          status === "done"
            ? {
                ...eventBase,
                status,
                rowCount: rows.length,
                outputRowCount: rows.length,
                ...(descriptor.inputRowCount != null
                  ? { inputRowCount: descriptor.inputRowCount }
                  : {}),
                ...(descriptor.computations ? { computations: descriptor.computations } : {}),
                ...(isRoot && input.captureRows === "full" ? { rows } : {}),
              }
            : {
                ...eventBase,
                status,
                error: errorMessage(error),
              };

        if (step && input.states.get(step.id)?.executionId === executionId) {
          input.states.set(step.id, eventToState(event, step.id));
        }
        input.publish(event);
      };

      const observation: RelExecutionObservation = {
        updateDescriptor(nextDescriptor) {
          if (settled || closed) {
            return;
          }
          descriptor = nextDescriptor;
          updateRunningState(step, {
            descriptor,
            executionId,
            occurrence,
            relNodeId: node.id,
            startedAt,
          });
        },
        complete(rows) {
          settle("done", rows);
        },
        fail(error) {
          settle("failed", [], error);
        },
      };
      return observation;
    },
    close(error) {
      if (error !== undefined) {
        for (const execution of [...active].reverse()) {
          execution.fail(error);
        }
      }
      active.clear();
      closed = true;
    },
  };

  function updateRunningState(
    step: QueryExecutionPlanStep | undefined,
    state: {
      descriptor: RelExecutionDescriptor;
      executionId: string;
      occurrence: number;
      relNodeId: string;
      startedAt: number;
    },
  ) {
    if (!step || closed) {
      return;
    }
    const currentOccurrence = input.states.get(step.id)?.occurrence ?? 0;
    if (currentOccurrence > state.occurrence) {
      return;
    }
    input.states.set(step.id, {
      id: step.id,
      relNodeId: state.relNodeId,
      executionId: state.executionId,
      occurrence: state.occurrence,
      kind: state.descriptor.kind,
      status: "running",
      summary: state.descriptor.summary,
      dependsOn: step.dependsOn,
      startedAt: state.startedAt,
      routeUsed: state.descriptor.routeUsed,
      ...(step.diagnostics ? { diagnostics: step.diagnostics } : {}),
    });
  }
}

function eventToState(event: QueryStepEvent, id: string): QueryStepState {
  return {
    ...event,
    id,
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
