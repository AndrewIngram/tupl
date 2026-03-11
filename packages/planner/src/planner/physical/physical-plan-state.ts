import type { PhysicalStep } from "../physical";

export interface PhysicalPlanningState {
  steps: PhysicalStep[];
}

export function createPhysicalPlanningState(): PhysicalPlanningState {
  return { steps: [] };
}

export function recordPhysicalStep(state: PhysicalPlanningState, step: PhysicalStep): string {
  state.steps.push(step);
  return step.id;
}
