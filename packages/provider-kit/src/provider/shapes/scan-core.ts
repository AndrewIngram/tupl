import {
  collectCapabilityAtomsForRel,
  inferRouteFamilyForRel,
  type ProviderCapabilityReport,
} from "..";
import type { RelNode } from "@tupl/foundation";

export interface ScanEntityBinding {
  entity: string;
}

export function buildScanUnsupportedReport(
  rel: RelNode,
  supportedAtoms: readonly string[],
  reason: string,
): ProviderCapabilityReport {
  const requiredAtoms = collectCapabilityAtomsForRel(rel);
  return {
    supported: false,
    reason,
    routeFamily: inferRouteFamilyForRel(rel),
    requiredAtoms,
    missingAtoms: requiredAtoms.filter((atom) => !supportedAtoms.includes(atom)),
  };
}
