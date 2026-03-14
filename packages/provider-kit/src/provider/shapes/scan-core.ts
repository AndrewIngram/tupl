import {
  buildCapabilityReport,
  type ProviderCapabilityAtom,
  type ProviderCapabilityReport,
} from "..";
import type { RelNode } from "@tupl/foundation";

export interface ScanEntityBinding {
  entity: string;
}

export function buildScanUnsupportedReport(
  rel: RelNode,
  supportedAtoms: readonly ProviderCapabilityAtom[],
  reason: string,
): ProviderCapabilityReport {
  return buildCapabilityReport(rel, supportedAtoms, reason);
}
