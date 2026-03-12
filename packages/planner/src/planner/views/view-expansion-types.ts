import type { RelNode } from "@tupl/foundation";

import type { ViewAliasColumnMap } from "../planner-types";

export interface ViewExpansionResult {
  node: RelNode;
  aliases: Map<string, ViewAliasColumnMap>;
}
