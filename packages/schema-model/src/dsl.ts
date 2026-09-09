/**
 * DSL contracts expose schema-builder token and view-shape types that planner internals and
 * advanced tooling may need explicitly. They are intentionally off the root to keep the ordinary
 * schema-model surface focused on authoring behavior rather than DSL implementation detail.
 */
export type {
  DslTableDefinition,
  DslViewDefinition,
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaTypedColumnDefinition,
} from "./contracts/table-definition-contracts";
export type { SchemaTypedColumnBuilder } from "./contracts/typed-column-builder-contracts";
export type {
  SchemaDslViewRelHelpers,
  SchemaViewAggregateMetric,
  SchemaViewAggregateNode,
  SchemaViewEqExpr,
  SchemaViewJoinNode,
  SchemaViewRelNode,
  SchemaViewRelNodeInput,
  SchemaViewScanNode,
} from "./contracts/schema-view-contracts";
export type { SchemaColRefToken, SchemaDslTableToken } from "./contracts/schema-contracts";

export type { SchemaDerivedValue, SchemaValueHandle } from "./dsl/derive";
