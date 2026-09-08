/**
 * Normalization is the curated schema-model surface for schema finalization and provider binding.
 */
export {
  buildRegisteredSchemaDefinition,
  copyNormalizedSchemaBindings,
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
} from "./normalization/schema-finalization";
export {
  getNormalizedColumnBindings,
  getNormalizedColumnSourceMap,
  resolveNormalizedColumnSource,
} from "./normalization/normalized-column-sources";
export {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
} from "./normalization/entity-bindings";
export {
  resolveTableProvider,
  validateProviderBindings,
} from "./normalization/provider-validation";
