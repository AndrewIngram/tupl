/**
 * Normalization is the curated schema-model surface for schema finalization and provider binding.
 */
export {
  buildRegisteredSchemaDefinition,
  copyNormalizedSchemaBindings,
  finalizeSchemaDefinition,
  getNormalizedTableBinding,
} from "./schema-finalization";
export {
  getNormalizedColumnBindings,
  getNormalizedColumnSourceMap,
  resolveNormalizedColumnSource,
} from "./binding-normalization";
export {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
} from "./entity-bindings";
export {
  resolveTableProvider,
  resolveTableProviderResult,
  validateProviderBindings,
  validateProviderBindingsResult,
} from "./provider-validation";
