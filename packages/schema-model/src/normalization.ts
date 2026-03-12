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
} from "./normalization/binding-normalization";
export {
  createPhysicalBindingFromEntity,
  createTableDefinitionFromEntity,
} from "./normalization/entity-bindings";
export {
  resolveTableProvider,
  resolveTableProviderResult,
  validateProviderBindings,
  validateProviderBindingsResult,
} from "./normalization/provider-validation";
