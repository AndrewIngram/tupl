import { copyNormalizedSchemaBindings, getNormalizedTableBinding } from "./normalized-schema-state";
import { buildRegisteredSchemaDefinition } from "./registered-schema-building";
import { finalizeSchemaDefinition } from "./schema-finalization-validation";

/**
 * Schema finalization is the curated normalization surface for hidden binding state, schema build
 * assembly, and the invariants that make finalized logical schemas safe to execute against.
 */
export { buildRegisteredSchemaDefinition, copyNormalizedSchemaBindings, getNormalizedTableBinding };
export { finalizeSchemaDefinition };
