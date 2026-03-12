/**
 * Binding normalization is the curated schema-model surface for normalized column bindings.
 */
export {
  buildColumnSourceMapFromBindings,
  getNormalizedColumnBindings,
  getNormalizedColumnSourceMap,
  resolveNormalizedColumnSource,
} from "./normalized-column-sources";
export { normalizeColumnBinding } from "./column-binding-normalizer";
export { resolveColumnExpr } from "./normalized-column-expr";
export { validateCalculatedColumnDependencies } from "./calculated-column-validation";
