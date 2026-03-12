/**
 * Foundation owns the shared relational model, diagnostics, and value helpers.
 * Callers should depend on its abstract data types, not on any higher-level planning or runtime behavior.
 */
export * from "./model/data-entity";
export * from "./model/diagnostics";
export * from "./model/errors";
export * from "./model/primitives";
export * from "./model/rel";
export * from "./stringify-unknown-value";
