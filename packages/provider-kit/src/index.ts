/**
 * Provider kit owns provider contracts, adapter-authoring helpers, entity binding,
 * and reusable shape helpers.
 * Callers can build providers against this surface without depending on schema
 * construction or query execution internals.
 */
export * from "./provider/entity-handles";
export { resolveRelProviderAdapter } from "./provider/rel-provider";
export * from "./provider/operations";
export * from "./provider/capabilities";
export * from "./provider/contracts";
export * from "./provider/relational/relational-provider";
export { createSqlRelationalProviderAdapter } from "./provider/relational/sql-relational-provider";
export * from "./provider/shapes";
