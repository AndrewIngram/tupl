import type { SchemaDefinition } from "./schema-contracts";

/**
 * Enum contracts describe linked enum resolution behavior.
 */
export interface EnumLinkReference {
  table: string;
  column: string;
}

export interface ResolveSchemaLinkedEnumsOptions {
  resolveEnumValues?: (
    ref: EnumLinkReference,
    schema: SchemaDefinition,
  ) => readonly string[] | undefined;
  onUnresolved?: "throw" | "ignore";
  strictUnmapped?: boolean;
}
