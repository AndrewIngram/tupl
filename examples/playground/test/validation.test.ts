import { describe, expect, it } from "vitest";

import { FACADE_SCHEMA, SCENARIO_PRESETS, serializeJson } from "../src/examples";
import {
  parseDownstreamRowsText,
  parseFacadeSchemaText,
  PLAYGROUND_SCHEMA_JSON_SCHEMA,
  buildRowsJsonSchema,
  buildTableRowsJsonSchema,
} from "../src/validation";

describe("playground/validation", () => {
  it("parses facade schema JSON", () => {
    const schemaResult = parseFacadeSchemaText(serializeJson(FACADE_SCHEMA));

    expect(schemaResult.ok).toBe(true);
    expect(schemaResult.issues).toEqual([]);
  });

  it("rejects downstream rows JSON with unknown columns", () => {
    const scenario = SCENARIO_PRESETS[0];
    if (!scenario) {
      throw new Error("Expected default scenario.");
    }

    const schemaResult = parseFacadeSchemaText(serializeJson(FACADE_SCHEMA));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const invalidRowsText = serializeJson({
      ...scenario.rows,
      users: [
        ...(scenario.rows.users ?? []),
        { id: "u_bad", org_id: "org_acme", email: "bad@example.com", display_name: "Bad", role: "buyer", extra_field: 1 },
      ],
    });

    const rowsResult = parseDownstreamRowsText(invalidRowsText);
    expect(rowsResult.ok).toBe(false);
    expect(rowsResult.issues.some((issue) => issue.message.includes("Unrecognized key"))).toBe(
      true,
    );
  });

  it("builds rows JSON schema from current schema", () => {
    const schemaResult = parseFacadeSchemaText(serializeJson(FACADE_SCHEMA));
    if (!schemaResult.ok || !schemaResult.schema) {
      throw new Error("Expected valid schema.");
    }

    const rowsJsonSchema = buildRowsJsonSchema(schemaResult.schema);
    expect(rowsJsonSchema).toMatchObject({
      type: "object",
      properties: expect.objectContaining({
        my_orders: expect.any(Object),
      }),
    });
  });

  it("builds table rows JSON schema with enum metadata", () => {
    const table = FACADE_SCHEMA.tables.my_orders;
    if (!table) {
      throw new Error("Expected my_orders table.");
    }

    const tableSchema = buildTableRowsJsonSchema(table);
    expect(tableSchema).toMatchObject({
      type: "array",
      items: {
        type: "object",
        properties: {
          status: {
            enum: ["pending", "paid", "shipped"],
          },
        },
      },
    });
  });

  it("schema editor JSON schema allows table constraints", () => {
    const tables = (PLAYGROUND_SCHEMA_JSON_SCHEMA.properties as Record<string, unknown>)
      .tables as Record<string, unknown>;
    const tableDefinition = tables.additionalProperties as Record<string, unknown>;
    const properties = tableDefinition.properties as Record<string, unknown>;
    const constraints = properties.constraints as Record<string, unknown>;
    const constraintsProperties = constraints.properties as Record<string, unknown>;

    expect(constraintsProperties).toHaveProperty("primaryKey");
    expect(constraintsProperties).toHaveProperty("unique");
    expect(constraintsProperties).toHaveProperty("foreignKeys");
    expect(constraintsProperties).toHaveProperty("checks");
  });

  it("schema editor JSON schema allows column-level foreignKey + enum metadata", () => {
    const tables = (PLAYGROUND_SCHEMA_JSON_SCHEMA.properties as Record<string, unknown>)
      .tables as Record<string, unknown>;
    const tableDefinition = tables.additionalProperties as Record<string, unknown>;
    const properties = tableDefinition.properties as Record<string, unknown>;
    const columns = properties.columns as Record<string, unknown>;
    const anyOf = (columns.additionalProperties as Record<string, unknown>).anyOf as Array<Record<
      string,
      unknown
    >>;
    const objectVariant = anyOf.find((entry) => entry.type === "object");
    if (!objectVariant) {
      throw new Error("Expected object-based column schema variant.");
    }

    const objectProperties = (objectVariant.properties as Record<string, unknown>) ?? {};
    expect(objectProperties).toHaveProperty("enum");
    expect(objectProperties).toHaveProperty("foreignKey");
    expect(objectProperties).toHaveProperty("primaryKey");
    expect(objectProperties).toHaveProperty("unique");

    const allOf = (objectVariant.allOf as Array<Record<string, unknown>> | undefined) ?? [];
    expect(allOf.length).toBeGreaterThan(0);
  });
});
