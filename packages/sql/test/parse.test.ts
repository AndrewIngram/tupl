import { describe, expect, it } from "vitest";

import { defineSchema } from "@sqlql/core";
import { parseSql } from "../src";

const schema = defineSchema({
  tables: {
    agent_events: {
      columns: {
        event_id: "text",
      },
    },
  },
});

describe("parseSql", () => {
  it("parses single table selects", () => {
    expect(parseSql({ text: "SELECT * FROM agent_events" }, schema)).toEqual({
      source: "agent_events",
      selectAll: true,
    });
  });

  it("rejects non-select statements", () => {
    expect(() => parseSql({ text: "DELETE FROM agent_events" }, schema)).toThrow(
      "Only SELECT statements are currently supported.",
    );
  });
});
