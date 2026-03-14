import { describe, expect, it } from "vitest";

import { extractSimpleRelScanRequest } from "@tupl/provider-kit/shapes";
import type { RelNode } from "@tupl/foundation";

describe("simple scan request extraction", () => {
  it("extracts a simple scan pipeline with project, filter, sort, and limit", () => {
    const rel: RelNode = {
      id: "limit_users",
      kind: "limit_offset",
      convention: "provider:memory",
      limit: 5,
      offset: 2,
      output: [{ name: "id" }, { name: "email" }],
      input: {
        id: "sort_users",
        kind: "sort",
        convention: "provider:memory",
        orderBy: [{ source: { column: "email" }, direction: "asc" }],
        output: [{ name: "id" }, { name: "email" }],
        input: {
          id: "project_users",
          kind: "project",
          convention: "provider:memory",
          columns: [
            {
              kind: "column",
              source: { column: "id" },
              output: "id",
            },
            {
              kind: "column",
              source: { column: "email" },
              output: "email",
            },
          ],
          output: [{ name: "id" }, { name: "email" }],
          input: {
            id: "filter_users",
            kind: "filter",
            convention: "provider:memory",
            where: [{ op: "eq", column: "org_id", value: "org_1" }],
            output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.org_id" }],
            input: {
              id: "scan_users",
              kind: "scan",
              convention: "provider:memory",
              table: "users",
              alias: "u",
              select: ["id", "email", "org_id"],
              output: [{ name: "u.id" }, { name: "u.email" }, { name: "u.org_id" }],
            },
          },
        },
      },
    };

    expect(extractSimpleRelScanRequest(rel)).toEqual({
      table: "users",
      alias: "u",
      select: ["id", "email"],
      where: [{ op: "eq", column: "org_id", value: "org_1" }],
      orderBy: [{ column: "email", direction: "asc" }],
      limit: 5,
      offset: 2,
    });
  });

  it("rejects non-simple shapes", () => {
    const rel: RelNode = {
      id: "project_users",
      kind: "project",
      convention: "provider:memory",
      columns: [
        {
          kind: "expr",
          expr: {
            kind: "function",
            name: "upper",
            args: [{ kind: "column", ref: { column: "email" } }],
          },
          output: "email_upper",
        },
      ],
      output: [{ name: "email_upper" }],
      input: {
        id: "scan_users",
        kind: "scan",
        convention: "provider:memory",
        table: "users",
        select: ["email"],
        output: [{ name: "email" }],
      },
    };

    expect(extractSimpleRelScanRequest(rel)).toBeNull();
  });
});
