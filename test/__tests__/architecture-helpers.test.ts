import { describe, expect, it } from "vite-plus/test";

import { getPublicSourceExports } from "../architecture/package-exports";
import {
  getModuleSpecifiers,
  getSourceExports,
  isForwardingModule,
  referencedWorkspacePackage,
} from "../architecture/typescript-modules";

describe("architecture source analysis", () => {
  it("finds static, side-effect, re-export, and literal dynamic dependencies", () => {
    const contents = `
      import {
        runtimeValue,
        type RuntimeType,
      } from "@tupl/runtime";
      import type { SchemaDefinition } from "@tupl/schema-model";
      import "@tupl/provider-kit/register";
      export {
        type PlannedQuery,
        planQuery,
      } from "@tupl/planner";
      export * from "./local-exports";
      const loadFoundation = () => import("@tupl/foundation");
      type RuntimeSession = import("@tupl/runtime/session").QuerySession;
      const ignored = (specifier: string) => import(specifier);
    `;

    expect(getModuleSpecifiers(contents)).toEqual([
      "@tupl/runtime",
      "@tupl/schema-model",
      "@tupl/provider-kit/register",
      "@tupl/planner",
      "./local-exports",
      "@tupl/foundation",
      "@tupl/runtime/session",
    ]);
  });

  it("reads multiline, multiple, and type-only re-exports", () => {
    const contents = `
      export {
        type QueryPlan,
        planQuery,
      } from "./planning";
      export type { QueryRow } from "./rows";
      export * from "./contracts";
    `;

    expect(getSourceExports(contents)).toEqual([
      {
        moduleSpecifier: "./planning",
        names: ["QueryPlan", "planQuery"],
        typeOnly: false,
      },
      { moduleSpecifier: "./rows", names: ["QueryRow"], typeOnly: true },
      { moduleSpecifier: "./contracts", names: "*", typeOnly: false },
    ]);
    expect(isForwardingModule(contents)).toBe(true);
  });

  it("does not classify a module with implementation as forwarding-only", () => {
    const contents = `
      export { planQuery } from "./planning";
      export function choosePlan() {
        return "local";
      }
    `;

    expect(isForwardingModule(contents)).toBe(false);
  });

  it("resolves aliased and relative references to workspace packages", () => {
    const context = {
      importerFile: "/repo/packages/foundation/src/model.ts",
      packageNamesByDirectory: new Map([
        ["foundation", "@tupl/foundation"],
        ["runtime", "@tupl/runtime"],
      ]),
      repoRoot: "/repo",
    };

    expect(referencedWorkspacePackage({ ...context, specifier: "@tupl/runtime/session" })).toBe(
      "@tupl/runtime",
    );
    expect(
      referencedWorkspacePackage({
        ...context,
        specifier: "../../runtime/src/runtime/session/index",
      }),
    ).toBe("@tupl/runtime");
    expect(referencedWorkspacePackage({ ...context, specifier: "./rel" })).toBe("@tupl/foundation");
    expect(referencedWorkspacePackage({ ...context, specifier: "typescript" })).toBeUndefined();
  });
});

describe("package export analysis", () => {
  it("derives direct and conditional source targets from a package manifest", () => {
    expect(
      getPublicSourceExports({
        name: "@tupl/example",
        exports: {
          ".": "./src/index.ts",
          "./session": {
            source: "./src/session/index.ts",
            types: "./dist/session.d.mts",
            import: "./dist/session.mjs",
          },
        },
      }),
    ).toEqual([
      { packageName: "@tupl/example", subpath: ".", target: "./src/index.ts" },
      {
        packageName: "@tupl/example",
        subpath: "./session",
        target: "./src/session/index.ts",
      },
    ]);
  });

  it("rejects public exports without a source target", () => {
    expect(() =>
      getPublicSourceExports({
        name: "@tupl/example",
        exports: {
          ".": {
            types: "./dist/index.d.mts",
            import: "./dist/index.mjs",
          },
        },
      }),
    ).toThrow("@tupl/example must declare a source export target");
  });
});
