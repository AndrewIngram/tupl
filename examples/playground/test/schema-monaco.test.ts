import { describe, expect, it, vi } from "vitest";

import { configureSchemaTypescriptProject } from "../src/schema-monaco";

describe("playground/schema-monaco", () => {
  it("applies TypeScript project config and registers sqlql types lib", () => {
    const setCompilerOptions = vi.fn();
    const setDiagnosticsOptions = vi.fn();
    const setEagerModelSync = vi.fn();
    const addExtraLib = vi.fn();

    const monaco = {
      languages: {
        typescript: {
          ScriptTarget: {
            ES2020: "ES2020",
          },
          ModuleKind: {
            ESNext: "ESNext",
          },
          ModuleResolutionKind: {
            NodeJs: "NodeJs",
          },
          typescriptDefaults: {
            setCompilerOptions,
            setDiagnosticsOptions,
            setEagerModelSync,
            addExtraLib,
          },
        },
      },
    } as unknown as Parameters<typeof configureSchemaTypescriptProject>[0];

    configureSchemaTypescriptProject(monaco, {
      sqlqlTypesText: "declare module 'sqlql' {}",
      sqlqlTypesLibPath: "file:///types/sqlql/index.d.ts",
    });

    expect(setCompilerOptions).toHaveBeenCalledTimes(1);
    expect(setCompilerOptions).toHaveBeenCalledWith(
      expect.objectContaining({
        strict: true,
        noEmit: true,
        baseUrl: "file:///",
      }),
    );
    expect(setDiagnosticsOptions).toHaveBeenCalledWith({
      noSyntaxValidation: false,
      noSemanticValidation: false,
      noSuggestionDiagnostics: false,
    });
    expect(setEagerModelSync).toHaveBeenCalledWith(true);
    expect(addExtraLib).toHaveBeenCalledWith(
      "declare module 'sqlql' {}",
      "file:///types/sqlql/index.d.ts",
    );
  });
});
