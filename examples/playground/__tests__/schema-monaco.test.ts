import { describe, expect, it, vi } from "vite-plus/test";

import { configureSchemaTypescriptProject } from "../src/schema-monaco";

describe("playground/schema-monaco", () => {
  it("applies TypeScript project config and registers workspace libraries", () => {
    const setCompilerOptions = vi.fn();
    const setDiagnosticsOptions = vi.fn();
    const setEagerModelSync = vi.fn();
    const addExtraLib = vi.fn();
    const createModel = vi.fn();

    const monaco = {
      editor: {
        createModel,
      },
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

    configureSchemaTypescriptProject(monaco);
    const addExtraLibCallsAfterFirstConfigure = addExtraLib.mock.calls.length;
    configureSchemaTypescriptProject(monaco);

    expect(setCompilerOptions).toHaveBeenCalledTimes(2);
    expect(setCompilerOptions).toHaveBeenCalledWith(
      expect.objectContaining({
        strict: true,
        noEmit: true,
        baseUrl: "file:///playground/workspace",
      }),
    );
    expect(setDiagnosticsOptions).toHaveBeenCalledWith({
      noSyntaxValidation: false,
      noSemanticValidation: false,
      noSuggestionDiagnostics: false,
    });
    expect(setEagerModelSync).toHaveBeenCalledWith(true);
    expect(addExtraLib).toHaveBeenCalled();
    expect(createModel).not.toHaveBeenCalled();
    expect(addExtraLibCallsAfterFirstConfigure).toBeGreaterThan(1);
    expect(addExtraLib.mock.calls.length).toBe(addExtraLibCallsAfterFirstConfigure);
    expect(createModel).not.toHaveBeenCalled();
  });
});
