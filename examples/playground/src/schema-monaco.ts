import type * as Monaco from "monaco-editor";

import {
  buildPlaygroundStaticWorkspaceSnapshot,
  PLAYGROUND_WORKSPACE_ROOT_URI,
} from "./playground-workspace";

const STATIC_WORKSPACE = buildPlaygroundStaticWorkspaceSnapshot();

let workspaceLibrariesRegistered = false;

export function configureSchemaTypescriptProject(monaco: typeof Monaco): void {
  monaco.languages.typescript.typescriptDefaults.setCompilerOptions({
    target: monaco.languages.typescript.ScriptTarget.ES2020,
    module: monaco.languages.typescript.ModuleKind.ESNext,
    moduleResolution: monaco.languages.typescript.ModuleResolutionKind.NodeJs,
    strict: true,
    noEmit: true,
    esModuleInterop: true,
    allowSyntheticDefaultImports: true,
    allowNonTsExtensions: true,
    allowImportingTsExtensions: true,
    skipLibCheck: true,
    lib: ["lib.es2021.d.ts", "lib.dom.d.ts"],
    baseUrl: PLAYGROUND_WORKSPACE_ROOT_URI,
    rootDirs: [PLAYGROUND_WORKSPACE_ROOT_URI],
  });
  monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
    noSyntaxValidation: false,
    noSemanticValidation: false,
    noSuggestionDiagnostics: false,
  });
  monaco.languages.typescript.typescriptDefaults.setEagerModelSync(true);

  if (workspaceLibrariesRegistered) {
    return;
  }

  // These files only need to exist for TS resolution and diagnostics. Creating editor
  // models for the whole readonly workspace triggers Monaco listener-pressure warnings.
  for (const [path, contents] of Object.entries(STATIC_WORKSPACE.sourceFiles)) {
    monaco.languages.typescript.typescriptDefaults.addExtraLib(contents, `file://${path}`);
  }
  for (const [path, contents] of Object.entries(STATIC_WORKSPACE.declarationFiles)) {
    monaco.languages.typescript.typescriptDefaults.addExtraLib(contents, `file://${path}`);
  }

  workspaceLibrariesRegistered = true;
}
