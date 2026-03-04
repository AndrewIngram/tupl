import type * as Monaco from "monaco-editor";

export interface SchemaMonacoBootstrap {
  sqlqlTypesText: string;
  sqlqlTypesLibPath: string;
}

export function configureSchemaTypescriptProject(
  monaco: typeof Monaco,
  input: SchemaMonacoBootstrap,
): void {
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
    baseUrl: "file:///",
    rootDirs: ["file:///playground", "file:///types"],
    paths: {
      sqlql: [input.sqlqlTypesLibPath],
    },
  });
  monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
    noSyntaxValidation: false,
    noSemanticValidation: false,
    noSuggestionDiagnostics: false,
  });
  monaco.languages.typescript.typescriptDefaults.setEagerModelSync(true);
  monaco.languages.typescript.typescriptDefaults.addExtraLib(
    input.sqlqlTypesText,
    input.sqlqlTypesLibPath,
  );
}
