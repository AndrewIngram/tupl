import {
  PLAYGROUND_CONTEXT_FILE_PATH,
  PLAYGROUND_CONTEXT_FILE_URI,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_DB_PROVIDER_FILE_URI,
  PLAYGROUND_GENERATED_DB_FILE_PATH,
  PLAYGROUND_GENERATED_DB_FILE_URI,
  PLAYGROUND_REDIS_PROVIDER_FILE_PATH,
  PLAYGROUND_REDIS_PROVIDER_FILE_URI,
  PLAYGROUND_SCHEMA_FILE_PATH,
  PLAYGROUND_SCHEMA_FILE_URI,
  PLAYGROUND_WORKSPACE_ROOT_URI,
  readPlaygroundWorkspaceManifest,
} from "./playground-workspace-manifest";

export {
  PLAYGROUND_CONTEXT_FILE_PATH,
  PLAYGROUND_CONTEXT_FILE_URI,
  PLAYGROUND_DB_PROVIDER_FILE_PATH,
  PLAYGROUND_DB_PROVIDER_FILE_URI,
  PLAYGROUND_GENERATED_DB_FILE_PATH,
  PLAYGROUND_GENERATED_DB_FILE_URI,
  PLAYGROUND_REDIS_PROVIDER_FILE_PATH,
  PLAYGROUND_REDIS_PROVIDER_FILE_URI,
  PLAYGROUND_SCHEMA_FILE_PATH,
  PLAYGROUND_SCHEMA_FILE_URI,
  PLAYGROUND_WORKSPACE_ROOT_URI,
} from "./playground-workspace-manifest";

export interface PlaygroundWorkspaceUserFiles {
  schemaCode: string;
  contextCode: string;
  dbProviderCode: string;
  redisProviderCode: string;
  generatedDbCode: string;
}

export interface PlaygroundWorkspaceSnapshot {
  rootPath: string;
  rootUri: string;
  entryPath: string;
  userFiles: Record<string, string>;
  sourceFiles: Record<string, string>;
  declarationFiles: Record<string, string>;
  readonlyPaths: string[];
  allFiles: Record<string, string>;
}

function normalizePath(value: string): string {
  const normalized = value.replace(/\\/gu, "/");
  const compact = normalized.replace(/\/+/gu, "/");
  if (compact === "") {
    return "/";
  }
  if (!compact.startsWith("/")) {
    return `/${compact}`;
  }
  return compact;
}

function toFileUri(path: string): string {
  return `file://${normalizePath(path)}`;
}

export function buildPlaygroundWorkspaceSnapshot(
  input: PlaygroundWorkspaceUserFiles,
): PlaygroundWorkspaceSnapshot {
  const manifest = readPlaygroundWorkspaceManifest();
  const userFiles: Record<string, string> = {
    [PLAYGROUND_SCHEMA_FILE_PATH]: input.schemaCode,
    [PLAYGROUND_CONTEXT_FILE_PATH]: input.contextCode,
    [PLAYGROUND_DB_PROVIDER_FILE_PATH]: input.dbProviderCode,
    [PLAYGROUND_REDIS_PROVIDER_FILE_PATH]: input.redisProviderCode,
    [PLAYGROUND_GENERATED_DB_FILE_PATH]: input.generatedDbCode,
  };

  return {
    rootPath: manifest.rootPath,
    rootUri: manifest.rootUri,
    entryPath: manifest.entryPath,
    userFiles,
    sourceFiles: manifest.sourceFiles,
    declarationFiles: manifest.declarationFiles,
    readonlyPaths: manifest.readonlyPaths,
    allFiles: {
      ...manifest.sourceFiles,
      ...manifest.declarationFiles,
      ...userFiles,
    },
  };
}

export function buildPlaygroundStaticWorkspaceSnapshot(): PlaygroundWorkspaceSnapshot {
  return buildPlaygroundWorkspaceSnapshot({
    schemaCode: "",
    contextCode: "",
    dbProviderCode: "",
    redisProviderCode: "",
    generatedDbCode: "",
  });
}

export function readPlaygroundWorkspaceUris(workspace: PlaygroundWorkspaceSnapshot): {
  userUris: Record<string, string>;
  readonlyUris: string[];
  sourceUris: string[];
  declarationUris: string[];
} {
  return {
    userUris: Object.fromEntries(
      Object.keys(workspace.userFiles).map((path) => [path, toFileUri(path)]),
    ),
    readonlyUris: workspace.readonlyPaths.map(toFileUri),
    sourceUris: Object.keys(workspace.sourceFiles).map(toFileUri),
    declarationUris: Object.keys(workspace.declarationFiles).map(toFileUri),
  };
}
