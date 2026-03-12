/// <reference types="vite/client" />

export const PLAYGROUND_WORKSPACE_ROOT_PATH = "/playground/workspace";
export const PLAYGROUND_NODE_MODULES_ROOT_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/node_modules`;

export const PLAYGROUND_WORKSPACE_ROOT_URI = `file://${PLAYGROUND_WORKSPACE_ROOT_PATH}`;
export const PLAYGROUND_SCHEMA_FILE_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/schema.ts`;
export const PLAYGROUND_CONTEXT_FILE_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/context.ts`;
export const PLAYGROUND_DB_PROVIDER_FILE_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/db-provider.ts`;
export const PLAYGROUND_REDIS_PROVIDER_FILE_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/redis-provider.ts`;
export const PLAYGROUND_GENERATED_DB_FILE_PATH = `${PLAYGROUND_WORKSPACE_ROOT_PATH}/generated-db.ts`;

export const PLAYGROUND_SCHEMA_FILE_URI = `file://${PLAYGROUND_SCHEMA_FILE_PATH}`;
export const PLAYGROUND_CONTEXT_FILE_URI = `file://${PLAYGROUND_CONTEXT_FILE_PATH}`;
export const PLAYGROUND_DB_PROVIDER_FILE_URI = `file://${PLAYGROUND_DB_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_REDIS_PROVIDER_FILE_URI = `file://${PLAYGROUND_REDIS_PROVIDER_FILE_PATH}`;
export const PLAYGROUND_GENERATED_DB_FILE_URI = `file://${PLAYGROUND_GENERATED_DB_FILE_PATH}`;

export interface PlaygroundWorkspaceManifest {
  rootPath: string;
  rootUri: string;
  entryPath: string;
  sourceFiles: Record<string, string>;
  declarationFiles: Record<string, string>;
  readonlyPaths: string[];
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

function relativeFromMarker(path: string, marker: string): string {
  const normalized = path.replace(/\\/gu, "/");
  const index = normalized.lastIndexOf(marker);
  if (index === -1) {
    throw new Error(`Unable to resolve virtual workspace path for ${path}`);
  }
  return normalized.slice(index + marker.length);
}

function mapVirtualFiles(
  input: Record<string, string>,
  marker: string,
  targetRoot: string,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [path, contents] of Object.entries(input)) {
    const relativePath = relativeFromMarker(path, marker);
    out[normalizePath(`${targetRoot}/${relativePath}`)] = contents;
  }
  return out;
}

const INTERNAL_FOUNDATION_SOURCE_IMPORTS = import.meta.glob(
  "../../../packages/foundation/src/**/*.ts",
  {
    eager: true,
    import: "default",
    query: "?raw",
  },
) as Record<string, string>;

const INTERNAL_PROVIDER_SOURCE_IMPORTS = import.meta.glob(
  "../../../packages/provider-kit/src/**/*.ts",
  {
    eager: true,
    import: "default",
    query: "?raw",
  },
) as Record<string, string>;

const INTERNAL_SCHEMA_SOURCE_IMPORTS = import.meta.glob(
  "../../../packages/schema-model/src/**/*.ts",
  {
    eager: true,
    import: "default",
    query: "?raw",
  },
) as Record<string, string>;

const INTERNAL_PLANNER_SOURCE_IMPORTS = import.meta.glob("../../../packages/planner/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const INTERNAL_RUNTIME_SOURCE_IMPORTS = import.meta.glob("../../../packages/runtime/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const SCHEMA_SOURCE_IMPORTS = import.meta.glob("../../../packages/schema/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_SOURCE_IMPORTS = import.meta.glob("../../../packages/provider-drizzle/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const IOREDIS_SOURCE_IMPORTS = import.meta.glob("../../../packages/provider-ioredis/src/**/*.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const DRIZZLE_DECLARATION_IMPORTS = import.meta.glob("../node_modules/drizzle-orm/**/*.d.ts", {
  eager: true,
  import: "default",
  query: "?raw",
}) as Record<string, string>;

const PGLITE_DECLARATION_IMPORTS = import.meta.glob(
  "../node_modules/@electric-sql/pglite/**/*.d.ts",
  {
    eager: true,
    import: "default",
    query: "?raw",
  },
) as Record<string, string>;

const BETTER_RESULT_DECLARATION_IMPORTS = {
  ...import.meta.glob("../node_modules/better-result/dist/index.d.mts", {
    eager: true,
    import: "default",
    query: "?raw",
  }),
  ...import.meta.glob("../../../node_modules/better-result/dist/index.d.mts", {
    eager: true,
    import: "default",
    query: "?raw",
  }),
} as Record<string, string>;

const INTERNAL_FOUNDATION_SOURCE_FILES = mapVirtualFiles(
  INTERNAL_FOUNDATION_SOURCE_IMPORTS,
  "/packages/foundation/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/foundation`,
);

const INTERNAL_PROVIDER_SOURCE_FILES = mapVirtualFiles(
  INTERNAL_PROVIDER_SOURCE_IMPORTS,
  "/packages/provider-kit/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/provider-kit`,
);

const INTERNAL_SCHEMA_SOURCE_FILES = mapVirtualFiles(
  INTERNAL_SCHEMA_SOURCE_IMPORTS,
  "/packages/schema-model/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/schema-model`,
);

const INTERNAL_PLANNER_SOURCE_FILES = mapVirtualFiles(
  INTERNAL_PLANNER_SOURCE_IMPORTS,
  "/packages/planner/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/planner`,
);

const INTERNAL_RUNTIME_SOURCE_FILES = mapVirtualFiles(
  INTERNAL_RUNTIME_SOURCE_IMPORTS,
  "/packages/runtime/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/runtime`,
);

const SCHEMA_SOURCE_FILES = mapVirtualFiles(
  SCHEMA_SOURCE_IMPORTS,
  "/packages/schema/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/schema`,
);

const DRIZZLE_SOURCE_FILES = mapVirtualFiles(
  DRIZZLE_SOURCE_IMPORTS,
  "/packages/provider-drizzle/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/provider-drizzle`,
);

const IOREDIS_SOURCE_FILES = mapVirtualFiles(
  IOREDIS_SOURCE_IMPORTS,
  "/packages/provider-ioredis/src/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/provider-ioredis`,
);

const DRIZZLE_DECLARATION_FILES = mapVirtualFiles(
  DRIZZLE_DECLARATION_IMPORTS,
  "/node_modules/drizzle-orm/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/drizzle-orm`,
);

const PGLITE_DECLARATION_FILES = mapVirtualFiles(
  PGLITE_DECLARATION_IMPORTS,
  "/node_modules/@electric-sql/pglite/",
  `${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@electric-sql/pglite`,
);

const BETTER_RESULT_DECLARATION_FILE = Object.values(BETTER_RESULT_DECLARATION_IMPORTS)[0];
if (typeof BETTER_RESULT_DECLARATION_FILE !== "string") {
  throw new Error("Unable to load better-result declarations for playground workspace.");
}

const BETTER_RESULT_DECLARATION_FILES: Record<string, string> = {
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/better-result/index.d.ts`]: BETTER_RESULT_DECLARATION_FILE,
};

const HOST_PACKAGE_DECLARATION_FILES: Record<string, string> = {
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@playground/runtime/index.d.ts`]: `
import type { IoredisProviderOperation, RedisLike } from "@tupl/provider-ioredis";

export interface PlaygroundIoredisRuntime {
  redis: RedisLike;
  recordOperation?: (operation: IoredisProviderOperation) => void;
}

export declare function getPlaygroundIoredisRuntime(): PlaygroundIoredisRuntime;
`.trim(),
};

const HOST_PACKAGE_SOURCE_FILES: Record<string, string> = {
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@playground/provider-ioredis-provider-core/index.ts`]: `
export * from "@tupl/provider-ioredis";

import { getPlaygroundIoredisRuntime } from "@playground/runtime";

export const playgroundIoredisRuntime = getPlaygroundIoredisRuntime();
`.trim(),
};

const PUBLIC_SUBPATH_SOURCE_FILES: Record<string, string> = {
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/provider-kit/shapes/index.ts`]:
    'export * from "../provider/shapes";',
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@tupl/runtime/executor.ts`]:
    'export * from "./runtime/executor";',
};

const PGLITE_ROOT_DECLARATION_FILES: Record<string, string> = {
  [`${PLAYGROUND_NODE_MODULES_ROOT_PATH}/@electric-sql/pglite/index.d.ts`]:
    'export * from "./dist/index";',
};

const sourceFiles = {
  ...INTERNAL_FOUNDATION_SOURCE_FILES,
  ...INTERNAL_PROVIDER_SOURCE_FILES,
  ...INTERNAL_SCHEMA_SOURCE_FILES,
  ...INTERNAL_PLANNER_SOURCE_FILES,
  ...INTERNAL_RUNTIME_SOURCE_FILES,
  ...SCHEMA_SOURCE_FILES,
  ...DRIZZLE_SOURCE_FILES,
  ...IOREDIS_SOURCE_FILES,
  ...PUBLIC_SUBPATH_SOURCE_FILES,
  ...HOST_PACKAGE_SOURCE_FILES,
};

const declarationFiles = {
  ...DRIZZLE_DECLARATION_FILES,
  ...PGLITE_DECLARATION_FILES,
  ...PGLITE_ROOT_DECLARATION_FILES,
  ...BETTER_RESULT_DECLARATION_FILES,
  ...HOST_PACKAGE_DECLARATION_FILES,
};

const readonlyPaths = [...Object.keys(sourceFiles), ...Object.keys(declarationFiles)].sort();

const PLAYGROUND_WORKSPACE_MANIFEST = {
  rootPath: PLAYGROUND_WORKSPACE_ROOT_PATH,
  rootUri: PLAYGROUND_WORKSPACE_ROOT_URI,
  entryPath: PLAYGROUND_SCHEMA_FILE_PATH,
  sourceFiles,
  declarationFiles,
  readonlyPaths,
} satisfies PlaygroundWorkspaceManifest;

export function readPlaygroundWorkspaceManifest(): PlaygroundWorkspaceManifest {
  return PLAYGROUND_WORKSPACE_MANIFEST;
}
