import { fileURLToPath } from "node:url";

function resolveRepoPath(path: string) {
  return fileURLToPath(new URL(`../../${path}`, import.meta.url));
}

export const localPackageAliases = {
  "@tupl/schema": resolveRepoPath("packages/schema/src/index.ts"),
  "@tupl/provider-drizzle": resolveRepoPath("packages/provider-drizzle/src/index.ts"),
  "@tupl/provider-ioredis": resolveRepoPath("packages/provider-ioredis/src/index.ts"),
  "@tupl/provider-kysely": resolveRepoPath("packages/provider-kysely/src/index.ts"),
  "@tupl/provider-objection": resolveRepoPath("packages/provider-objection/src/index.ts"),
  "@tupl/example-shared": resolveRepoPath("examples/_shared/src/index.ts"),
} as const;
