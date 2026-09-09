import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

function resolveRepoPath(path) {
  return fileURLToPath(new URL(`../../${path}`, import.meta.url));
}

// Workspace execution must not mix built entrypoints with source-owned registries.
// More-specific subpaths precede roots because Vite's string aliases also match prefixes.
const coreAliases = ["foundation", "provider-kit", "schema-model", "planner", "runtime", "schema"]
  .flatMap((directory) => {
    const manifest = JSON.parse(
      readFileSync(resolveRepoPath(`packages/${directory}/package.json`), "utf8"),
    );
    return Object.entries(manifest.exports).map(([subpath, conditions]) => {
      if (!conditions.source)
        throw new Error(`${manifest.name}${subpath} needs a workspace source export.`);
      return [
        manifest.name + (subpath === "." ? "" : subpath.slice(1)),
        resolveRepoPath(`packages/${directory}/${conditions.source.slice(2)}`),
      ];
    });
  })
  .sort(([left], [right]) => right.length - left.length);

export const localPackageAliases = {
  ...Object.fromEntries(coreAliases),
  "@tupl/provider-drizzle": resolveRepoPath("packages/provider-drizzle/src/index.ts"),
  "@tupl/provider-ioredis": resolveRepoPath("packages/provider-ioredis/src/index.ts"),
  "@tupl/provider-kysely": resolveRepoPath("packages/provider-kysely/src/index.ts"),
  "@tupl/provider-objection": resolveRepoPath("packages/provider-objection/src/index.ts"),
  "@tupl/example-shared": resolveRepoPath("examples/_shared/src/index.ts"),
};
