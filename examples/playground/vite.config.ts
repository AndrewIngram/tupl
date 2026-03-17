import { readFileSync } from "node:fs";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

import { localPackageAliases } from "../../scripts/vite/localPackageAliases.js";

const PGLITE_PACKAGE_NAME = "@electric-sql/pglite";
const PGLITE_CDN_VIRTUAL_ID = "virtual:tupl/playground-pglite-cdn";
const RESOLVED_PGLITE_CDN_VIRTUAL_ID = `\0${PGLITE_CDN_VIRTUAL_ID}`;

function readPinnedPgliteCdnUrl(): string {
  const packageJson = JSON.parse(
    readFileSync(new URL("./package.json", import.meta.url), "utf8"),
  ) as {
    dependencies?: Record<string, string>;
  };
  const specifier = packageJson.dependencies?.["@electric-sql/pglite"];
  if (typeof specifier !== "string" || specifier.length === 0) {
    throw new Error("examples/playground/package.json is missing @electric-sql/pglite.");
  }

  const version = specifier.replace(/^[~^]/u, "");
  return `https://cdn.jsdelivr.net/npm/@electric-sql/pglite@${version}/dist/index.js`;
}

function pgliteCdnPlugin(cdnUrl: string): Plugin {
  return {
    name: "playground-pglite-cdn",
    enforce: "pre",
    resolveId(id) {
      if (id === PGLITE_PACKAGE_NAME) {
        return RESOLVED_PGLITE_CDN_VIRTUAL_ID;
      }

      if (id === PGLITE_CDN_VIRTUAL_ID) {
        return RESOLVED_PGLITE_CDN_VIRTUAL_ID;
      }

      if (id === cdnUrl) {
        return {
          id,
          external: true,
        };
      }

      return null;
    },
    load(id) {
      if (id !== RESOLVED_PGLITE_CDN_VIRTUAL_ID) {
        return null;
      }

      return `
export * from ${JSON.stringify(cdnUrl)};
`.trim();
    },
  };
}

export default defineConfig(() => {
  const pinnedPgliteCdnUrl = readPinnedPgliteCdnUrl();

  return {
    plugins: [react(), !process.env.VITEST ? pgliteCdnPlugin(pinnedPgliteCdnUrl) : null].filter(
      Boolean,
    ),
    resolve: {
      alias: Object.entries(localPackageAliases).map(([find, replacement]) => ({
        find,
        replacement,
      })),
      conditions: ["source", "module", "import", "default"],
    },
    worker: {
      format: "es" as const,
    },
    server: {
      host: true,
      port: 5174,
    },
  };
});
