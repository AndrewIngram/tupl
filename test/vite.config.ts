import { defineConfig } from "vite-plus";

import { localPackageAliases } from "../scripts/vite/localPackageAliases.js";

export default defineConfig({
  ...(process.env.VITEST && process.env.CI
    ? { cacheDir: "../node_modules/.vite-ci/workspace-tests" }
    : {}),
  resolve: {
    alias: localPackageAliases,
    conditions: ["source", "module", "import", "default"],
  },
  test: {
    include: ["**/__tests__/**/*.test.{ts,tsx}"],
    exclude: ["**/node_modules/**", "**/dist/**", "**/.{idea,git,cache,output,temp}/**"],
    coverage: {
      provider: "v8" as const,
      reporter: ["text", "html", "lcov", "json-summary"],
      reportsDirectory: "../coverage/workspace-tests",
      exclude: ["**/*.d.ts", "**/*.result-type-inference.ts"],
    },
    testTimeout: 30_000,
  },
});
