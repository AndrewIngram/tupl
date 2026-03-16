import { defineConfig } from "vite-plus";

import { localPackageAliases } from "../../scripts/vite/localPackageAliases.js";

export default defineConfig({
  resolve: {
    alias: localPackageAliases,
    conditions: ["source", "module", "import", "default"],
  },
  pack: {
    entry: ["src/index.ts"],
    format: ["esm", "cjs"],
    sourcemap: true,
    clean: true,
    dts: true,
    tsconfig: "tsconfig.build.json",
  },
});
