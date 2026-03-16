import { defineConfig } from "vite-plus";

import { localPackageAliases } from "../../scripts/vite/localPackageAliases.ts";

export default defineConfig({
  resolve: {
    alias: localPackageAliases,
    conditions: ["source", "module", "import", "default"],
  },
});
