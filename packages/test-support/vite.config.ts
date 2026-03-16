import { defineConfig } from "vite-plus";

import { localPackageAliases } from "../../scripts/vite/localPackageAliases.js";

export default defineConfig({
  resolve: {
    alias: localPackageAliases,
    conditions: ["source", "module", "import", "default"],
  },
});
