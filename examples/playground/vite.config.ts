import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

import { localPackageAliases } from "../../scripts/vite/localPackageAliases.js";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: localPackageAliases,
    conditions: ["source", "module", "import", "default"],
  },
  worker: {
    format: "es" as const,
  },
  server: {
    host: true,
    port: 5174,
  },
});
