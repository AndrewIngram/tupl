import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
      "relational-sql": "src/relational-sql.ts",
      shapes: "src/provider/shapes/index.ts",
      testing: "src/testing.ts",
    },
    format: ["esm", "cjs"],
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
