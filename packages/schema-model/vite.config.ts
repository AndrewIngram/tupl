import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
      constraints: "src/constraints.ts",
      ddl: "src/ddl.ts",
      dsl: "src/dsl.ts",
      definition: "src/definition.ts",
      enums: "src/enums.ts",
      mapping: "src/mapping/index.ts",
      normalized: "src/normalized.ts",
      planning: "src/planning.ts",
      "table-planning": "src/table-planning.ts",
      normalization: "src/normalization.ts",
    },
    format: ["esm", "cjs"],
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
