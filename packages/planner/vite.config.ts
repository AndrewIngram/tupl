import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
    },
    format: ["esm", "cjs"],
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
