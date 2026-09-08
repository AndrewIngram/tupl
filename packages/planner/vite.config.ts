import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: ["src/index.ts"],
    format: ["esm", "cjs"],
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
