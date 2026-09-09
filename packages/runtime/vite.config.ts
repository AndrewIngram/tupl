import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
      executor: "src/runtime/executor.ts",
      session: "src/runtime/session/index.ts",
    },
    format: ["esm"],
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
