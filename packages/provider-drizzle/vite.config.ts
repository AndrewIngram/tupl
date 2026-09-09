import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: ["src/index.ts"],
    format: ["esm"],
    sourcemap: true,
    clean: true,
    dts: true,
    tsconfig: "tsconfig.build.json",
    unbundle: true,
  },
});
