import { defineConfig } from "vite-plus";
import { sharedPackageIdentity } from "../../scripts/vite/sharedPackageIdentity.js";

export default defineConfig({
  pack: {
    deps: { resolveDepSubpath: true },
    entry: {
      index: "src/index.ts",
      executor: "src/runtime/executor.ts",
      session: "src/runtime/session/index.ts",
    },
    format: ["cjs"],
    onSuccess: sharedPackageIdentity(new URL("./package.json", import.meta.url)),
    sourcemap: true,
    clean: true,
    dts: true,
  },
});
