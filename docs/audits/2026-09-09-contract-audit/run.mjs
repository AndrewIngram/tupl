import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../../../", import.meta.url));
process.chdir(root);
const require = createRequire(`${root}/package.json`);
const { createServer } = await import(require.resolve("vite"));
const server = await createServer({
  root,
  resolve: {
    alias: { "@tupl/runtime/session": `${root}/packages/runtime/src/runtime/session/index.ts` },
  },
  server: { middlewareMode: true, hmr: false, ws: false },
});
try {
  for (const file of ["probe.mjs", "lifecycle.mjs"]) {
    await server.ssrLoadModule(`${root}/docs/audits/2026-09-09-contract-audit/${file}`);
  }
} finally {
  await server.close();
}
