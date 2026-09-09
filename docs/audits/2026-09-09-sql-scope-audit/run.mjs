import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../../../", import.meta.url));
const require = createRequire(`${root}/package.json`);
const { createServer } = await import(require.resolve("vite"));
const server = await createServer({
  root,
  server: { middlewareMode: true, hmr: false, ws: false },
});
try {
  for (const name of ["scope", "missing-columns"]) {
    await server.ssrLoadModule(fileURLToPath(new URL(`./${name}.mjs`, import.meta.url)));
  }
} finally {
  await server.close();
}
