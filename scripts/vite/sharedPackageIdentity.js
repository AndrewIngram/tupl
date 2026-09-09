import { readFile, writeFile } from "node:fs/promises";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** Keep runtime and declaration identities shared between import and require. */
export function sharedPackageIdentity(manifestUrl) {
  const directory = dirname(fileURLToPath(manifestUrl));
  return async () => {
    const manifest = JSON.parse(await readFile(manifestUrl, "utf8"));
    for (const [name, entry] of Object.entries(manifest.exports)) {
      const implementation = entry.require?.default;
      const runtimeFacade = entry.import?.default;
      const declarationFacade = entry.import?.types;
      if (
        !implementation?.endsWith(".cjs") ||
        !runtimeFacade?.endsWith(".mjs") ||
        !declarationFacade?.endsWith(".d.mts")
      ) {
        throw new Error(
          `${manifest.name}${name} needs CommonJS implementation and ESM facade exports.`,
        );
      }
      for (const facade of [runtimeFacade, declarationFacade]) {
        const filename = resolve(directory, facade);
        const target = relative(dirname(filename), resolve(directory, implementation)).replaceAll(
          "\\",
          "/",
        );
        await writeFile(
          filename,
          `// Generated facade: both module formats share one implementation.\nexport * from ${JSON.stringify(`./${target}`)};\n`,
        );
      }
    }
  };
}
