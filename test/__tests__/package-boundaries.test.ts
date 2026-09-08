import {
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vite-plus/test";

import { getPackageName, getPublicSourceExports } from "../architecture/package-exports";
import {
  getModuleSpecifiers,
  getSourceExports,
  isForwardingModule,
  referencedWorkspacePackage,
} from "../architecture/typescript-modules";

const REPO_ROOT = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");

const LAYER_RULES = {
  "packages/foundation/src": new Set<string>(["@tupl/foundation"]),
  "packages/provider-kit/src": new Set<string>(["@tupl/foundation", "@tupl/provider-kit"]),
  "packages/schema-model/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
  ]),
  "packages/planner/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
  ]),
  "packages/runtime/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
    "@tupl/runtime",
  ]),
  "packages/schema/src": new Set<string>(["@tupl/schema-model", "@tupl/runtime", "@tupl/schema"]),
  "packages/test-support/src": new Set<string>([
    "@tupl/foundation",
    "@tupl/provider-kit",
    "@tupl/schema-model",
    "@tupl/planner",
    "@tupl/runtime",
    "@tupl/test-support",
  ]),
} as const;

const DISALLOWED_PUBLIC_REFS = [
  "README.md",
  "docs",
  "examples",
  "packages/provider-drizzle",
  "packages/provider-ioredis",
  "packages/provider-kysely",
  "packages/provider-objection",
  "packages/schema/README.md",
] as const;

function walkFiles(root: string): string[] {
  const entries = readdirSync(root);
  const out: string[] = [];

  for (const entry of entries) {
    if (
      entry === "node_modules" ||
      entry === "dist" ||
      entry === ".git" ||
      entry === "coverage" ||
      entry === ".turbo"
    ) {
      continue;
    }

    const path = join(root, entry);
    const stat = lstatSync(path);
    if (stat.isSymbolicLink()) {
      continue;
    }
    if (stat.isDirectory()) {
      out.push(...walkFiles(path));
      continue;
    }
    out.push(path);
  }

  return out;
}

function discoverPackageSources(packagesRoot: string) {
  const sources: Array<{ packageName: string; sourceDir: string }> = [];

  for (const entry of readdirSync(packagesRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const sourceDir = join(packagesRoot, entry.name, "src");
    try {
      const sourceStat = lstatSync(sourceDir);
      if (!sourceStat.isSymbolicLink() && sourceStat.isDirectory()) {
        sources.push({ packageName: entry.name, sourceDir });
      }
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") {
        throw error;
      }
    }
  }

  return sources;
}

function readPackageJson(packageDirectory: string): unknown {
  return JSON.parse(readFileSync(join(packageDirectory, "package.json"), "utf8"));
}

function getPackageNamesByDirectory(packagesRoot: string) {
  return new Map(
    discoverPackageSources(packagesRoot).map(({ packageName: packageDirectory }) => [
      packageDirectory,
      getPackageName(readPackageJson(join(packagesRoot, packageDirectory))),
    ]),
  );
}

function getWorkspaceImports(
  contents: string,
  importerFile: string,
  packageNamesByDirectory: ReadonlyMap<string, string>,
) {
  const imports = new Set<string>();
  for (const specifier of getModuleSpecifiers(contents, importerFile)) {
    const packageName = referencedWorkspacePackage({
      importerFile,
      packageNamesByDirectory,
      repoRoot: REPO_ROOT,
      specifier,
    });
    if (packageName) {
      imports.add(packageName);
    }
  }
  return [...imports];
}

function getPublicSourceModules(packagesRoot: string) {
  const modules = new Map<string, string>();

  for (const { packageName: packageDirectory } of discoverPackageSources(packagesRoot)) {
    const packageRoot = join(packagesRoot, packageDirectory);
    for (const entry of getPublicSourceExports(readPackageJson(packageRoot))) {
      modules.set(
        resolve(packageRoot, entry.target),
        `${entry.packageName}${entry.subpath.slice(1)}`,
      );
    }
  }

  return modules;
}

describe("package boundaries", () => {
  it("discovers package sources among ordinary filesystem entries", () => {
    const packagesRoot = mkdtempSync(join(tmpdir(), "tupl-package-discovery-"));
    try {
      mkdirSync(join(packagesRoot, "runtime", "src"), { recursive: true });
      mkdirSync(join(packagesRoot, "notes"));
      writeFileSync(join(packagesRoot, ".DS_Store"), "ordinary file");

      expect(discoverPackageSources(packagesRoot)).toEqual([
        {
          packageName: "runtime",
          sourceDir: join(packagesRoot, "runtime", "src"),
        },
      ]);
    } finally {
      rmSync(packagesRoot, { recursive: true, force: true });
    }
  });

  it("keeps the schema facade explicitly curated", () => {
    const schemaIndexPath = join(REPO_ROOT, "packages/schema/src/index.ts");
    const exports = getSourceExports(readFileSync(schemaIndexPath, "utf8"), schemaIndexPath);
    const exportedNames = new Set(
      exports.flatMap((entry) => (entry.names === "*" ? [] : entry.names)),
    );

    expect(
      exports.filter(
        (entry) =>
          entry.names === "*" &&
          (entry.moduleSpecifier === "@tupl/schema-model" ||
            entry.moduleSpecifier === "@tupl/runtime"),
      ),
    ).toEqual([]);
    expect(exportedNames.has("QueryExecutionPlan")).toBe(false);
    expect(exportedNames.has("QueryStepEvent")).toBe(false);
    expect(exportedNames.has("validateTableConstraintRows")).toBe(false);
  });

  it("keeps the semantic package graph acyclic and downward-only", () => {
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));

    for (const [dir, allowedImports] of Object.entries(LAYER_RULES)) {
      for (const file of walkFiles(join(REPO_ROOT, dir))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }
        if (file.includes("/__tests__/")) {
          continue;
        }

        const imports = getWorkspaceImports(
          readFileSync(file, "utf8"),
          file,
          packageNamesByDirectory,
        );
        const disallowed = imports.filter((pkg) => !allowedImports.has(pkg));
        expect(
          disallowed,
          `${relative(REPO_ROOT, file)} imported disallowed packages: ${disallowed.join(", ")}`,
        ).toEqual([]);
      }
    }
  });

  it("keeps legacy package names out of docs, examples, and provider packages", () => {
    const offenders: string[] = [];

    for (const target of DISALLOWED_PUBLIC_REFS) {
      const fullPath = join(REPO_ROOT, target);
      const files = statSync(fullPath).isDirectory() ? walkFiles(fullPath) : [fullPath];
      for (const file of files) {
        if (file.includes("/dist/")) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (contents.includes("@tupl/core") || contents.includes("@tupl-internal/")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps first-party SQL-like providers on the canonical sql relational helper", () => {
    const providerRoots = [
      "packages/provider-drizzle/src/index.ts",
      "packages/provider-kysely/src/index.ts",
      "packages/provider-objection/src/index.ts",
    ] as const;

    for (const file of providerRoots) {
      const contents = readFileSync(join(REPO_ROOT, file), "utf8");
      expect(contents, `${file} should use createSqlRelationalProviderAdapter`).toContain(
        "createSqlRelationalProviderAdapter",
      );
      expect(
        contents,
        `${file} should not import createRelationalProviderAdapter directly`,
      ).not.toMatch(/createRelationalProviderAdapter/);
    }
  });

  it("keeps package exports pointing at real source modules", () => {
    for (const [target, entrypoint] of getPublicSourceModules(join(REPO_ROOT, "packages"))) {
      expect(statSync(target).isFile(), entrypoint).toBe(true);
    }
  });

  it("avoids wrapper-only files outside package roots and public subpath roots", () => {
    const offenders: string[] = [];
    const publicSourceModules = getPublicSourceModules(join(REPO_ROOT, "packages"));

    for (const { sourceDir } of discoverPackageSources(join(REPO_ROOT, "packages"))) {
      for (const file of walkFiles(sourceDir)) {
        if (!file.endsWith(".ts") || file.endsWith(".d.ts")) {
          continue;
        }

        const relFile = relative(REPO_ROOT, file);
        if (publicSourceModules.has(file)) {
          continue;
        }
        if (isForwardingModule(readFileSync(file, "utf8"), file)) {
          offenders.push(relFile);
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps package-local test support on the owning layer or below", () => {
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));

    for (const [dir, allowedImports] of Object.entries(LAYER_RULES)) {
      const supportRoot = join(REPO_ROOT, dir);
      for (const file of walkFiles(supportRoot)) {
        if (!file.includes("/__tests__/support/") || !file.endsWith(".ts")) {
          continue;
        }

        const imports = getWorkspaceImports(
          readFileSync(file, "utf8"),
          file,
          packageNamesByDirectory,
        );
        const disallowed = imports.filter((pkg) => !allowedImports.has(pkg));
        expect(
          disallowed,
          `${relative(REPO_ROOT, file)} imported disallowed packages: ${disallowed.join(", ")}`,
        ).toEqual([]);
      }
    }
  });

  it("keeps low-level packages and tests off the schema facade", () => {
    const offenders: string[] = [];
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));

    for (const file of walkFiles(join(REPO_ROOT, "packages"))) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }

      const relFile = relative(REPO_ROOT, file);
      if (relFile.startsWith("packages/schema/") || relFile.startsWith("packages/test-support/")) {
        continue;
      }

      const imports = getWorkspaceImports(
        readFileSync(file, "utf8"),
        file,
        packageNamesByDirectory,
      );
      if (imports.includes("@tupl/schema")) {
        offenders.push(relFile);
      }
    }

    for (const file of walkFiles(join(REPO_ROOT, "test"))) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }

      const relFile = relative(REPO_ROOT, file);
      if (
        relFile === "test/__tests__/public-package-imports.test.ts" ||
        relFile === "test/__tests__/package-boundaries.test.ts"
      ) {
        continue;
      }

      const imports = getWorkspaceImports(
        readFileSync(file, "utf8"),
        file,
        packageNamesByDirectory,
      );
      if (imports.includes("@tupl/schema")) {
        offenders.push(relFile);
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps sql-node detection owned by foundation", () => {
    const offenders: string[] = [];

    for (const root of ["packages", "examples", "test"] as const) {
      const rootDir = join(REPO_ROOT, root);
      for (const file of walkFiles(rootDir)) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }
        const relFile = relative(REPO_ROOT, file);
        if (
          relFile.startsWith("packages/foundation/") ||
          relFile === "test/__tests__/package-boundaries.test.ts"
        ) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (/export function hasSqlNode|function hasSqlNode/.test(contents)) {
          offenders.push(relFile);
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps long relative helper traversal out of the repo", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(REPO_ROOT)) {
      if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
        continue;
      }
      if (relative(REPO_ROOT, file) === "test/__tests__/package-boundaries.test.ts") {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (contents.includes("/test/support/") || contents.includes("/__tests__/support/")) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps private test-support imports out of product source", () => {
    const offenders: string[] = [];
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));

    for (const { packageName, sourceDir } of discoverPackageSources(join(REPO_ROOT, "packages"))) {
      if (packageName === "test-support") {
        continue;
      }

      for (const file of walkFiles(sourceDir)) {
        if (!file.endsWith(".ts") || file.includes("/__tests__/")) {
          continue;
        }

        const imports = getWorkspaceImports(
          readFileSync(file, "utf8"),
          file,
          packageNamesByDirectory,
        );
        if (imports.includes("@tupl/test-support")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps foundation free of testing surfaces", () => {
    const offenders: string[] = [];
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));

    for (const file of walkFiles(join(REPO_ROOT, "packages/foundation/src"))) {
      if (!file.endsWith(".ts")) {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      const imports = getWorkspaceImports(contents, file, packageNamesByDirectory);
      const moduleSpecifiers = getModuleSpecifiers(contents, file);
      if (
        imports.includes("@tupl/test-support") ||
        moduleSpecifiers.includes("@tupl/provider-kit/testing")
      ) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps first-party adapter conformance on the public provider-kit/testing surface", () => {
    const contents = readFileSync(
      join(REPO_ROOT, "packages/test-support/src/__tests__/providers/conformance.test.ts"),
      "utf8",
    );
    expect(contents).toContain("@tupl/provider-kit/testing");
  });

  it("keeps first-party providers on the provider-kit adapter facade", () => {
    const offenders: string[] = [];
    const packageNamesByDirectory = getPackageNamesByDirectory(join(REPO_ROOT, "packages"));
    const providerRoots = [
      "packages/provider-drizzle/src",
      "packages/provider-ioredis/src",
      "packages/provider-kysely/src",
      "packages/provider-objection/src",
    ];

    for (const root of providerRoots) {
      for (const file of walkFiles(join(REPO_ROOT, root))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }

        const imports = getWorkspaceImports(
          readFileSync(file, "utf8"),
          file,
          packageNamesByDirectory,
        );
        if (imports.includes("@tupl/schema-model")) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps first-party relational providers on the relational helper instead of manual wiring", () => {
    const offenders: string[] = [];
    const relationalProviderRoots = [
      "packages/provider-drizzle/src",
      "packages/provider-kysely/src",
      "packages/provider-objection/src",
    ];
    const disallowedPrimitives = [
      "bindProviderEntities",
      "createDataEntityHandle",
      "inferRouteFamilyForRel",
      "normalizeDataEntityShape",
    ];

    for (const root of relationalProviderRoots) {
      for (const file of walkFiles(join(REPO_ROOT, root))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }

        const contents = readFileSync(file, "utf8");
        if (!contents.includes("@tupl/provider-kit")) {
          continue;
        }

        if (disallowedPrimitives.some((name) => contents.includes(name))) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps provider internal modules importing their owning families, not the package root", () => {
    const offenders: string[] = [];
    const providerRoots = [
      "packages/provider-kysely/src",
      "packages/provider-objection/src",
      "packages/provider-drizzle/src",
    ];

    for (const root of providerRoots) {
      for (const file of walkFiles(join(REPO_ROOT, root))) {
        if (!file.endsWith(".ts") || file.includes("/__tests__/") || file.endsWith("/index.ts")) {
          continue;
        }

        const packageRoot = join(REPO_ROOT, root);
        const importsPackageRoot = getModuleSpecifiers(readFileSync(file, "utf8"), file).some(
          (specifier) => {
            if (!specifier.startsWith(".")) {
              return false;
            }

            const resolvedImport = resolve(dirname(file), specifier);
            return (
              resolvedImport === packageRoot ||
              resolvedImport === join(packageRoot, "index") ||
              resolvedImport === join(packageRoot, "index.ts")
            );
          },
        );
        if (importsPackageRoot) {
          offenders.push(relative(REPO_ROOT, file));
        }
      }
    }

    expect(offenders).toEqual([]);
  });

  it("keeps runtime free of schema-view lowering logic", () => {
    const offenders: string[] = [];

    for (const file of walkFiles(join(REPO_ROOT, "packages/runtime/src"))) {
      if (!file.endsWith(".ts")) {
        continue;
      }

      const contents = readFileSync(file, "utf8");
      if (
        contents.includes("SchemaViewRelNode") ||
        contents.includes("compileViewRelToExecutableResult") ||
        contents.includes("compileSchemaViewRelNodeResult") ||
        contents.includes("rewriteViewBindingExprForExecution")
      ) {
        offenders.push(relative(REPO_ROOT, file));
      }
    }

    expect(offenders).toEqual([]);
  });
});
