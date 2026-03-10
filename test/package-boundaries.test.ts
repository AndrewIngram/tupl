import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const REPO_ROOT = process.cwd();
const IMPORT_PATTERN = /(?:from\s+["']([^"']+)["']|import\s*\(\s*["']([^"']+)["']\s*\))/g;

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
    const path = join(root, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      out.push(...walkFiles(path));
      continue;
    }
    out.push(path);
  }

  return out;
}

function getWorkspaceImports(contents: string): string[] {
  const imports = new Set<string>();
  for (const match of contents.matchAll(IMPORT_PATTERN)) {
    const specifier = match[1] ?? match[2];
    if (!specifier?.startsWith("@tupl/")) {
      continue;
    }
    imports.add(rootPackageOf(specifier));
  }
  return [...imports];
}

function rootPackageOf(specifier: string): string {
  const [scope, name] = specifier.split("/");
  return `${scope}/${name}`;
}

describe("package boundaries", () => {
  it("keeps the semantic package graph acyclic and downward-only", () => {
    for (const [dir, allowedImports] of Object.entries(LAYER_RULES)) {
      for (const file of walkFiles(join(REPO_ROOT, dir))) {
        if (!file.endsWith(".ts") && !file.endsWith(".tsx")) {
          continue;
        }

        const imports = getWorkspaceImports(readFileSync(file, "utf8"));
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
});
