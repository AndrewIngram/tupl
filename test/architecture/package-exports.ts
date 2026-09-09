export interface PublicSourceExport {
  packageName: string;
  subpath: string;
  target: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function collectSourceTargets(value: unknown): string[] {
  if (typeof value === "string") {
    return value.startsWith("./src/") ? [value] : [];
  }

  if (Array.isArray(value)) {
    return value.flatMap(collectSourceTargets);
  }

  if (!isRecord(value)) {
    return [];
  }

  if ("source" in value) {
    return collectSourceTargets(value.source);
  }

  return Object.values(value).flatMap(collectSourceTargets);
}

export function getPackageName(packageJson: unknown) {
  if (!isRecord(packageJson) || typeof packageJson.name !== "string") {
    throw new Error("Package manifest must declare a package name.");
  }

  return packageJson.name;
}

export function getPublicSourceExports(packageJson: unknown): PublicSourceExport[] {
  const packageName = getPackageName(packageJson);
  if (!isRecord(packageJson)) {
    return [];
  }

  const packageExports = packageJson.exports;
  if (packageExports === undefined) {
    return [];
  }

  const entries: Array<[string, unknown]> =
    isRecord(packageExports) && Object.keys(packageExports).some((key) => key.startsWith("."))
      ? Object.entries(packageExports)
      : [[".", packageExports]];

  return entries.flatMap(([subpath, value]) => {
    const targets = collectSourceTargets(value);
    if (targets.length === 0) {
      throw new Error(`${packageName}${subpath.slice(1)} must declare a source export target.`);
    }

    return targets.map((target) => ({
      packageName,
      subpath,
      target,
    }));
  });
}
