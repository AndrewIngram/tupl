import { dirname, relative, resolve } from "node:path";
import * as ts from "typescript";

export interface SourceExport {
  moduleSpecifier: string | null;
  names: "*" | string[];
  typeOnly: boolean;
}

export interface WorkspaceReferenceContext {
  importerFile: string;
  packageNamesByDirectory: ReadonlyMap<string, string>;
  repoRoot: string;
  specifier: string;
}

function parseSource(contents: string, fileName: string) {
  const scriptKind = fileName.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS;
  return ts.createSourceFile(fileName, contents, ts.ScriptTarget.Latest, true, scriptKind);
}

function readModuleSpecifier(node: ts.Expression | undefined) {
  return node && ts.isStringLiteralLike(node) ? node.text : undefined;
}

export function getModuleSpecifiers(contents: string, fileName = "source.ts") {
  const sourceFile = parseSource(contents, fileName);
  const specifiers = new Set<string>();

  function visit(node: ts.Node) {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      const specifier = readModuleSpecifier(node.moduleSpecifier);
      if (specifier) {
        specifiers.add(specifier);
      }
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference)
    ) {
      const specifier = readModuleSpecifier(node.moduleReference.expression);
      if (specifier) {
        specifiers.add(specifier);
      }
    } else if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      const specifier = readModuleSpecifier(node.arguments[0]);
      if (specifier) {
        specifiers.add(specifier);
      }
    } else if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument)) {
      const specifier = readModuleSpecifier(node.argument.literal);
      if (specifier) {
        specifiers.add(specifier);
      }
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return [...specifiers];
}

export function getSourceExports(contents: string, fileName = "source.ts") {
  const sourceFile = parseSource(contents, fileName);
  const exports: SourceExport[] = [];

  for (const statement of sourceFile.statements) {
    if (!ts.isExportDeclaration(statement)) {
      continue;
    }

    const moduleSpecifier = readModuleSpecifier(statement.moduleSpecifier) ?? null;
    if (!statement.exportClause) {
      exports.push({ moduleSpecifier, names: "*", typeOnly: statement.isTypeOnly });
      continue;
    }

    if (ts.isNamespaceExport(statement.exportClause)) {
      exports.push({
        moduleSpecifier,
        names: [statement.exportClause.name.text],
        typeOnly: statement.isTypeOnly,
      });
      continue;
    }

    exports.push({
      moduleSpecifier,
      names: statement.exportClause.elements.map((element) => element.name.text),
      typeOnly:
        statement.isTypeOnly ||
        statement.exportClause.elements.every((element) => element.isTypeOnly),
    });
  }

  return exports;
}

export function isForwardingModule(contents: string, fileName = "source.ts") {
  const statements = parseSource(contents, fileName).statements;
  return (
    statements.length > 0 &&
    statements.every(
      (statement) =>
        ts.isExportDeclaration(statement) &&
        readModuleSpecifier(statement.moduleSpecifier) !== undefined,
    )
  );
}

export function referencedWorkspacePackage({
  importerFile,
  packageNamesByDirectory,
  repoRoot,
  specifier,
}: WorkspaceReferenceContext) {
  if (specifier.startsWith("@tupl/")) {
    const [scope, name] = specifier.split("/");
    return name ? `${scope}/${name}` : specifier;
  }

  if (!specifier.startsWith(".")) {
    return undefined;
  }

  const targetFromRoot = relative(repoRoot, resolve(dirname(importerFile), specifier)).replaceAll(
    "\\",
    "/",
  );
  const [packagesDirectory, packageDirectory] = targetFromRoot.split("/");
  if (packagesDirectory !== "packages" || !packageDirectory) {
    return undefined;
  }

  return packageNamesByDirectory.get(packageDirectory);
}
