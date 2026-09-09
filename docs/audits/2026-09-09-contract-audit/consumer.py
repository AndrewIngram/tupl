"""Pack local artifacts and test imports without workspace package resolution."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

root = Path(__file__).resolve().parents[3]
output = Path(__file__).resolve().parent
base = Path(tempfile.mkdtemp(prefix="tupl-contract-consumer-", dir="/private/tmp"))
archives = base / "archives"
archives.mkdir()
modules = base / "node_modules"
(modules / "@tupl").mkdir(parents=True)
packages = ["foundation", "provider-kit", "schema-model", "planner", "runtime", "schema", "provider-objection"]
for package in packages:
    subprocess.run(["pnpm", "pack", "--pack-destination", str(archives)], cwd=root / "packages" / package, check=True, capture_output=True)
for archive in archives.glob("*.tgz"):
    with tarfile.open(archive) as source:
        manifest = json.load(source.extractfile("package/package.json"))
        target = modules / manifest["name"]
        target.mkdir()
        for member in source.getmembers():
            relative = Path(member.name).relative_to("package")
            destination = target / relative
            if ".." in relative.parts or member.issym() or member.islnk():
                raise ValueError("Unexpected archive member")
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(source.extractfile(member).read())
# Only the external dependency is shared. Every tupl package is an extracted tarball.
os.symlink((root / "node_modules/better-result").resolve(), modules / "better-result")
(base / "package.json").write_text(json.dumps({"private": True, "type": "module"}))
(base / "consumer.ts").write_text("import {createSchemaBuilder,createExecutableSchema} from '@tupl/schema';\ncreateExecutableSchema(createSchemaBuilder());\n")
node = shutil.which("node")
checks = [
    ("ESM import", [node, "--input-type=module", "-e", "await import('@tupl/schema')"]),
    ("CommonJS require", [node, "-e", "require('@tupl/schema')"]),
    ("NodeNext consumer types", [str(root / "node_modules/.bin/tsgo"), "--ignoreConfig", "--noEmit", "--module", "nodenext", "--target", "es2022", "--skipLibCheck", "consumer.ts"]),
]
results = []
for name, command in checks:
    result = subprocess.run(command, cwd=base, capture_output=True, text=True)
    details = result.stdout + result.stderr
    results.append({"check": name, "exitCode": result.returncode, "output": details[:6000], "fullOutputCharacters": len(details)})
record = {"consumerDirectory": str(base), "node": subprocess.check_output([node, "--version"], text=True).strip(), "packages": packages, "checks": results}
(output / "consumer-results.json").write_text(json.dumps(record, indent=2) + "\n")
print(json.dumps({"consumerDirectory": str(base), "checks": [{"check": r["check"], "exitCode": r["exitCode"]} for r in results]}))
