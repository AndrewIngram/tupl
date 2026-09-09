"""Verify real package exports without workspace aliases. Run after `vp run -r build`.

Only external dependencies reuse installed copies; every @tupl package is loaded
from its actual pnpm tarball. No registry access is needed.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = sorted(
    path.parent.name for path in (ROOT / "packages").glob("*/package.json")
    if not json.loads(path.read_text()).get("private", False)
)


def run(command, cwd):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{' '.join(map(str, command))}\n{result.stdout}{result.stderr}")
    return result.stdout


def extract_package(archive, modules):
    with tarfile.open(archive) as source:
        manifest = json.load(source.extractfile("package/package.json"))
        assert manifest["type"] == "module", manifest["name"]
        assert manifest["main"].endswith(".mjs"), manifest["name"]
        for entry in manifest["exports"].values():
            assert set(entry) == {"source", "types", "import"}, entry
            assert entry["import"].endswith(".mjs"), entry
            assert entry["types"].endswith(".d.mts"), entry
        target = modules / manifest["name"]
        target.mkdir()
        for member in source.getmembers():
            relative = Path(member.name).relative_to("package")
            assert not member.name.endswith((".cjs", ".cjs.map", ".cts", ".cts.map")), member.name
            if ".." in relative.parts or member.issym() or member.islnk():
                raise ValueError(f"Unexpected archive member: {member.name}")
            destination = target / relative
            if member.isdir():
                destination.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(source.extractfile(member).read())
        return [manifest["name"] + ("" if key == "." else key[1:]) for key in manifest["exports"]]


TYPED_CONSUMER = """
import {createSchemaBuilder, createExecutableSchema} from '@tupl/schema';
import {createDataEntityHandle} from '@tupl/provider-kit';
const entity = createDataEntityHandle<'id' | 'name', {id: number; name: string}>({entity: 'records', provider: 'db'});
const builder = createSchemaBuilder();
const table = builder.table('records', entity, {
  columns: ({col, derive}) => {
    const name = col.string('name', {nullable: false});
    const object = col.json(derive({name}, ({name}) => ({label: name.toUpperCase()})), {nullable: false});
    const label = derive({object}, ({object}) => {
      const text: string = object.label;
      // @ts-expect-error Packed declarations preserve the derived JSON object shape.
      object.missing;
      return text;
    });
    // @ts-expect-error Packed declarations reject asynchronous callbacks.
    derive({}, async () => 'no');
    return {object, label: col.string(label, {nullable: false})};
  },
});
builder.view('labels', ({scan}) => scan(table), {
  columns: ({col, derive}) => ({label: col.string(derive({value: col.json(table, 'object', {nullable: false})}, ({value}) => value.label))}),
});
createExecutableSchema(builder);
"""

RUNTIME_CONSUMER = """
async function checkQuery() {
  const db = knex({client: 'better-sqlite3', connection: {filename: ':memory:'}, useNullAsDefault: true});
  try {
    await db.schema.createTable('records', table => {table.integer('id'); table.text('name');});
    await db('records').insert([{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]);
    const provider = createObjectionProvider({knex: db, entities: {records: {table: 'records', shape: {id: 'integer', name: 'text'}}}});
    const builder = createSchemaBuilder();
    const table = builder.table('records', provider.entities.records, {
      columns: ({col, derive}) => {
        const name = col.string('name', {nullable: false});
        return {id: col.integer('id'), label: col.string(derive({name}, ({name}) => name.toUpperCase()))};
      },
    });
    // Cross-entry state (builder ownership and operation registries) must survive packaging.
    assert.equal(isSchemaBuilder(builder), true);
    for (const input of [builder, builder.build().unwrap()]) {
      const schema = createExecutableSchema(input).unwrap();
      assert.deepEqual((await schema.query({sql: 'SELECT id FROM records WHERE id = 2', context: {}})).unwrap(), [{id: 2}]);
      assert.deepEqual((await schema.query({sql: "SELECT id, label FROM records WHERE label = 'BOB'", context: {}})).unwrap(), [{id: 2, label: 'BOB'}]);
    }
  } finally {
    await db.destroy();
  }
}
"""


def main():
    node = shutil.which("node")
    if not node:
        raise RuntimeError("Node.js is required")
    with tempfile.TemporaryDirectory(prefix="tupl-packed-consumer-") as directory:
        base = Path(directory)
        archives = base / "archives"
        archives.mkdir()
        modules = base / "node_modules"
        (modules / "@tupl").mkdir(parents=True)
        for package in PACKAGES:
            run(["pnpm", "pack", "--pack-destination", str(archives)], ROOT / "packages" / package)
        exports = []
        for archive in sorted(archives.glob("*.tgz")):
            exports.extend(extract_package(archive, modules))
        external = {
            "better-result": ROOT / "node_modules/better-result",
            "knex": ROOT / "packages/provider-objection/node_modules/knex",
            "better-sqlite3": ROOT / "node_modules/better-sqlite3",
        }
        for package in PACKAGES:
            directory = ROOT / "packages" / package
            manifest = json.loads((directory / "package.json").read_text())
            for name in manifest.get("dependencies", {}) | manifest.get("peerDependencies", {}):
                if not name.startswith("@tupl/"):
                    external[name] = directory / "node_modules" / name
        for name, installed in external.items():
            (modules / name).parent.mkdir(parents=True, exist_ok=True)
            os.symlink(installed.resolve(strict=True), modules / name)
        (base / "package.json").write_text(json.dumps({"private": True, "type": "module"}))
        names = json.dumps(exports)
        esm = f"""
import assert from 'node:assert/strict';
import knex from 'knex';
import {{createSchemaBuilder, createExecutableSchema}} from '@tupl/schema';
import {{isSchemaBuilder, createSchemaBuilder as modelBuilder}} from '@tupl/schema-model';
assert.equal(createSchemaBuilder, modelBuilder);
import {{createObjectionProvider}} from '@tupl/provider-objection';
for (const name of {names}) {{
  assert.ok(import.meta.resolve(name).endsWith('.mjs'), name);
  await import(name);
}}
{RUNTIME_CONSUMER}
await checkQuery();
"""
        (base / "consumer.mjs").write_text(esm)
        run([node, "consumer.mjs"], base)
        print(f"PASS consumer.mjs: {len(exports)} ESM exports and native/derived SQLite queries")
        imports = "\n".join(f"import * as entry{i} from {json.dumps(name)};\nvoid entry{i};" for i, name in enumerate(exports))
        # Keep strict declaration checks for the core and Objection integration.
        # Drizzle's declarations pull in optional drivers and upstream type errors.
        strict_exports = [name for name in exports if name not in {
            "@tupl/provider-drizzle", "@tupl/provider-kysely", "@tupl/provider-ioredis"
        }]
        strict_imports = "\n".join(f"import * as entry{i} from {json.dumps(name)};\nvoid entry{i};" for i, name in enumerate(strict_exports))
        (base / "consumer.mts").write_text(strict_imports + TYPED_CONSUMER)
        run([str(ROOT / "node_modules/.bin/tsgo"), "--ignoreConfig", "--noEmit", "--strict", "--module", "nodenext", "--target", "es2022", "consumer.mts"], base)
        print("PASS consumer.mts: NodeNext ESM export resolution and derived inference")
        (base / "providers.mts").write_text(imports)
        run([str(ROOT / "node_modules/.bin/tsgo"), "--ignoreConfig", "--noEmit", "--strict", "--skipLibCheck", "--module", "nodenext", "--target", "es2022", "providers.mts"], base)
        print("PASS providers.mts: all public ESM imports resolve (dependency declaration checking skipped)")


if __name__ == "__main__":
    main()
