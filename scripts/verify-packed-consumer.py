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
PACKAGES = ["foundation", "provider-kit", "schema-model", "planner", "runtime", "schema", "provider-objection"]


def run(command, cwd):
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{' '.join(map(str, command))}\n{result.stdout}{result.stderr}")
    return result.stdout


def extract_package(archive, modules):
    with tarfile.open(archive) as source:
        manifest = json.load(source.extractfile("package/package.json"))
        target = modules / manifest["name"]
        target.mkdir()
        for member in source.getmembers():
            relative = Path(member.name).relative_to("package")
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
    const schema = createExecutableSchema(builder).unwrap();
    assert.deepEqual((await schema.query({sql: 'SELECT id FROM records WHERE id = 2', context: {}})).unwrap(), [{id: 2}]);
    assert.deepEqual((await schema.query({sql: "SELECT id, label FROM records WHERE label = 'BOB'", context: {}})).unwrap(), [{id: 2, label: 'BOB'}]);
  } finally {
    await db.destroy();
  }
}
"""


MIXED_CONSUMER = """
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import knex from 'knex';
import {createObjectionProvider} from '@tupl/provider-objection';
const require = createRequire(import.meta.url);
const esm = await import('@tupl/schema');
const cjs = require('@tupl/schema');
const esmModel = await import('@tupl/schema-model');
const cjsModel = require('@tupl/schema-model');
const db = knex({client: 'better-sqlite3', connection: {filename: ':memory:'}, useNullAsDefault: true});
try {
  await db.schema.createTable('records', table => {table.integer('id'); table.text('name');});
  await db('records').insert([{id: 1, name: 'alice'}, {id: 2, name: 'bob'}]);
  const provider = createObjectionProvider({knex: db, entities: {records: {table: 'records', shape: {id: 'integer', name: 'text'}}}});
  for (const [author, consumer, model] of [[cjs, esm, esmModel], [esm, cjs, cjsModel]]) {
    const builder = author.createSchemaBuilder();
    builder.table('records', provider.entities.records, {
      columns: ({col, derive}) => {
        const name = col.string('name', {nullable: false});
        const label = derive({name}, ({name}) => name.toUpperCase());
        return {id: col.integer('id'), label: col.string(label)};
      },
    });
    assert.equal(model.isSchemaBuilder(builder), true);
    for (const input of [builder, builder.build().unwrap()]) {
      const schema = consumer.createExecutableSchema(input).unwrap();
      assert.deepEqual((await schema.query({sql: "SELECT id,label FROM records WHERE label='BOB'", context: {}})).unwrap(), [{id: 2, label: 'BOB'}]);
    }
  }
} finally {
  await db.destroy();
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
        for name, installed in external.items():
            os.symlink(installed.resolve(strict=True), modules / name)
        (base / "package.json").write_text(json.dumps({"private": True, "type": "module"}))
        names = json.dumps(exports)
        esm = f"""
import assert from 'node:assert/strict';
import knex from 'knex';
import {{createSchemaBuilder, createExecutableSchema}} from '@tupl/schema';
import {{isSchemaBuilder}} from '@tupl/schema-model';
import {{createObjectionProvider}} from '@tupl/provider-objection';
for (const name of {names}) {{
  assert.ok(import.meta.resolve(name).endsWith('.mjs'), name);
  await import(name);
}}
{RUNTIME_CONSUMER}
await checkQuery();
"""
        cjs = f"""
const assert = require('node:assert/strict');
const knex = require('knex');
const {{createSchemaBuilder, createExecutableSchema}} = require('@tupl/schema');
const {{isSchemaBuilder}} = require('@tupl/schema-model');
const {{createObjectionProvider}} = require('@tupl/provider-objection');
for (const name of {names}) {{
  assert.ok(require.resolve(name).endsWith('.cjs'), name);
  require(name);
}}
{RUNTIME_CONSUMER}
checkQuery().catch(error => {{console.error(error); process.exitCode = 1;}});
"""
        for filename, text in [("consumer.mjs", esm), ("consumer.cjs", cjs)]:
            (base / filename).write_text(text)
            run([node, filename], base)
            print(f"PASS {filename}: {len(exports)} exports and native/derived SQLite queries")
        mixed = MIXED_CONSUMER + f"""
for (const name of {names}) {{
  if (name === '@tupl/provider-objection') continue;
  const imported = await import(name);
  const required = require(name);
  for (const key of Object.keys(required)) {{
    assert.equal(imported[key], required[key], `${{name}}.${{key}} must share identity`);
  }}
}}
"""
        (base / "mixed.mjs").write_text(mixed)
        run([node, "mixed.mjs"], base)
        print("PASS mixed.mjs: shared export identities, builders, normalized schemas, and derived SQLite queries in both directions")
        imports = "\n".join(f"import * as entry{i} from {json.dumps(name)};\nvoid entry{i};" for i, name in enumerate(exports))
        for extension in ["mts", "cts"]:
            filename = f"consumer.{extension}"
            (base / filename).write_text(imports + TYPED_CONSUMER)
            run([str(ROOT / "node_modules/.bin/tsgo"), "--ignoreConfig", "--noEmit", "--strict", "--module", "nodenext", "--target", "es2022", filename], base)
            print(f"PASS {filename}: NodeNext export resolution and derived inference")

        (base / "mixed.mts").write_text(TYPED_CONSUMER + """
import cjs = require('@tupl/schema');
import type {SchemaDerivedValue as ImportedValue} from '@tupl/schema-model/dsl' with {"resolution-mode": "import"};
import type {SchemaDerivedValue as RequiredValue} from '@tupl/schema-model/dsl' with {"resolution-mode": "require"};
cjs.createExecutableSchema(builder);
cjs.createExecutableSchema(builder.build().unwrap());
createExecutableSchema(cjs.createSchemaBuilder());
declare const requiredValue: RequiredValue<{label: string}>;
const importedValue: ImportedValue<{label: string}> = requiredValue;
const back: RequiredValue<{label: string}> = importedValue;
void back;
""")
        run([str(ROOT / "node_modules/.bin/tsgo"), "--ignoreConfig", "--noEmit", "--strict", "--module", "nodenext", "--target", "es2022", "mixed.mts"], base)
        print("PASS mixed.mts: mixed import/require declarations")


if __name__ == "__main__":
    main()
