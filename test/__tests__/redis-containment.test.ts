import { fc, test } from "@fast-check/vitest";
import { expect } from "vite-plus/test";
import { createIoredisProvider, type RedisLike } from "@tupl/provider-ioredis";
import { createExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder } from "@tupl/schema-model";
import type { RelNode } from "@tupl/foundation";

test.prop(
  {
    org: fc.integer({ min: 1, max: 2 }),
    requestedOrg: fc.integer({ min: 1, max: 3 }),
    hiddenValue: fc.integer(),
  },
  { numRuns: 100 },
)(
  "Redis scans and lookups always resolve keys with the execution context",
  async ({ org, requestedOrg, hiddenValue }) => {
    const requestedKeys: string[] = [];
    const hashes = new Map<string, Record<string, string>>();
    for (const tenant of [1, 2])
      for (const id of [1, 2]) {
        hashes.set(`${tenant}:${id}`, {
          id: String(id),
          org: String(tenant),
          value: String(tenant === org ? id : hiddenValue),
        });
      }
    const redis: RedisLike = {
      pipeline() {
        const keys: string[] = [];
        return {
          hgetall(key) {
            keys.push(key);
            requestedKeys.push(key);
            return this;
          },
          async exec() {
            return keys.map(
              (key) =>
                [null, hashes.get(key) ?? {}] satisfies [Error | null, Record<string, string>],
            );
          },
        };
      },
    };
    const provider = createIoredisProvider<{ org: number }>({
      name: "redis",
      redis,
      entities: {
        accounts: {
          entity: "accounts",
          lookupKey: "id",
          columns: ["id", "org", "value"],
          buildRedisKey: ({ key, context }) => `${context.org}:${String(key)}`,
          decodeRow: ({ hash }) => ({
            id: Number(hash.id),
            org: Number(hash.org),
            value: Number(hash.value),
          }),
        },
      },
    });
    const expected = [
      { id: 1, org },
      { id: 2, org },
    ];
    const request = { table: "accounts", key: "id", keys: [1, 2], select: ["id", "org"] };
    expect((await provider.lookupMany(request, { org })).unwrap()).toEqual(expected);
    const rel: RelNode = {
      id: "scan",
      kind: "scan",
      convention: "local",
      table: "accounts",
      select: ["id", "org"],
      where: [{ op: "in", column: "id", values: [1, 2] }],
      output: [{ name: "id" }, { name: "org" }],
    };
    const plan = (await provider.compile(rel, { org: 3 - org })).unwrap();
    expect((await provider.execute(plan, { org })).unwrap()).toEqual(expected);
    const builder = createSchemaBuilder<{ org: number }>();
    builder.table("accounts", provider.entities.accounts!, {
      columns: { id: { type: "integer" }, org: { type: "integer" }, value: { type: "integer" } },
    });
    const schema = createExecutableSchema(builder).unwrap();
    const result = await schema.query({
      sql: `SELECT id, org FROM accounts WHERE id IN (1,2) AND (org=${requestedOrg} OR NOT (org=${requestedOrg}))`,
      context: { org },
    });
    expect(result.unwrap()).toEqual(expected);
    expect(requestedKeys.every((key) => key.startsWith(`${org}:`))).toBe(true);
  },
);
