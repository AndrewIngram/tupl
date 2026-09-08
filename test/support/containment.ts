import Database from "better-sqlite3";
import { PGlite } from "@electric-sql/pglite";
import { drizzle as drizzleSqlite } from "drizzle-orm/better-sqlite3";
import { drizzle as drizzlePg } from "drizzle-orm/pglite";
import { and, eq, gte } from "drizzle-orm";
import { sqliteTable, integer, text } from "drizzle-orm/sqlite-core";
import { pgTable, integer as pgInteger, text as pgText } from "drizzle-orm/pg-core";
import { Kysely, SqliteDialect } from "kysely";
import { KyselyPGlite } from "kysely-pglite";
import knexFactory from "knex";
import ClientPgLite from "knex-pglite";
import { createDrizzleProvider } from "@tupl/provider-drizzle";
import { createKyselyProvider, type KyselyProviderEntityConfig } from "@tupl/provider-kysely";
import { createObjectionProvider } from "@tupl/provider-objection";
import { createExecutableSchema } from "@tupl/runtime";
import { createSchemaBuilder } from "@tupl/schema-model";

export type ContainmentProvider = "drizzle" | "kysely" | "objection";
export type ContainmentDialect = "sqlite" | "postgres";
type Context = { org: number; deny?: boolean };
const sources = ["accounts", "details"] as const;
const visibleRows = {
  accounts: "(1,1,'one',10),(2,1,'two',20),(5,1,NULL,NULL)",
  details: "(1,1,'oneD',100),(4,1,'fourD',400),(6,1,'nullD',NULL)",
};
const physical = (table: string) => `${table}_raw`;
const physicalSql = (sql: string) => sql.replace(/\b(accounts|details)\b/g, physical);
const tableDdl = sources.map(
  (table) =>
    `create table ${table}(id integer, org integer, label text, value integer, secret text)`,
);

export async function createContainmentFixture(
  name: ContainmentProvider,
  local: boolean,
  dialect: ContainmentDialect,
) {
  const sqlite = dialect === "sqlite" ? new Database(":memory:") : undefined;
  const pg = dialect === "postgres" ? new PGlite() : undefined;
  const statements: string[] = [];
  const execute = async (sql: string) => {
    if (sqlite) {
      sqlite.exec(sql);
      return;
    }
    if (pg) {
      await pg.exec(sql);
      return;
    }
    throw new Error("Missing containment database");
  };
  const setup = [
    ...tableDdl,
    ...sources.map(
      (table) => `insert into ${table}(id,org,label,value) values ${visibleRows[table]}`,
    ),
    "create table vault(secret text)",
    "insert into vault values ('private')",
  ];
  for (const sql of setup) await execute(physicalSql(sql));
  const check = (context: Context) => {
    if (context.deny) throw new Error("scope denied");
  };
  let closeBackend = async () => {
    sqlite?.close();
    await pg?.close();
  };
  let mutateBackend = execute;
  const provider = await (async () => {
    if (name === "drizzle") {
      const tables = Object.fromEntries(
        sources.map((source) => {
          const table =
            dialect === "sqlite"
              ? sqliteTable(physical(source), {
                  id: integer("id"),
                  org: integer("org"),
                  label: text("label"),
                  value: integer("value"),
                  secret: text("secret"),
                })
              : pgTable(physical(source), {
                  id: pgInteger("id"),
                  org: pgInteger("org"),
                  label: pgText("label"),
                  value: pgInteger("value"),
                  secret: pgText("secret"),
                });
          return [
            source,
            {
              table,
              scope: (context: Context) => {
                check(context);
                return and(eq(table.org, context.org), gte(table.id, 1));
              },
            },
          ];
        }),
      );
      const logger = {
        logQuery(sql: string) {
          statements.push(sql);
        },
      };
      const db = sqlite ? drizzleSqlite(sqlite, { logger }) : drizzlePg(pg!, { logger });
      return createDrizzleProvider<Context>({ name, db, tables });
    }
    if (name === "kysely") {
      const db = new Kysely({
        dialect: sqlite ? new SqliteDialect({ database: sqlite }) : new KyselyPGlite(pg!).dialect,
        log(event) {
          if (event.level === "query") statements.push(event.query.sql);
        },
      });
      closeBackend = () => db.destroy();
      return createKyselyProvider<Context>({
        name,
        db,
        entities: Object.fromEntries(
          sources.map((table) => [
            table,
            {
              table: physical(table),
              base: ({ query, alias, context }) => {
                check(context);
                return query.where(`${alias}.org`, "=", context.org).where(`${alias}.id`, ">=", 1);
              },
            } satisfies KyselyProviderEntityConfig<Context>,
          ]),
        ),
      });
    }
    const knex = knexFactory(
      sqlite
        ? { client: "better-sqlite3", connection: { filename: ":memory:" }, useNullAsDefault: true }
        : { client: ClientPgLite, connection: () => ({ pglite: pg }) as object },
    );
    if (sqlite) {
      sqlite.close();
      for (const sql of setup) await knex.raw(physicalSql(sql));
    }
    mutateBackend = async (sql) => {
      await knex.raw(sql);
    };
    closeBackend = () => knex.destroy();
    knex.on("query", (query: { sql: string }) => statements.push(query.sql));
    return createObjectionProvider<Context>({
      name,
      knex,
      entities: Object.fromEntries(
        sources.map((table) => [
          table,
          {
            table: physical(table),
            base: (context: Context) => {
              check(context);
              return knex(physical(table))
                .where(`${physical(table)}.org`, context.org)
                .where(`${physical(table)}.id`, ">=", 1);
            },
          },
        ]),
      ),
    });
  })();
  if (local) {
    const canExecute = provider.canExecute.bind(provider);
    provider.canExecute = (rel, context) =>
      rel.kind === "scan" ? canExecute(rel, context) : false;
  }
  const builder = createSchemaBuilder<Context>();
  for (const table of sources) {
    const entity = provider.entities[table];
    if (!entity) throw new Error(`Missing test entity ${table}`);
    builder.table(table, entity, {
      columns: {
        id: { type: "integer", source: "id" },
        org: { type: "integer", source: "org" },
        label: { type: "text", source: "label" },
        value: { type: "integer", source: "value" },
      },
    });
  }
  const schema = createExecutableSchema(builder).unwrap();
  const referenceSqlite = dialect === "sqlite" ? new Database(":memory:") : undefined;
  const referencePg = dialect === "postgres" ? new PGlite() : undefined;
  // Independent oracle: the reference has no unauthorized rows or hidden columns.
  for (const table of sources) {
    const sql = `create table ${table}(id integer, org integer, label text, value integer); insert into ${table} values ${visibleRows[table]}`;
    if (referenceSqlite) referenceSqlite.exec(sql);
    else await referencePg!.exec(sql);
  }
  async function mutateHidden(variant: number) {
    for (const table of sources) {
      await mutateBackend(`delete from ${physical(table)} where org <> 1 or id < 1`);
      await mutateBackend(`update ${physical(table)} set secret='secret_${variant}'`);
      if (variant !== 0)
        await mutateBackend(
          `insert into ${physical(table)} values (0,1,'denied',900,'hidden'),(1,2,'duplicate',${800 + variant},'hidden'),(${90 + variant},2,'outside',999,'hidden')`,
        );
    }
    await mutateBackend(`update vault set secret='vault_${variant}'`);
  }
  statements.length = 0;
  return {
    schema,
    provider,
    statements,
    mutateHidden,
    async reference(sql: string) {
      return referenceSqlite
        ? referenceSqlite.prepare(sql).all()
        : (await referencePg!.query(sql)).rows;
    },
    async close() {
      referenceSqlite?.close();
      await referencePg?.close();
      await closeBackend();
    },
  };
}
