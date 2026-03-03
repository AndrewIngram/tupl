import { PGlite } from "../node_modules/@electric-sql/pglite/dist/index.js";
import { drizzle } from "drizzle-orm/pglite";
import type { QueryRow } from "sqlql";

import type { DownstreamRows } from "./types";
import {
  orderItemsTable,
  ordersTable,
  orgsTable,
  productsTable,
  userProductAccessTable,
  usersTable,
  vendorsTable,
} from "./downstream-model";

interface PlaygroundPgliteRuntime {
  client: PGlite;
  db: ReturnType<typeof drizzle>;
}

export interface ExecutedSqlQuery {
  sql: string;
  params: unknown[];
}

let runtimePromise: Promise<PlaygroundPgliteRuntime> | null = null;
const executedSqlQueries: ExecutedSqlQuery[] = [];

const CREATE_SCHEMA_STATEMENTS = [
  `DO $$ BEGIN CREATE TYPE user_role AS ENUM ('buyer', 'manager'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE vendor_tier AS ENUM ('standard', 'preferred'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE product_category AS ENUM ('hardware', 'software', 'services'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `DO $$ BEGIN CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,
  `CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
  );`,
  `CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    role user_role NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL,
    tier vendor_tier NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS products (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    category product_category NOT NULL,
    active BOOLEAN NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id)
  );`,
  `CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    vendor_id TEXT NOT NULL,
    status order_status NOT NULL,
    total_cents NUMERIC(12,0) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id)
  );`,
  `CREATE TABLE IF NOT EXISTS order_items (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    line_total_cents INTEGER NOT NULL,
    FOREIGN KEY (org_id) REFERENCES orgs(id),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
  );`,
  `CREATE TABLE IF NOT EXISTS user_product_access (
    id TEXT PRIMARY KEY NOT NULL,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
  );`,
];

const DROP_SCHEMA_STATEMENTS = [
  "DROP TABLE IF EXISTS user_product_access;",
  "DROP TABLE IF EXISTS order_items;",
  "DROP TABLE IF EXISTS orders;",
  "DROP TABLE IF EXISTS products;",
  "DROP TABLE IF EXISTS vendors;",
  "DROP TABLE IF EXISTS users;",
  "DROP TABLE IF EXISTS orgs;",
  "DROP TYPE IF EXISTS order_status;",
  "DROP TYPE IF EXISTS product_category;",
  "DROP TYPE IF EXISTS vendor_tier;",
  "DROP TYPE IF EXISTS user_role;",
];

async function execStatements(client: PGlite, statements: readonly string[]): Promise<void> {
  for (const statement of statements) {
    await client.exec(statement);
  }
}

async function insertRowsSerial(
  db: PlaygroundPgliteRuntime["db"],
  table: unknown,
  rows: QueryRow[],
): Promise<void> {
  for (const row of rows) {
    await db.insert(table as never).values(row as never).execute();
  }
}

async function createRuntime(): Promise<PlaygroundPgliteRuntime> {
  const client = new PGlite();

  const logger = {
    logQuery(query: string, params: unknown[]): void {
      executedSqlQueries.push({
        sql: query,
        params,
      });
    },
  };

  return {
    client,
    db: drizzle(client, { logger } as never),
  };
}

export async function getPlaygroundPgliteRuntime(): Promise<PlaygroundPgliteRuntime> {
  runtimePromise ??= createRuntime();
  return runtimePromise;
}

export async function reseedDownstreamDatabase(rows: DownstreamRows): Promise<void> {
  const runtime = await getPlaygroundPgliteRuntime();
  await execStatements(runtime.client, DROP_SCHEMA_STATEMENTS);
  await execStatements(runtime.client, CREATE_SCHEMA_STATEMENTS);

  const orgRows = (rows.orgs ?? []) as QueryRow[];
  const userRows = (rows.users ?? []) as QueryRow[];
  const vendorRows = (rows.vendors ?? []) as QueryRow[];
  const productRows = (rows.products ?? []) as QueryRow[];
  const orderRows = (rows.orders ?? []) as QueryRow[];
  const itemRows = (rows.order_items ?? []) as QueryRow[];
  const accessRows = (rows.user_product_access ?? []) as QueryRow[];

  if (orgRows.length > 0) {
    await insertRowsSerial(runtime.db, orgsTable, orgRows);
  }
  if (userRows.length > 0) {
    await insertRowsSerial(runtime.db, usersTable, userRows);
  }
  if (vendorRows.length > 0) {
    await insertRowsSerial(runtime.db, vendorsTable, vendorRows);
  }
  if (productRows.length > 0) {
    await insertRowsSerial(runtime.db, productsTable, productRows);
  }
  if (orderRows.length > 0) {
    await insertRowsSerial(runtime.db, ordersTable, orderRows);
  }
  if (itemRows.length > 0) {
    await insertRowsSerial(runtime.db, orderItemsTable, itemRows);
  }
  if (accessRows.length > 0) {
    await insertRowsSerial(runtime.db, userProductAccessTable, accessRows);
  }
}

export function clearExecutedSqlQueries(): void {
  executedSqlQueries.length = 0;
}

export function getExecutedSqlQueries(): ExecutedSqlQuery[] {
  return [...executedSqlQueries];
}
