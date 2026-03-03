import { defineSchema, type SchemaDefinition } from "sqlql";

import type {
  CatalogQueryEntry,
  DownstreamRows,
  PlaygroundQueryPreset,
  PlaygroundScenarioPreset,
} from "./types";

export const FACADE_SCHEMA: SchemaDefinition = defineSchema({
  tables: {
    my_orders: {
      provider: "dbProvider",
      columns: {
        id: { type: "text", nullable: false, primaryKey: true },
        vendor_id: {
          type: "text",
          nullable: false,
          foreignKey: {
            table: "vendors_for_org",
            column: "id",
          },
        },
        status: { type: "text", nullable: false, enum: ["pending", "paid", "shipped"] as const },
        total_cents: { type: "integer", nullable: false },
        created_at: { type: "timestamp", nullable: false },
      },
    },
    my_order_items: {
      provider: "dbProvider",
      columns: {
        id: { type: "text", nullable: false, primaryKey: true },
        order_id: {
          type: "text",
          nullable: false,
          foreignKey: {
            table: "my_orders",
            column: "id",
          },
        },
        product_id: {
          type: "text",
          nullable: false,
          foreignKey: {
            table: "active_products",
            column: "id",
          },
        },
        quantity: { type: "integer", nullable: false },
        line_total_cents: { type: "integer", nullable: false },
      },
    },
    vendors_for_org: {
      provider: "dbProvider",
      columns: {
        id: { type: "text", nullable: false, primaryKey: true },
        name: { type: "text", nullable: false },
        tier: { type: "text", nullable: false, enum: ["standard", "preferred"] as const },
      },
    },
    active_products: {
      provider: "dbProvider",
      columns: {
        id: { type: "text", nullable: false, primaryKey: true },
        sku: { type: "text", nullable: false },
        name: { type: "text", nullable: false },
        category: {
          type: "text",
          nullable: false,
          enum: ["hardware", "software", "services"] as const,
        },
      },
    },
  },
});

export const QUERY_PRESETS: PlaygroundQueryPreset[] = [
  {
    id: "orders_with_vendors",
    label: "My orders with vendors",
    description: "Simple join over facade tables; downstream adds org/user scope.",
    sql: `
SELECT o.id, o.status, o.total_cents, o.created_at, v.name AS vendor_name
FROM my_orders o
JOIN vendors_for_org v ON o.vendor_id = v.id
ORDER BY o.created_at DESC;
    `.trim(),
  },
  {
    id: "vendor_spend",
    label: "My spend by vendor",
    description: "Join + grouped aggregate with ORDER BY metric alias.",
    sql: `
SELECT v.name AS vendor_name, COUNT(*) AS order_count, SUM(o.total_cents) AS spend_cents
FROM my_orders o
JOIN vendors_for_org v ON o.vendor_id = v.id
GROUP BY v.name
ORDER BY spend_cents DESC;
    `.trim(),
  },
  {
    id: "items_with_products",
    label: "My line items with products",
    description: "Facade hides downstream user-product access join table filtering active_products.",
    sql: `
SELECT i.order_id, p.sku, p.name, i.quantity, i.line_total_cents
FROM my_order_items i
JOIN active_products p ON i.product_id = p.id
ORDER BY i.order_id, p.sku;
    `.trim(),
  },
  {
    id: "top_products",
    label: "Top products by spend",
    description: "Aggregate over m2m-filtered active_products via a hidden downstream access table.",
    sql: `
SELECT p.name, SUM(i.line_total_cents) AS spend_cents, SUM(i.quantity) AS units
FROM my_order_items i
JOIN active_products p ON i.product_id = p.id
GROUP BY p.name
ORDER BY spend_cents DESC;
    `.trim(),
  },
  {
    id: "preferred_vendor_orders",
    label: "Orders from preferred vendors",
    description: "IN subquery over another facade table.",
    sql: `
SELECT id, vendor_id, total_cents
FROM my_orders
WHERE vendor_id IN (
  SELECT id
  FROM vendors_for_org
  WHERE tier = 'preferred'
)
ORDER BY total_cents DESC;
    `.trim(),
  },
  {
    id: "status_distinct",
    label: "Distinct statuses",
    description: "DISTINCT projection shape.",
    sql: `
SELECT DISTINCT status
FROM my_orders
ORDER BY status;
    `.trim(),
  },
  {
    id: "product_coverage",
    label: "Product coverage (left join)",
    description: "LEFT JOIN with aggregate to include products with zero units.",
    sql: `
SELECT p.name, p.category, COUNT(i.id) AS line_count, SUM(i.quantity) AS units
FROM active_products p
LEFT JOIN my_order_items i ON i.product_id = p.id
GROUP BY p.name, p.category
ORDER BY units DESC, p.name;
    `.trim(),
  },
  {
    id: "activity_union",
    label: "Activity union",
    description: "UNION ALL over two facade tables.",
    sql: `
SELECT id, vendor_id AS activity_ref
FROM my_orders
UNION ALL
SELECT id, product_id AS activity_ref
FROM my_order_items
ORDER BY activity_ref, id;
    `.trim(),
  },
  {
    id: "paid_orders",
    label: "Paid orders over threshold",
    description: "Simple filter + ordering.",
    sql: `
SELECT id, total_cents, created_at
FROM my_orders
WHERE status = 'paid' AND total_cents >= 20000
ORDER BY total_cents DESC;
    `.trim(),
  },
  {
    id: "vendor_rank",
    label: "Vendor spend rank",
    description: "CTE + window function shape.",
    sql: `
WITH vendor_totals AS (
  SELECT
    v.name AS vendor_name,
    SUM(o.total_cents) AS spend_cents
  FROM my_orders o
  JOIN vendors_for_org v ON o.vendor_id = v.id
  GROUP BY v.name
)
SELECT
  vendor_name,
  spend_cents,
  DENSE_RANK() OVER (ORDER BY spend_cents DESC) AS spend_rank
FROM vendor_totals
ORDER BY spend_rank, vendor_name;
    `.trim(),
  },
];

const BASE_DOWNSTREAM_ROWS: DownstreamRows = {
  orgs: [
    { id: "org_acme", name: "Acme Corp" },
    { id: "org_globex", name: "Globex LLC" },
  ],
  users: [
    {
      id: "u_alex",
      org_id: "org_acme",
      email: "alex@acme.example",
      display_name: "Alex Rivera",
      role: "buyer",
    },
    {
      id: "u_jordan",
      org_id: "org_acme",
      email: "jordan@acme.example",
      display_name: "Jordan Lee",
      role: "manager",
    },
    {
      id: "u_sam",
      org_id: "org_globex",
      email: "sam@globex.example",
      display_name: "Sam Patel",
      role: "buyer",
    },
  ],
  vendors: [
    { id: "v_northwind", org_id: "org_acme", name: "Northwind Supply", tier: "preferred" },
    { id: "v_metro", org_id: "org_acme", name: "Metro Parts", tier: "standard" },
    { id: "v_sunrise", org_id: "org_globex", name: "Sunrise Tech", tier: "preferred" },
  ],
  products: [
    {
      id: "p_router",
      org_id: "org_acme",
      sku: "RTR-100",
      name: "Edge Router",
      category: "hardware",
      active: true,
    },
    {
      id: "p_backup",
      org_id: "org_acme",
      sku: "BKP-200",
      name: "Backup Service",
      category: "services",
      active: true,
    },
    {
      id: "p_archive",
      org_id: "org_acme",
      sku: "ARC-300",
      name: "Archive Toolkit",
      category: "software",
      active: false,
    },
    {
      id: "p_sensor",
      org_id: "org_globex",
      sku: "SNS-500",
      name: "Plant Sensor",
      category: "hardware",
      active: true,
    },
  ],
  orders: [
    {
      id: "o_1001",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_northwind",
      status: "paid",
      total_cents: 48000,
      created_at: "2026-02-02T10:00:00Z",
    },
    {
      id: "o_1002",
      org_id: "org_acme",
      user_id: "u_alex",
      vendor_id: "v_metro",
      status: "shipped",
      total_cents: 25500,
      created_at: "2026-02-06T09:30:00Z",
    },
    {
      id: "o_1003",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_metro",
      status: "pending",
      total_cents: 11500,
      created_at: "2026-02-10T14:15:00Z",
    },
    {
      id: "o_2001",
      org_id: "org_globex",
      user_id: "u_sam",
      vendor_id: "v_sunrise",
      status: "paid",
      total_cents: 91000,
      created_at: "2026-02-04T08:20:00Z",
    },
  ],
  order_items: [
    {
      id: "oi_1",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1001",
      product_id: "p_router",
      quantity: 2,
      line_total_cents: 36000,
    },
    {
      id: "oi_2",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1001",
      product_id: "p_backup",
      quantity: 1,
      line_total_cents: 12000,
    },
    {
      id: "oi_3",
      org_id: "org_acme",
      user_id: "u_alex",
      order_id: "o_1002",
      product_id: "p_backup",
      quantity: 2,
      line_total_cents: 25500,
    },
    {
      id: "oi_4",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1003",
      product_id: "p_router",
      quantity: 1,
      line_total_cents: 11500,
    },
    {
      id: "oi_5",
      org_id: "org_globex",
      user_id: "u_sam",
      order_id: "o_2001",
      product_id: "p_sensor",
      quantity: 7,
      line_total_cents: 91000,
    },
  ],
  user_product_access: [
    { id: "upa_1", user_id: "u_alex", product_id: "p_router" },
    { id: "upa_2", user_id: "u_alex", product_id: "p_backup" },
    { id: "upa_3", user_id: "u_jordan", product_id: "p_router" },
    { id: "upa_4", user_id: "u_sam", product_id: "p_sensor" },
  ],
};

function cloneRows(rows: DownstreamRows): DownstreamRows {
  return JSON.parse(JSON.stringify(rows)) as DownstreamRows;
}

function withJordanExpansion(): DownstreamRows {
  const rows = cloneRows(BASE_DOWNSTREAM_ROWS);
  rows.orders = [
    ...(rows.orders ?? []),
    {
      id: "o_1004",
      org_id: "org_acme",
      user_id: "u_jordan",
      vendor_id: "v_northwind",
      status: "paid",
      total_cents: 64000,
      created_at: "2026-02-12T12:00:00Z",
    },
  ];
  rows.order_items = [
    ...(rows.order_items ?? []),
    {
      id: "oi_6",
      org_id: "org_acme",
      user_id: "u_jordan",
      order_id: "o_1004",
      product_id: "p_router",
      quantity: 3,
      line_total_cents: 64000,
    },
  ];
  return rows;
}

export const SCENARIO_PRESETS: PlaygroundScenarioPreset[] = [
  {
    id: "acme_alex",
    label: "Acme: Alex",
    description: "Alex is a buyer in Acme. Data includes multiple vendors and active/inactive products.",
    context: {
      orgId: "org_acme",
      userId: "u_alex",
    },
    rows: cloneRows(BASE_DOWNSTREAM_ROWS),
    defaultQueryId: "orders_with_vendors",
  },
  {
    id: "acme_jordan",
    label: "Acme: Jordan",
    description: "Jordan is a manager in Acme with a larger paid order profile.",
    context: {
      orgId: "org_acme",
      userId: "u_jordan",
    },
    rows: withJordanExpansion(),
    defaultQueryId: "vendor_spend",
  },
  {
    id: "globex_sam",
    label: "Globex: Sam",
    description: "Sam is a buyer in a different org to demonstrate strict org/user lens scoping.",
    context: {
      orgId: "org_globex",
      userId: "u_sam",
    },
    rows: cloneRows(BASE_DOWNSTREAM_ROWS),
    defaultQueryId: "items_with_products",
  },
];

export const DEFAULT_SCENARIO_ID = SCENARIO_PRESETS[0]?.id ?? "acme_alex";
export const DEFAULT_QUERY_ID =
  SCENARIO_PRESETS[0]?.defaultQueryId ?? QUERY_PRESETS[0]?.id ?? "orders_with_vendors";

export function buildQueryCatalog(queries: PlaygroundQueryPreset[]): CatalogQueryEntry[] {
  return queries.map((query) => ({
    id: query.id,
    label: query.label,
    sql: query.sql,
    ...(query.description ? { description: query.description } : {}),
  }));
}

export function serializeJson(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}
