import { createDataEntityHandle, defineSchema } from "../src";

const ordersEntity = createDataEntityHandle({
  entity: "orders_raw",
  provider: "warehouse",
});
const vendorsEntity = createDataEntityHandle<"id" | "name">({
  entity: "vendors_raw",
  provider: "warehouse",
});

defineSchema(({ table, view, rel, col, expr, agg }) => {
  const myOrders = table({
    from: ordersEntity,
    columns: {
      id: col("id"),
      vendorId: col("vendor_id"),
      totalCents: col("total_cents"),
      vendorName: {
        source: col(vendorsEntity, "name"),
      },
    },
  });

  const okEntityColRef = col(vendorsEntity, "id");
  void okEntityColRef;
  // @ts-expect-error - column does not exist on vendorsEntity
  col(vendorsEntity, "doesNotExist");

  const okColRef = col(myOrders, "vendorId");
  void okColRef;

  // @ts-expect-error - column does not exist on myOrders
  col(myOrders, "doesNotExist");

  return {
    tables: {
      myOrders,
      spendByVendor: view({
        rel: () =>
          rel.aggregate({
            from: rel.join({
              left: rel.scan(myOrders),
              right: rel.scan(myOrders),
              on: expr.eq(col(myOrders, "vendorId"), col(myOrders, "id")),
              type: "inner",
            }),
            groupBy: [col(myOrders, "vendorId")],
            measures: {
              spend: agg.sum(col(myOrders, "totalCents")),
            },
          }),
        columns: {
          vendorId: col("vendorId"),
          spend: col("spend"),
        },
      }),
    },
  };
});
