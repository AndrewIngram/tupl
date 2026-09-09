import { createDataEntityHandle } from "@tupl/provider-kit";
import { createSchemaBuilder } from "../builder";

const builder = createSchemaBuilder();
const entity = createDataEntityHandle<"body" | "name" | "amount">({
  entity: "raw",
  provider: "db",
});
builder.table("computed", entity, {
  columns: ({ col, derive, expr }) => {
    const name = col.string("name", { nullable: false });
    const amount = col.integer("amount");
    const body = col.json("body");
    const intermediate = derive({ name, amount, body }, ({ name, amount, body }) => {
      const check: string = name;
      const nullable: number | null = amount;
      // @ts-expect-error JSON is unknown until validated.
      void body.text;
      // @ts-expect-error Nullable inputs require explicit handling.
      amount.toFixed();
      return { title: check, value: nullable ?? 0, lengths: new Map([[check, check.length]]) };
    });
    const text = derive({ intermediate }, ({ intermediate }) => intermediate.title);
    const number = derive({ intermediate }, ({ intermediate }) => intermediate.value);
    // @ts-expect-error Async callbacks are outside the initial contract.
    derive({ name }, async ({ name }) => name);
    // @ts-expect-error Dependencies must be column definitions or derive handles.
    derive({ name: "name" }, ({ name }) => name);
    // @ts-expect-error The computation returns a number, not a SQL string.
    col.string(number);
    col.string(
      // @ts-expect-error Non-nullable output cannot accept nullable computation results.
      derive({ name }, () => null),
      { nullable: false },
    );
    const calculated = col.real(expr.add(expr.literal(1), expr.literal(2)), { nullable: false });
    const doubled = derive({ calculated }, ({ calculated }) => calculated * 2);
    return { name, title: col.string(text), doubled: col.real(doubled) };
  },
});

const typedEntity = createDataEntityHandle<"body", { body: { title: string } }>({
  entity: "typed",
  provider: "db",
});
const typedTable = builder.table("typed", typedEntity, {
  columns: ({ col }) => ({ body: col.json("body", { nullable: false }) }),
});
builder.view("typedView", ({ scan }) => scan(typedTable), {
  columns: ({ col, derive }) => ({
    title: col.string(
      derive({ body: col.json(typedTable, "body", { nullable: false }) }, ({ body }) => {
        const title: string = body.title;
        // @ts-expect-error Authoritative JSON retains its declared object shape through the table.
        void body.missing;
        return title;
      }),
    ),
  }),
});
