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
    // @ts-expect-error Every callback branch must be synchronous.
    derive({ name }, ({ name }) => (name ? name : Promise.resolve(name)));
    // @ts-expect-error A nullable async branch is still asynchronous.
    derive({}, (): string | null | Promise<string> => Promise.resolve("name"));
    // @ts-expect-error Promise-like values are asynchronous even without a native Promise.
    derive({}, (): number | PromiseLike<number> => Promise.resolve(1));
    const unknown = derive({ body }, ({ body }) => body);
    // @ts-expect-error Unknown output must be narrowed before declaring a SQL string.
    col.string(unknown);
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

const derivedJsonTable = builder.table("derivedJson", entity, {
  columns: ({ col, derive }) => {
    const object = derive({}, () => ({ title: "known", count: 1 }));
    const body = col.json(object, { nullable: false });
    const nullableBody = col.json(object);
    col.json(
      // @ts-expect-error Non-nullable JSON rejects a known null result.
      derive({}, () => null),
      { nullable: false },
    );
    col.json(
      // @ts-expect-error Non-nullable JSON rejects a known undefined result.
      derive({}, () => undefined),
      { nullable: false },
    );
    const maybeObject = derive({}, (): { title: string } | null => null);
    // @ts-expect-error Nullable derive values preserve null before column wrapping.
    derive({ maybeObject }, ({ maybeObject }) => maybeObject.title);
    // @ts-expect-error Non-nullable JSON rejects nullable object unions.
    col.json(maybeObject, { nullable: false });
    const optionalObject = derive({}, (): { title: string } | undefined => undefined);
    // @ts-expect-error Non-nullable JSON rejects optional object unions.
    col.json(optionalObject, { nullable: false });
    const runtimeJson = derive({ body: col.json("body") }, ({ body }) => body);
    // Unknown JSON is validated at runtime and must not be mistaken for a known null result.
    col.json(runtimeJson, { nullable: false });
    const nullableResult = col.json(derive({}, (): { title: string } | null => null));
    const unknown = col.json(derive({ body: col.json("body") }, ({ body }) => body));
    const title = derive({ body, nullableBody, nullableResult, unknown }, (values) => {
      const title: string = values.body.title;
      const count: number = values.body.count;
      const optionalTitle: string | undefined = values.nullableBody?.title;
      const optionalResult: string | undefined = values.nullableResult?.title;
      // @ts-expect-error Derived JSON preserves its known keys.
      void values.body.missing;
      // @ts-expect-error Nullable column declarations still require a null check.
      void values.nullableBody.title;
      // @ts-expect-error Nullable computation results require a null check.
      void values.nullableResult.title;
      // @ts-expect-error Unknown JSON remains unknown after column wrapping.
      void values.unknown.title;
      void [count, optionalTitle, optionalResult];
      return title;
    });
    return { body, nullableBody, title: col.string(title) };
  },
});
builder.view("derivedJsonView", ({ scan }) => scan(derivedJsonTable), {
  columns: ({ col, derive }) => {
    const body = col.json(derivedJsonTable, "body", { nullable: false });
    const nullableBody = col.json(derivedJsonTable, "nullableBody");
    return {
      title: col.string(
        derive({ body, nullableBody }, ({ body, nullableBody }) => {
          const title: string = body.title;
          const count: number = body.count;
          const optionalTitle: string | undefined = nullableBody?.title;
          // @ts-expect-error Derived JSON retains its shape across a view reference.
          void body.missing;
          // @ts-expect-error View references preserve nullable declarations.
          void nullableBody.title;
          void [count, optionalTitle];
          return title;
        }),
      ),
    };
  },
});
