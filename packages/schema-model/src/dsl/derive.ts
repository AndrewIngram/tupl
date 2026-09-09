import type { RelExpr, RelLocalOperation } from "@tupl/foundation";
import type {
  SchemaCalculatedColumnDefinition,
  SchemaColumnLensDefinition,
  SchemaTypedColumnDefinition,
} from "../contracts/table-definition-contracts";

/** Value metadata is erased at runtime; the handle itself owns dependency identity. */
export interface SchemaValueHandle<T> {
  readonly __value?: T;
}

export interface SchemaDerivedValue<T = unknown> extends SchemaValueHandle<T> {
  readonly kind: "dsl_derived";
  readonly operation: RelLocalOperation;
}

export type DeriveInput =
  | SchemaTypedColumnDefinition
  | SchemaColumnLensDefinition
  | SchemaCalculatedColumnDefinition
  | SchemaDerivedValue;

export type DeriveValues<T> = {
  readonly [K in keyof T]: T[K] extends SchemaValueHandle<infer V> ? V : unknown;
};

interface LocalImplementation {
  dependencies: Readonly<Record<string, DeriveInput>>;
  evaluate: (values: Readonly<Record<string, unknown>>) => unknown;
}

const implementations = new WeakMap<RelLocalOperation, LocalImplementation>();
let nextOperation = 0;

export function registerLocalOperation(
  label: string,
  implementation: LocalImplementation,
): RelLocalOperation {
  const operation = Object.freeze({ id: `derive_${++nextOperation}`, label });
  implementations.set(operation, implementation);
  return operation;
}

export function getLocalImplementation(operation: RelLocalOperation) {
  const implementation = implementations.get(operation);
  if (!implementation) throw new Error(`Unknown local computation: ${operation.id}`);
  return implementation;
}

export function evaluateLocalOperation(operation: RelLocalOperation, args: unknown[]) {
  const implementation = getLocalImplementation(operation);
  const values = Object.fromEntries(
    Object.keys(implementation.dependencies).map((name, index) => [name, args[index]]),
  );
  const result = implementation.evaluate(values);
  if (
    result != null &&
    (typeof result === "object" || typeof result === "function") &&
    "then" in result &&
    typeof result.then === "function"
  ) {
    // Invalid async results must still be observed so rejection cannot escape the query failure.
    void Promise.resolve(result).catch(() => undefined);
    throw new Error(`Computation ${operation.label} must return synchronously.`);
  }
  return result;
}

export function derive<const D extends Record<string, DeriveInput>, T>(
  dependencies: D,
  compute: (
    values: DeriveValues<D>,
  ) => T & ([Extract<T, PromiseLike<unknown>>] extends [never] ? unknown : never),
): SchemaDerivedValue<T> {
  // The dependency keys and their value types are established together by this public generic.
  const operation = registerLocalOperation("derive", {
    dependencies: Object.freeze({ ...dependencies }),
    evaluate: (values) => compute(values as DeriveValues<D>),
  });
  return Object.freeze({ kind: "dsl_derived", operation });
}

export function isDerivedValue(value: unknown): value is SchemaDerivedValue {
  return (
    !!value &&
    typeof value === "object" &&
    "kind" in value &&
    value.kind === "dsl_derived" &&
    "operation" in value &&
    typeof value.operation === "object" &&
    value.operation !== null &&
    implementations.has(value.operation as RelLocalOperation)
  );
}

export function derivedExpression(value: SchemaDerivedValue): RelExpr {
  return { kind: "local", operation: value.operation, args: [] };
}

const owners = new WeakMap<object, symbol>();

export function ownColumn<T extends object>(value: T, owner: symbol): T {
  owners.set(value, owner);
  return value;
}

export function createDerive(owner: symbol): typeof derive {
  return (dependencies, compute) => {
    for (const dependency of Object.values(dependencies)) {
      if (owners.get(dependency) !== owner)
        throw new Error("Derived dependencies must belong to this columns declaration.");
    }
    return ownColumn(derive(dependencies, compute), owner);
  };
}

export function assertColumnOwner(value: object, owner: symbol) {
  if (owners.get(value) !== owner)
    throw new Error("Derived values must belong to this columns declaration.");
}
