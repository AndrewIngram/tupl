/** Assign SQL names as own data properties, including the inherited __proto__ setter name. */
export function setOwnProperty<T>(record: Record<string, T>, name: string, value: T): void {
  Object.defineProperty(record, name, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}
