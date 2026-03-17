import { afterEach, describe, expect, it, vi } from "vite-plus/test";

describe("playground sandbox client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.resetModules();
    vi.doUnmock("../src/playground-sandbox");
    delete (globalThis as typeof globalThis & { Worker?: typeof Worker }).Worker;
  });

  it("falls back to in-process execution when the worker request cannot be posted", async () => {
    const validateSchemaInSandbox = vi.fn().mockResolvedValue({
      ok: false,
      issues: [{ severity: "error", message: "SCHEMA_EXPORT_MISSING" }],
      diagnostics: [],
      graph: null,
      schemaText: "",
      exports: [],
    });

    vi.doMock("../src/playground-sandbox", () => {
      return {
        validateSchemaInSandbox,
        createSandboxSession: vi.fn(),
        disposeSandboxSession: vi.fn(),
        nextSandboxSessionEvent: vi.fn(),
        replaySandboxSession: vi.fn(),
        runSandboxSessionToCompletion: vi.fn(),
      };
    });

    class ThrowingWorker {
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onerror: ((event: ErrorEvent) => void) | null = null;
      onmessageerror: ((event: MessageEvent<unknown>) => void) | null = null;

      postMessage(): void {
        throw new DOMException("Could not clone request.", "DataCloneError");
      }

      terminate(): void {}
    }

    (
      globalThis as typeof globalThis & {
        Worker?: new (url: URL | string, options?: WorkerOptions) => Worker;
      }
    ).Worker = ThrowingWorker as unknown as new (
      url: URL | string,
      options?: WorkerOptions,
    ) => Worker;

    const { requestSandboxWorker } = await import("../src/playground-sandbox-client");
    const result = await requestSandboxWorker("validate_schema", {
      schemaCode: "",
      options: {},
    });

    expect(validateSchemaInSandbox).toHaveBeenCalledWith("", {});
    expect(result.ok).toBe(false);
    expect(result.issues[0]?.message).toContain("SCHEMA_EXPORT_MISSING");
  });

  it("rejects other pending worker requests when a transport failure resets the worker", async () => {
    const validateSchemaInSandbox = vi.fn().mockResolvedValue({
      ok: false,
      issues: [{ severity: "error", message: "SCHEMA_EXPORT_MISSING" }],
      diagnostics: [],
      graph: null,
      schemaText: "",
      exports: [],
    });

    vi.doMock("../src/playground-sandbox", () => {
      return {
        validateSchemaInSandbox,
        createSandboxSession: vi.fn(),
        disposeSandboxSession: vi.fn(),
        nextSandboxSessionEvent: vi.fn(),
        replaySandboxSession: vi.fn(),
        runSandboxSessionToCompletion: vi.fn(),
      };
    });

    class FlakyWorker {
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onerror: ((event: ErrorEvent) => void) | null = null;
      onmessageerror: ((event: MessageEvent<unknown>) => void) | null = null;
      private postCount = 0;

      postMessage(): void {
        this.postCount += 1;
        if (this.postCount === 2) {
          throw new DOMException("Could not clone request.", "DataCloneError");
        }
      }

      terminate(): void {}
    }

    (
      globalThis as typeof globalThis & {
        Worker?: new (url: URL | string, options?: WorkerOptions) => Worker;
      }
    ).Worker = FlakyWorker as unknown as new (url: URL | string, options?: WorkerOptions) => Worker;

    const { requestSandboxWorker } = await import("../src/playground-sandbox-client");
    const firstRequest = requestSandboxWorker("validate_schema", {
      schemaCode: "export const schema = null;",
      options: {},
    });
    const secondRequest = requestSandboxWorker("validate_schema", {
      schemaCode: "",
      options: {},
    });

    await expect(firstRequest).resolves.toMatchObject({
      ok: false,
    });
    await expect(secondRequest).resolves.toMatchObject({
      ok: false,
    });
    expect(validateSchemaInSandbox).toHaveBeenCalledTimes(2);
  });

  it("recreates the worker and retries once after a stale bundle error", async () => {
    let workerInstanceCount = 0;

    class StaleBundleWorker {
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onerror: ((event: ErrorEvent) => void) | null = null;
      onmessageerror: ((event: MessageEvent<unknown>) => void) | null = null;
      private readonly instanceNumber: number;

      constructor() {
        workerInstanceCount += 1;
        this.instanceNumber = workerInstanceCount;
      }

      postMessage(request: { id: number }): void {
        queueMicrotask(() => {
          if (this.instanceNumber === 1) {
            this.onmessage?.({
              data: {
                id: request.id,
                ok: false,
                error: "[SANDBOX_RESEED] Invalid FS bundle size: 2 !== 1",
              },
            } as MessageEvent<unknown>);
            return;
          }

          this.onmessage?.({
            data: {
              id: request.id,
              ok: true,
              payload: {
                ok: false,
                issues: [{ severity: "error", message: "SCHEMA_EXPORT_MISSING" }],
                diagnostics: [],
                graph: null,
                schemaText: "",
                exports: [],
              },
            },
          } as MessageEvent<unknown>);
        });
      }

      terminate(): void {}
    }

    (
      globalThis as typeof globalThis & {
        Worker?: new (url: URL | string, options?: WorkerOptions) => Worker;
      }
    ).Worker = StaleBundleWorker as unknown as new (
      url: URL | string,
      options?: WorkerOptions,
    ) => Worker;

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const { requestSandboxWorker } = await import("../src/playground-sandbox-client");
    const result = await requestSandboxWorker("validate_schema", {
      schemaCode: "",
      options: {},
    });

    expect(workerInstanceCount).toBe(2);
    expect(warnSpy).toHaveBeenCalledWith(
      "[playground-sandbox] resetting worker after stale bundle error",
      expect.any(Error),
    );
    expect(result.ok).toBe(false);
    expect(result.issues[0]?.message).toContain("SCHEMA_EXPORT_MISSING");
  });

  it("rejects other pending requests when retrying after a stale bundle error", async () => {
    const validateSchemaInSandbox = vi.fn().mockResolvedValue({
      ok: false,
      issues: [{ severity: "error", message: "FALLBACK_SCHEMA_EXPORT_MISSING" }],
      diagnostics: [],
      graph: null,
      schemaText: "",
      exports: [],
    });

    vi.doMock("../src/playground-sandbox", () => {
      return {
        validateSchemaInSandbox,
        createSandboxSession: vi.fn(),
        disposeSandboxSession: vi.fn(),
        nextSandboxSessionEvent: vi.fn(),
        replaySandboxSession: vi.fn(),
        runSandboxSessionToCompletion: vi.fn(),
      };
    });

    let workerInstanceCount = 0;
    let firstWorkerRequestCount = 0;

    class StaleBundleWorker {
      onmessage: ((event: MessageEvent<unknown>) => void) | null = null;
      onerror: ((event: ErrorEvent) => void) | null = null;
      onmessageerror: ((event: MessageEvent<unknown>) => void) | null = null;
      private readonly instanceNumber: number;

      constructor() {
        workerInstanceCount += 1;
        this.instanceNumber = workerInstanceCount;
      }

      postMessage(request: { id: number }): void {
        if (this.instanceNumber === 1) {
          firstWorkerRequestCount += 1;
          if (firstWorkerRequestCount === 1) {
            queueMicrotask(() => {
              this.onmessage?.({
                data: {
                  id: request.id,
                  ok: false,
                  error: "[SANDBOX_RESEED] Invalid FS bundle size: 2 !== 1",
                },
              } as MessageEvent<unknown>);
            });
          }
          return;
        }

        queueMicrotask(() => {
          this.onmessage?.({
            data: {
              id: request.id,
              ok: true,
              payload: {
                ok: false,
                issues: [{ severity: "error", message: "SCHEMA_EXPORT_MISSING" }],
                diagnostics: [],
                graph: null,
                schemaText: "",
                exports: [],
              },
            },
          } as MessageEvent<unknown>);
        });
      }

      terminate(): void {}
    }

    (
      globalThis as typeof globalThis & {
        Worker?: new (url: URL | string, options?: WorkerOptions) => Worker;
      }
    ).Worker = StaleBundleWorker as unknown as new (
      url: URL | string,
      options?: WorkerOptions,
    ) => Worker;

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const { requestSandboxWorker } = await import("../src/playground-sandbox-client");
    const retriedRequest = requestSandboxWorker("validate_schema", {
      schemaCode: "",
      options: {},
    });
    const pendingRequest = requestSandboxWorker("validate_schema", {
      schemaCode: "export const schema = null;",
      options: {},
    });

    await expect(retriedRequest).resolves.toMatchObject({
      ok: false,
      issues: [{ message: "SCHEMA_EXPORT_MISSING" }],
    });
    await expect(pendingRequest).resolves.toMatchObject({
      ok: false,
      issues: [{ message: "FALLBACK_SCHEMA_EXPORT_MISSING" }],
    });
    expect(workerInstanceCount).toBe(2);
    expect(warnSpy).toHaveBeenCalledWith(
      "[playground-sandbox] resetting worker after stale bundle error",
      expect.any(Error),
    );
    expect(validateSchemaInSandbox).toHaveBeenCalledWith("export const schema = null;", {});
  });
});
