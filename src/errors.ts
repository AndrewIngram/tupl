import { TaggedError, type Result as BetterResult } from "better-result";

import type { SqlqlDiagnostic } from "./provider";

export class SqlqlDiagnosticError extends TaggedError("SqlqlDiagnosticError")<{
  diagnostics: SqlqlDiagnostic[];
  message: string;
}>() {}

export class SqlqlGuardrailError extends TaggedError("SqlqlGuardrailError")<{
  actual: number;
  guardrail: string;
  limit: number;
  message: string;
}>() {}

export class SqlqlTimeoutError extends TaggedError("SqlqlTimeoutError")<{
  cause?: unknown;
  message: string;
  operation: string;
  timeoutMs: number;
}>() {}

export class SqlqlRuntimeError extends TaggedError("SqlqlRuntimeError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class SqlqlParseError extends TaggedError("SqlqlParseError")<{
  cause?: unknown;
  message: string;
  sql: string;
}>() {}

export class SqlqlPlanningError extends TaggedError("SqlqlPlanningError")<{
  cause?: unknown;
  message: string;
  operation: string;
}>() {}

export class SqlqlProviderBindingError extends TaggedError("SqlqlProviderBindingError")<{
  cause?: unknown;
  message: string;
  provider?: string;
  table?: string;
}>() {}

export type SqlqlError =
  | SqlqlDiagnosticError
  | SqlqlGuardrailError
  | SqlqlTimeoutError
  | SqlqlRuntimeError
  | SqlqlParseError
  | SqlqlPlanningError
  | SqlqlProviderBindingError;

export type SqlqlResult<T> = BetterResult<T, SqlqlError>;
