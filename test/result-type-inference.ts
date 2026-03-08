import type { Result as BetterResult } from "better-result";

import {
  createExecutableSchemaResult,
  createSchemaBuilder,
  lowerSqlToRelResult,
  resolveTableProviderResult,
  type ExecutableSchema,
  type QueryRow,
  type QuerySession,
  type RelLoweringResult,
  type SchemaDefinition,
  type SqlqlParseError,
  type SqlqlPlanningError,
  type SqlqlProviderBindingError,
  type SqlqlResult,
} from "../src";
import { parseSqliteSelectAstResult } from "../src/sqlite-parser/parser";
import type { SelectAst } from "../src/sqlite-parser/ast";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;
type Expect<T extends true> = T;

const builder = createSchemaBuilder<Record<string, never>>();
const createExecutableSchemaResultValue = createExecutableSchemaResult(builder);

type _createExecutableSchemaResultStaysExplicit = Expect<
  Equal<
    typeof createExecutableSchemaResultValue,
    SqlqlResult<ExecutableSchema<Record<string, never>, SchemaDefinition>>
  >
>;

declare const executableSchema: ExecutableSchema<Record<string, never>, SchemaDefinition>;

type _queryResultStaysExplicit = Expect<
  Equal<
    ReturnType<typeof executableSchema.queryResult>,
    Promise<SqlqlResult<QueryRow[]>>
  >
>;

type _createSessionResultStaysExplicit = Expect<
  Equal<ReturnType<typeof executableSchema.createSessionResult>, SqlqlResult<QuerySession>>
>;

declare const resolveTableProviderResultValue: ReturnType<typeof resolveTableProviderResult>;
const _resolveTableProviderResultNarrows: BetterResult<string, SqlqlProviderBindingError> =
  resolveTableProviderResultValue;

declare const lowerSqlToRelResultValue: ReturnType<typeof lowerSqlToRelResult>;
const _lowerSqlToRelResultNarrows: BetterResult<
  RelLoweringResult,
  SqlqlParseError | SqlqlPlanningError
> = lowerSqlToRelResultValue;

declare const parseSqliteSelectAstResultValue: ReturnType<typeof parseSqliteSelectAstResult>;
const _parseSqliteSelectAstResultNarrows: BetterResult<SelectAst, SqlqlParseError> =
  parseSqliteSelectAstResultValue;
