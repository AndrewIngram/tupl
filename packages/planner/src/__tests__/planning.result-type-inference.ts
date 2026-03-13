import type { Result as BetterResult } from "better-result";

import { lowerSqlToRelResult, type RelLoweringResult } from "../index";
import type {
  TuplParseError,
  TuplPlanningError,
  TuplSchemaNormalizationError,
} from "@tupl/foundation";

declare const lowerSqlToRelResultValue: ReturnType<typeof lowerSqlToRelResult>;

const _lowerSqlToRelResultNarrows: BetterResult<
  RelLoweringResult,
  TuplParseError | TuplPlanningError | TuplSchemaNormalizationError
> = lowerSqlToRelResultValue;
