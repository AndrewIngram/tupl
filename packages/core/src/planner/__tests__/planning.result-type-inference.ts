import type { Result as BetterResult } from "better-result";

import {
  lowerSqlToRelResult,
  type RelLoweringResult,
  type TuplParseError,
  type TuplPlanningError,
} from "@tupl/core";

declare const lowerSqlToRelResultValue: ReturnType<typeof lowerSqlToRelResult>;

const _lowerSqlToRelResultNarrows: BetterResult<
  RelLoweringResult,
  TuplParseError | TuplPlanningError
> = lowerSqlToRelResultValue;
