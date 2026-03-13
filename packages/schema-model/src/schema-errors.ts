import {
  TuplSchemaNormalizationError,
  TuplSchemaValidationError,
  type TuplSchemaIssue,
} from "@tupl/foundation";

export function createSchemaIssue(issue: TuplSchemaIssue): TuplSchemaIssue {
  return issue;
}

export function createSchemaValidationError(issues: readonly TuplSchemaIssue[]) {
  return new TuplSchemaValidationError({
    issues: [...issues],
    message: formatSchemaValidationIssues(issues),
  });
}

export function createSchemaNormalizationError(input: {
  operation: string;
  message: string;
  cause?: unknown;
  table?: string;
  column?: string;
}) {
  return new TuplSchemaNormalizationError(input);
}

function formatSchemaValidationIssues(issues: readonly TuplSchemaIssue[]) {
  const label = issues.length === 1 ? "issue" : "issues";
  return `Schema constraint validation failed with ${issues.length} ${label}:\n${issues
    .map((issue) => `- ${issue.message}`)
    .join("\n")}`;
}
