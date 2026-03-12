function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function positionFromIndex(sql: string, rawIndex: number): { line: number; column: number } {
  const index = Math.max(0, Math.min(rawIndex, sql.length));
  let line = 1;
  let column = 1;

  for (let i = 0; i < index; i += 1) {
    if (sql[i] === "\n") {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }

  return { line, column };
}

function rangeFromIndex(sql: string, startIndex: number, endIndexExclusive: number) {
  const start = positionFromIndex(sql, startIndex);
  const end = positionFromIndex(sql, Math.max(startIndex + 1, endIndexExclusive));
  return {
    startLineNumber: start.line,
    startColumn: start.column,
    endLineNumber: end.line,
    endColumn: end.column,
  };
}

function findTokenRangeAtPosition(sql: string, position: number) {
  const tokenChar = /[A-Za-z0-9_.$"]/u;
  const clamped = Math.max(0, Math.min(position, Math.max(0, sql.length - 1)));
  let start = clamped;
  let end = clamped + 1;

  while (start > 0 && tokenChar.test(sql[start - 1] ?? "")) {
    start -= 1;
  }
  while (end < sql.length && tokenChar.test(sql[end] ?? "")) {
    end += 1;
  }

  if (start === end) {
    end = Math.min(sql.length, start + 1);
  }
  return rangeFromIndex(sql, start, end);
}

function findIdentifierRange(sql: string, identifier: string) {
  if (!identifier) {
    return undefined;
  }

  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findQualifiedIdentifierRange(sql: string, qualifier: string, identifier: string) {
  if (!qualifier || !identifier) {
    return undefined;
  }

  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }

  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findClauseBounds(
  sql: string,
  clause: "where" | "order_by",
): {
  start: number;
  end: number;
} | null {
  const source = sql;
  const startMatch =
    clause === "where" ? /\bwhere\b/iu.exec(source) : /\border\s+by\b/iu.exec(source);
  if (!startMatch || startMatch.index == null) {
    return null;
  }

  const start = startMatch.index;
  const remainder = source.slice(start + startMatch[0].length);
  const endPatterns =
    clause === "where"
      ? [
          /\bgroup\s+by\b/iu,
          /\border\s+by\b/iu,
          /\blimit\b/iu,
          /\boffset\b/iu,
          /\bhaving\b/iu,
          /\bunion\b/iu,
          /\bintersect\b/iu,
          /\bexcept\b/iu,
          /;/u,
        ]
      : [/\blimit\b/iu, /\boffset\b/iu, /\bunion\b/iu, /\bintersect\b/iu, /\bexcept\b/iu, /;/u];
  let minRelativeEnd = remainder.length;

  for (const pattern of endPatterns) {
    const match = pattern.exec(remainder);
    if (match?.index == null) {
      continue;
    }
    minRelativeEnd = Math.min(minRelativeEnd, match.index);
  }

  return {
    start,
    end: start + startMatch[0].length + minRelativeEnd,
  };
}

function findQualifiedIdentifierRangeInSlice(
  sql: string,
  qualifier: string,
  identifier: string,
  start: number,
  end: number,
) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(
    `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(identifier)}\\b`,
    "iu",
  );
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function findIdentifierRangeInSlice(sql: string, identifier: string, start: number, end: number) {
  const slice = sql.slice(start, end);
  const pattern = new RegExp(`\\b${escapeRegExp(identifier)}\\b`, "iu");
  const match = pattern.exec(slice);
  if (!match || match.index == null) {
    return undefined;
  }
  const absoluteStart = start + match.index;
  return rangeFromIndex(sql, absoluteStart, absoluteStart + match[0].length);
}

function readAliasesForSql(sqlText: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const aliasRegex = /\b(?:from|join)\s+([a-z_][\w]*)\s*(?:as\s+)?([a-z_][\w]*)?/gi;

  let match = aliasRegex.exec(sqlText);
  while (match) {
    const tableName = match[1];
    if (tableName) {
      const alias = (match[2] ?? tableName).toLowerCase();
      aliases.set(alias, tableName);
    }
    match = aliasRegex.exec(sqlText);
  }

  return aliases;
}

function findStringLiteralRange(
  sql: string,
  value: string,
): ReturnType<typeof rangeFromIndex> | undefined {
  const escapedValue = value.replaceAll("'", "''");
  const pattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  const match = pattern.exec(sql);
  if (!match || match.index == null) {
    return undefined;
  }
  return rangeFromIndex(sql, match.index, match.index + match[0].length);
}

function findEnumLiteralRange(
  sql: string,
  tableName: string,
  columnName: string,
  enumValue: string,
): ReturnType<typeof rangeFromIndex> | undefined {
  const aliases = readAliasesForSql(sql);
  const qualifiers = new Set<string>([tableName.toLowerCase()]);
  for (const [alias, resolvedTable] of aliases.entries()) {
    if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
      qualifiers.add(alias);
    }
  }

  const escapedValue = enumValue.replaceAll("'", "''");
  const literalPattern = new RegExp(`'${escapeRegExp(escapedValue)}'`, "gu");
  let literalMatch = literalPattern.exec(sql);
  while (literalMatch) {
    if (literalMatch.index == null) {
      literalMatch = literalPattern.exec(sql);
      continue;
    }

    const contextStart = Math.max(0, literalMatch.index - 180);
    const contextText = sql.slice(contextStart, literalMatch.index);
    const simpleColumnPattern = new RegExp(`\\b${escapeRegExp(columnName)}\\b`, "iu");
    const qualifiedPatterns = [...qualifiers].map(
      (qualifier) =>
        new RegExp(
          `\\b${escapeRegExp(qualifier)}\\b\\s*\\.\\s*\\b${escapeRegExp(columnName)}\\b`,
          "iu",
        ),
    );
    const hasColumnContext =
      simpleColumnPattern.test(contextText) ||
      qualifiedPatterns.some((pattern) => pattern.test(contextText));

    if (hasColumnContext) {
      return rangeFromIndex(sql, literalMatch.index, literalMatch.index + literalMatch[0].length);
    }

    literalMatch = literalPattern.exec(sql);
  }

  return findStringLiteralRange(sql, enumValue);
}

export function inferSqlErrorRange(sql: string, message: string) {
  const unsupportedFilterMatch =
    /^Filtering on\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedFilterMatch?.[1] && unsupportedFilterMatch?.[2]) {
    const tableName = unsupportedFilterMatch[1];
    const columnName = unsupportedFilterMatch[2];
    const aliases = readAliasesForSql(sql);
    const whereBounds = findClauseBounds(sql, "where");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (whereBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          whereBounds.start,
          whereBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(sql, columnName, whereBounds.start, whereBounds.end);
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const unsupportedSortMatch =
    /^Sorting by\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s+is not supported by schema policy\.?$/u.exec(
      message,
    );
  if (unsupportedSortMatch?.[1] && unsupportedSortMatch?.[2]) {
    const tableName = unsupportedSortMatch[1];
    const columnName = unsupportedSortMatch[2];
    const aliases = readAliasesForSql(sql);
    const orderByBounds = findClauseBounds(sql, "order_by");
    const qualifiers = new Set<string>([tableName]);
    for (const [alias, resolvedTable] of aliases.entries()) {
      if (resolvedTable.toLowerCase() === tableName.toLowerCase()) {
        qualifiers.add(alias);
      }
    }

    if (orderByBounds) {
      for (const qualifier of qualifiers) {
        const range = findQualifiedIdentifierRangeInSlice(
          sql,
          qualifier,
          columnName,
          orderByBounds.start,
          orderByBounds.end,
        );
        if (range) {
          return range;
        }
      }
      const range = findIdentifierRangeInSlice(
        sql,
        columnName,
        orderByBounds.start,
        orderByBounds.end,
      );
      if (range) {
        return range;
      }
    }

    for (const qualifier of qualifiers) {
      const range = findQualifiedIdentifierRange(sql, qualifier, columnName);
      if (range) {
        return range;
      }
    }
    return findIdentifierRange(sql, columnName);
  }

  const invalidEnumMatch =
    /^Invalid enum value for\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*):\s*(.+?)\.\s+Allowed values:/u.exec(
      message,
    );
  if (invalidEnumMatch?.[1] && invalidEnumMatch?.[2] && invalidEnumMatch?.[3]) {
    try {
      const parsedValue = JSON.parse(invalidEnumMatch[3]);
      if (typeof parsedValue === "string") {
        const literalRange = findEnumLiteralRange(
          sql,
          invalidEnumMatch[1],
          invalidEnumMatch[2],
          parsedValue,
        );
        if (literalRange) {
          return literalRange;
        }
      }
    } catch {
      // fall through to generic handling
    }

    return (
      findQualifiedIdentifierRange(sql, invalidEnumMatch[1], invalidEnumMatch[2]) ??
      findIdentifierRange(sql, invalidEnumMatch[2])
    );
  }

  const parserPositionMatch = /\(at position (\d+)\)\s*$/u.exec(message);
  if (parserPositionMatch?.[1]) {
    return findTokenRangeAtPosition(sql, Number.parseInt(parserPositionMatch[1], 10));
  }

  const unknownQualifiedColumnMatch =
    /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownQualifiedColumnMatch?.[1] && unknownQualifiedColumnMatch?.[2]) {
    return findQualifiedIdentifierRange(
      sql,
      unknownQualifiedColumnMatch[1],
      unknownQualifiedColumnMatch[2],
    );
  }

  const unknownColumnMatch = /^Unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownColumnMatch?.[1]) {
    return findIdentifierRange(sql, unknownColumnMatch[1]);
  }

  const unknownAliasMatch = /^Unknown table alias:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownAliasMatch?.[1]) {
    return findIdentifierRange(sql, unknownAliasMatch[1]);
  }

  const unknownTableMatch =
    /^Unknown table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^No table methods registered for table:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message) ??
    /^Table not found in schema:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (unknownTableMatch?.[1]) {
    return findIdentifierRange(sql, unknownTableMatch[1]);
  }

  const ambiguousColumnMatch =
    /^Ambiguous unqualified column reference:\s*([A-Za-z_][A-Za-z0-9_]*)/iu.exec(message);
  if (ambiguousColumnMatch?.[1]) {
    return findIdentifierRange(sql, ambiguousColumnMatch[1]);
  }

  return undefined;
}

export function findTableLineNumber(schemaText: string, tableName: string): number | null {
  const regex = new RegExp(
    `^\\s*(?:"${escapeRegExp(tableName)}"|${escapeRegExp(tableName)})\\s*:`,
    "mu",
  );
  const match = regex.exec(schemaText);
  const quoted = schemaText.indexOf(`"${tableName}"`);
  const unquoted = schemaText.indexOf(`${tableName}:`);
  const index = match?.index ?? (quoted >= 0 ? quoted : unquoted);

  if (index < 0) {
    return null;
  }

  return schemaText.slice(0, index).split("\n").length;
}
