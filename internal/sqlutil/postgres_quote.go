// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlutil

import "strings"

// QuoteIdentifier quotes an identifier (e.g., table name, column name) for use
// in a PostgreSQL query. This prevents SQL injection and handles special characters
// properly.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral quotes a literal string value for use in a PostgreSQL query.
// This prevents SQL injection by properly escaping the string.
func QuoteLiteral(literal string) string {
	// Use dollar quoting if the literal contains single quotes or backslashes,
	// otherwise use standard single-quote escaping
	if strings.Contains(literal, "'") || strings.Contains(literal, `\`) {
		// Use E'' syntax for strings with special characters
		escaped := strings.ReplaceAll(literal, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, "'", "''")
		return "E'" + escaped + "'"
	}
	return "'" + literal + "'"
}
