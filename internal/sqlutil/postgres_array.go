// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlutil

import (
	"database/sql/driver"
	"strconv"
	"strings"
)

// Int64Array is a slice of int64 that implements sql.Scanner and driver.Valuer
// for PostgreSQL int8[] (bigint[]) columns.
type Int64Array []int64

// Scan implements the sql.Scanner interface.
func (a *Int64Array) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var str string
	switch v := src.(type) {
	case []byte:
		str = string(v)
	case string:
		str = v
	default:
		*a = nil
		return nil
	}

	// Handle empty array
	if str == "{}" {
		*a = Int64Array{}
		return nil
	}

	// Remove braces and split
	str = strings.TrimPrefix(str, "{")
	str = strings.TrimSuffix(str, "}")

	if str == "" {
		*a = Int64Array{}
		return nil
	}

	parts := strings.Split(str, ",")
	result := make(Int64Array, len(parts))
	for i, p := range parts {
		n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return err
		}
		result[i] = n
	}
	*a = result
	return nil
}

// Value implements the driver.Valuer interface.
func (a Int64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	if len(a) == 0 {
		return "{}", nil
	}

	parts := make([]string, len(a))
	for i, n := range a {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return "{" + strings.Join(parts, ",") + "}", nil
}

// StringArray is a slice of string that implements sql.Scanner and driver.Valuer
// for PostgreSQL text[] columns.
type StringArray []string

// Scan implements the sql.Scanner interface.
func (a *StringArray) Scan(src interface{}) error {
	if src == nil {
		*a = nil
		return nil
	}

	var str string
	switch v := src.(type) {
	case []byte:
		str = string(v)
	case string:
		str = v
	default:
		*a = nil
		return nil
	}

	// Handle empty array
	if str == "{}" {
		*a = StringArray{}
		return nil
	}

	// Parse PostgreSQL array format
	*a = parsePostgresStringArray(str)
	return nil
}

// Value implements the driver.Valuer interface.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}
	if len(a) == 0 {
		return "{}", nil
	}

	parts := make([]string, len(a))
	for i, s := range a {
		// Escape backslashes and double quotes
		escaped := strings.ReplaceAll(s, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		parts[i] = `"` + escaped + `"`
	}
	return "{" + strings.Join(parts, ",") + "}", nil
}

// parsePostgresStringArray parses a PostgreSQL text array literal.
func parsePostgresStringArray(s string) []string {
	// Remove outer braces
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")

	if s == "" {
		return []string{}
	}

	var result []string
	var current strings.Builder
	inQuotes := false
	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			current.WriteByte(c)
			escaped = false
			continue
		}

		if c == '\\' {
			escaped = true
			continue
		}

		if c == '"' {
			inQuotes = !inQuotes
			continue
		}

		if c == ',' && !inQuotes {
			result = append(result, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// Add the last element
	result = append(result, current.String())

	return result
}
