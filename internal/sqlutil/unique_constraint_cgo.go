// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm && cgo

package sqlutil

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"
)

// IsUniqueConstraintViolationErr returns true if the error is an unique_violation error.
func IsUniqueConstraintViolationErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	var sqliteErr *sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrConstraint
	}
	var sqliteErrVal sqlite3.Error
	if errors.As(err, &sqliteErrVal) {
		return sqliteErrVal.Code == sqlite3.ErrConstraint
	}
	return false
}
