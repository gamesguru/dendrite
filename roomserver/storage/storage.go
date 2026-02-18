// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm

package storage

import (
	"context"
	"fmt"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// Open opens a database connection.
func Open(ctx context.Context, conMan *sqlutil.Connections, dbProperties *config.DatabaseOptions, cache caching.RoomServerCaches) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.Open(ctx, conMan, dbProperties, cache)
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.Open(ctx, conMan, dbProperties, cache)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
