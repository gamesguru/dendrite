// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

//go:build !wasm

package storage

import (
	"fmt"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/relayapi/storage/postgres"
	"codefloe.com/pat-s/dendrite/relayapi/storage/sqlite3"
	"codefloe.com/pat-s/dendrite/setup/config"
)

// NewDatabase opens a new database.
func NewDatabase(
	conMan *sqlutil.Connections,
	dbProperties *config.DatabaseOptions,
	cache caching.FederationCache,
	isLocalServerName func(spec.ServerName) bool,
) (Database, error) {
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		return sqlite3.NewDatabase(conMan, dbProperties, cache, isLocalServerName)
	case dbProperties.ConnectionString.IsPostgres():
		return postgres.NewDatabase(conMan, dbProperties, cache, isLocalServerName)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
