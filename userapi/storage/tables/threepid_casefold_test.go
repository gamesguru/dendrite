// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/userapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

// TestFoldEmailThreePIDs covers the migration that case-folds email addresses
// stored before MSC2265.
func TestFoldEmailThreePIDs(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		db, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, nil)
		require.NoError(t, err)
		switch dbType {
		case test.DBTypePostgres:
			_, err = postgres.NewPostgresThreePIDTable(ctx, db)
		case test.DBTypeSQLite:
			_, err = sqlite3.NewSQLiteThreePIDTable(ctx, db)
		}
		require.NoError(t, err)

		// Rows as an older server stored them.
		for _, row := range [][3]string{
			{"Strauß@Example.com", "email", "alice"},
			{"bob@example.com", "email", "bob"},
			{"Carol@Example.com", "email", "carol"},
			{"carol@example.com", "email", "carol2"},
			{"+44 Mixed", "msisdn", "dave"},
		} {
			_, err = db.ExecContext(ctx,
				"INSERT INTO userapi_threepids (threepid, medium, localpart, server_name) VALUES ($1, $2, $3, 'test')",
				row[0], row[1], row[2])
			require.NoError(t, err)
		}

		var txn *sql.Tx
		txn, err = db.Begin()
		require.NoError(t, err)
		require.NoError(t, tables.FoldEmailThreePIDs(ctx, txn))
		require.NoError(t, txn.Commit())

		got := map[string]string{}
		rows, err := db.QueryContext(ctx, "SELECT threepid, localpart FROM userapi_threepids")
		require.NoError(t, err)
		for rows.Next() {
			var address, localpart string
			require.NoError(t, rows.Scan(&address, &localpart))
			got[localpart] = address
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		assert.Equal(t, map[string]string{
			"alice": "strauss@example.com",
			"bob":   "bob@example.com",
			// The folded form belongs to another account, so this one stays.
			"carol":  "Carol@Example.com",
			"carol2": "carol@example.com",
			"dave":   "+44 Mixed",
		}, got)
	})
}
