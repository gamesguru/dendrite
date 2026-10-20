// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustCreateErasedUsersTable(t *testing.T, dbType test.DBType) (tab tables.ErasedUsers, close func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	require.NoError(t, err)
	switch dbType {
	case test.DBTypePostgres:
		require.NoError(t, postgres.CreateErasedUsersTable(db))
		tab, err = postgres.PrepareErasedUsersTable(db)
	case test.DBTypeSQLite:
		require.NoError(t, sqlite3.CreateErasedUsersTable(db))
		tab, err = sqlite3.PrepareErasedUsersTable(db)
	}
	require.NoError(t, err)
	return tab, close
}

func TestErasedUsersTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, close := mustCreateErasedUsersTable(t, dbType)
		defer close()

		erased, err := tab.SelectErasedUsers(ctx, nil, []string{"@alice:test"})
		require.NoError(t, err)
		assert.Empty(t, erased)

		require.NoError(t, tab.InsertErasedUser(ctx, nil, "@alice:test", 1))
		// Erasing twice is not an error.
		require.NoError(t, tab.InsertErasedUser(ctx, nil, "@alice:test", 2))
		require.NoError(t, tab.InsertErasedUser(ctx, nil, "@bob:test", 3))

		erased, err = tab.SelectErasedUsers(ctx, nil, []string{"@alice:test", "@charlie:test"})
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"@alice:test": {}}, erased)

		// More users than SQLite binds in one statement.
		many := make([]string, 0, 1500)
		for i := range 1500 {
			many = append(many, fmt.Sprintf("@user%d:test", i))
		}
		many = append(many, "@bob:test")
		erased, err = tab.SelectErasedUsers(ctx, nil, many)
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"@bob:test": {}}, erased)

		require.NoError(t, tab.DeleteErasedUser(ctx, nil, "@alice:test"))
		erased, err = tab.SelectErasedUsers(ctx, nil, []string{"@alice:test", "@bob:test"})
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{"@bob:test": {}}, erased)
	})
}
