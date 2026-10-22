// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlutil

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// SQLite is opened with a write-ahead log and synchronous=NORMAL when asked,
// whichever driver the binary was built with. Without the option the journal
// mode is left alone.
func TestOpenSQLiteWAL(t *testing.T) {
	const synchronousNormal = 1
	for name, tc := range map[string]struct {
		wal         bool
		wantJournal string
	}{
		"default":    {wal: false, wantJournal: "delete"},
		"sqlite_wal": {wal: true, wantJournal: "wal"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			db, err := Open(&config.DatabaseOptions{
				ConnectionString: config.DataSource("file:" + path),
				SQLiteWAL:        tc.wal,
			}, NewExclusiveWriter())
			require.NoError(t, err)
			defer db.Close()

			var journal string
			require.NoError(t, db.QueryRow("PRAGMA journal_mode").Scan(&journal))
			assert.Equal(t, tc.wantJournal, journal)

			if tc.wal {
				var synchronous int
				require.NoError(t, db.QueryRow("PRAGMA synchronous").Scan(&synchronous))
				assert.Equal(t, synchronousNormal, synchronous)
			}
		})
	}
}
