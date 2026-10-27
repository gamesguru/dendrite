//go:build cgo

package sqlutil

import (
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const SQLITE_DRIVER_NAME = "sqlite3"

// sqliteDSNExtension appends a 10,000-millisecond busy timeout to the DSN.
// When wal is true, it also requests WAL journal mode and NORMAL synchronization.
func sqliteDSNExtension(dsn string, wal bool) string {
	if strings.Contains(dsn, "?") {
		dsn += "&"
	} else {
		dsn += "?"
	}
	dsn += "_busy_timeout=10000"
	if wal {
		dsn += "&_journal_mode=WAL&_synchronous=NORMAL"
	}
	return dsn
}
