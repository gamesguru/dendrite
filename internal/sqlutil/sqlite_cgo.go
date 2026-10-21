//go:build cgo

package sqlutil

import (
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const SQLITE_DRIVER_NAME = "sqlite3"

func sqliteDSNExtension(dsn string, wal bool) string {
	if !wal {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		dsn += "&"
	} else {
		dsn += "?"
	}
	return dsn + "_journal_mode=WAL&_synchronous=NORMAL"
}
