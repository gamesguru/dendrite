//go:build !cgo

package sqlutil

import (
	"strings"
)

const SQLITE_DRIVER_NAME = "sqlite"

func sqliteDSNExtension(dsn string, wal bool) string {
	// add query parameters to the dsn
	if strings.Contains(dsn, "?") {
		dsn += "&"
	} else {
		dsn += "?"
	}

	// wait some time before erroring if the db is locked
	// https://gitlab.com/cznic/sqlite/-/issues/106#note_1058094993
	dsn += "_pragma=busy_timeout%3d10000"
	if wal {
		dsn += "&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	return dsn
}
