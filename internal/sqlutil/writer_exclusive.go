package sqlutil

import (
	"database/sql"
	"errors"
	"sync"
)

// ExclusiveWriter serializes database writes on the calling goroutines.
// This keeps writes inside their caller's application lifetime instead of
// leaving a permanently running queue goroutine behind after database closure.
type ExclusiveWriter struct {
	mu          sync.Mutex
	initialized bool
}

func NewExclusiveWriter() Writer {
	return &ExclusiveWriter{initialized: true}
}

// Do runs one task at a time, optionally within the supplied or a new transaction.
// Callers must not recursively invoke Do on the same writer.
func (w *ExclusiveWriter) Do(db *sql.DB, txn *sql.Tx, f func(txn *sql.Tx) error) error {
	if !w.initialized {
		return errors.New("not initialized")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case db != nil && txn != nil:
		return f(txn)
	case db != nil && txn == nil:
		return WithTransaction(db, f)
	default:
		return f(nil)
	}
}
