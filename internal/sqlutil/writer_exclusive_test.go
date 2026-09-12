package sqlutil

import (
	"database/sql"
	"errors"
	"sync"
	"testing"
)

func TestExclusiveWriterSerializesCallers(t *testing.T) {
	writer := NewExclusiveWriter()
	var workers sync.WaitGroup
	count := 0
	for range 100 {
		workers.Go(func() {
			if err := writer.Do(nil, nil, func(*sql.Tx) error { count++; return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if count != 100 {
		t.Fatalf("lost writes: %d", count)
	}
	failure := errors.New("write failed")
	if err := writer.Do(nil, nil, func(*sql.Tx) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("lost error: %v", err)
	}
}
