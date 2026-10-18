//go:build !wasm

package fulltext

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2"
)

// ErrRecovering reports that the index is still being rebuilt, so a search
// would miss history rather than fail visibly.
var ErrRecovering = errors.New("the search index is still being rebuilt")

// Recovering lets live consumers write to an index while it is rebuilt behind
// them, and refuses searches until Finish.
//
// A rebuild writes documents it read earlier, so it can restore one that a
// consumer has deleted since, for a redaction or an edit. Every deletion made
// while recovering is therefore replayed by Finish, after the rebuild's last
// write and before any search can see the index. Event IDs are never reused,
// so a document deleted once never has to come back.
type Recovering struct {
	Indexer
	mu      sync.Mutex
	deleted map[string]struct{}
	done    atomic.Bool
}

// NewRecovering wraps index, which the rebuild itself writes to directly.
func NewRecovering(index Indexer) *Recovering {
	return &Recovering{Indexer: index, deleted: make(map[string]struct{})}
}

// Delete removes a document now and, while recovering, again in Finish.
func (r *Recovering) Delete(eventID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleted != nil {
		r.deleted[eventID] = struct{}{}
	}
	return r.Indexer.Delete(eventID)
}

// Search refuses with ErrRecovering until Finish has succeeded.
func (r *Recovering) Search(term string, roomIDs, keys []string, limit, from int, orderByStreamPos bool) (*bleve.SearchResult, error) {
	if !r.done.Load() {
		return nil, ErrRecovering
	}
	return r.Indexer.Search(term, roomIDs, keys, limit, from, orderByStreamPos)
}

// Finish replays the deletions made while recovering and admits searches.
// Call it once the rebuild has written its last document. After a failure it
// keeps every deletion, so calling it again after another rebuild is safe.
func (r *Recovering) Finish() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for eventID := range r.deleted {
		if err := r.Indexer.Delete(eventID); err != nil {
			return err
		}
	}
	r.deleted = nil
	r.done.Store(true)
	return nil
}

// Recovered reports whether searches are admitted.
func (r *Recovering) Recovered() bool { return r.done.Load() }
