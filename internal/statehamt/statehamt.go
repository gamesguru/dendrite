// Package statehamt adapts rezzy's persisted state-HAMT records to the
// roomserver. It is a thin, build-tagged wrapper: the rezzy build calls the
// native library, every other build reports Enabled=false and the roomserver
// keeps using state blocks.
package statehamt

import "errors"

// ErrDisabled is returned when the build does not link the native rezzy
// library, so no HAMT persistence is available.
var ErrDisabled = errors.New("statehamt: native rezzy library is not linked")

// Entry is one state leaf: event type, state key, and event ID.
type Entry struct {
	Type     string
	StateKey string
	EventID  string
}

// Key is one state key to look up in a persisted HAMT.
type Key struct {
	Type     string
	StateKey string
}

// Node is one persisted MTHN record.
type Node struct {
	Hash  []byte
	Bytes []byte
}

// Persisted is the persisted form of a state HAMT: identity, the MTHR root
// record, and every MTHN node reachable from it.
type Persisted struct {
	RootHash     []byte
	StateGroupID []byte
	RootRecord   []byte
	Nodes        []Node
	EntryCount   int
	NodeBytes    int
}

// Verification is the result of verifying a root record and node set.
type Verification struct {
	RootHash     []byte
	StateGroupID []byte
	RoomID       string
	Prefix       []byte
	EntryCount   int
}

// MissingNodesError reports node hashes needed to complete a HAMT read.
type MissingNodesError struct {
	Hashes [][]byte
}

func (e *MissingNodesError) Error() string {
	return "statehamt: state HAMT is missing nodes"
}
