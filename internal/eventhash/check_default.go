//go:build !(rezzy && linux && cgo)

package eventhash

import "github.com/gamesguru/gomatrixserverlib"

// Enabled reports whether this build links rezzy.
const Enabled = false

// StateEntry is one (event type, state key, event ID) row of a room state.
type StateEntry struct {
	Type     string
	StateKey string
	EventID  string
}

// Check does nothing in builds without rezzy.
func Check(gomatrixserverlib.PDU) error { return nil }

// CheckContentHash does nothing in builds without rezzy.
func CheckContentHash(gomatrixserverlib.PDU) error { return nil }

// CheckStateHAMT does nothing in builds without rezzy.
func CheckStateHAMT(string, []StateEntry) error { return nil }
