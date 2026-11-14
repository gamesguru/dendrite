//go:build rezzy && linux && cgo

package eventhash

import (
	"fmt"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/rezzy/rezzy-ffi/go/rezzy"
)

// Enabled reports whether this build links rezzy.
const Enabled = true

// StateEntry is one (event type, state key, event ID) row of a room state.
type StateEntry struct {
	Type     string
	StateKey string
	EventID  string
}

// Check returns an error when rezzy computes a different event ID, or cannot
// hash the event. Room versions 1 and 2 have server-assigned IDs that cannot
// be derived from the event, so they are skipped.
func Check(event gomatrixserverlib.PDU) error {
	switch event.Version() {
	case gomatrixserverlib.RoomVersionV1, gomatrixserverlib.RoomVersionV2:
		return nil
	}
	id, err := rezzy.EventID(string(event.Version()), event.JSON())
	if err != nil {
		return fmt.Errorf("rezzy could not hash event: %w", err)
	}
	if id != event.EventID() {
		return fmt.Errorf(
			"event ID mismatch for %s event (room version %s): gomatrixserverlib=%s rezzy=%s",
			event.Type(), event.Version(), event.EventID(), id,
		)
	}
	return nil
}

// CheckContentHash returns an error when rezzy cannot verify the event's
// content hash. Room versions 1 and 2 do not have content hashes.
func CheckContentHash(event gomatrixserverlib.PDU) error {
	switch event.Version() {
	case gomatrixserverlib.RoomVersionV1, gomatrixserverlib.RoomVersionV2:
		return nil
	}
	if err := rezzy.VerifyContentHash(string(event.Version()), event.JSON()); err != nil {
		return fmt.Errorf("rezzy could not verify content hash for %s event (room version %s): %w", event.Type(), event.Version(), err)
	}
	return nil
}

// maxStateHAMTLookups bounds the number of point lookups per check. Each
// lookup rebuilds the HAMT from the entries, so checking every key would make
// an event O(n^2) in the size of the room state.
const maxStateHAMTLookups = 3

// CheckStateHAMT builds a state HAMT from the entries and verifies that its
// root does not depend on entry order and that sample lookups return the event
// IDs that the roomserver has in the snapshot.
func CheckStateHAMT(roomID string, entries []StateEntry) error {
	hamtEntries := make([]rezzy.HAMTEntry, 0, len(entries))
	seen := make(map[[2]string]struct{}, len(entries))
	for _, entry := range entries {
		key := [2]string{entry.Type, entry.StateKey}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate state key (%s, %q) in roomserver state", entry.Type, entry.StateKey)
		}
		seen[key] = struct{}{}
		hamtEntries = append(hamtEntries, rezzy.HAMTEntry{entry.Type, entry.StateKey, entry.EventID})
	}

	root, stateGroupID, err := rezzy.StateHAMTRoot(roomID, hamtEntries)
	if err != nil {
		return fmt.Errorf("rezzy could not build state HAMT: %w", err)
	}
	if root == "" || stateGroupID == "" {
		return fmt.Errorf("rezzy returned an empty state HAMT root or state group ID")
	}

	reversed := make([]rezzy.HAMTEntry, len(hamtEntries))
	for i := range hamtEntries {
		reversed[len(hamtEntries)-1-i] = hamtEntries[i]
	}
	reverseRoot, reverseGroupID, err := rezzy.StateHAMTRoot(roomID, reversed)
	if err != nil {
		return fmt.Errorf("rezzy could not build reordered state HAMT: %w", err)
	}
	if root != reverseRoot || stateGroupID != reverseGroupID {
		return fmt.Errorf(
			"state HAMT depends on entry order: root %s then %s, state group %s then %s",
			root, reverseRoot, stateGroupID, reverseGroupID,
		)
	}

	if len(hamtEntries) == 0 {
		return nil
	}
	indexes := []int{0, len(hamtEntries) / 2, len(hamtEntries) - 1}
	checked := make(map[int]struct{}, len(indexes))
	for _, index := range indexes {
		if _, ok := checked[index]; ok {
			continue
		}
		checked[index] = struct{}{}
		entry := entries[index]
		eventID, ok, err := rezzy.StateHAMTLookup(roomID, hamtEntries, entry.Type, entry.StateKey)
		if err != nil {
			return fmt.Errorf("rezzy could not look up state key (%s, %q): %w", entry.Type, entry.StateKey, err)
		}
		if !ok {
			return fmt.Errorf("rezzy state HAMT is missing state key (%s, %q)", entry.Type, entry.StateKey)
		}
		if eventID != entry.EventID {
			return fmt.Errorf(
				"state HAMT lookup mismatch for (%s, %q): roomserver=%s rezzy=%s",
				entry.Type, entry.StateKey, entry.EventID, eventID,
			)
		}
	}
	return nil
}
