// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.
package input

import (
	"testing"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

func memberEntry(stateKey types.EventStateKeyNID, eventNID types.EventNID) types.StateEntry {
	return types.StateEntry{
		StateKeyTuple: types.StateKeyTuple{EventTypeNID: types.MRoomMemberNID, EventStateKeyNID: stateKey},
		EventNID:      eventNID,
	}
}

func nonMemberEntry(typeNID types.EventTypeNID, eventNID types.EventNID) types.StateEntry {
	return types.StateEntry{
		StateKeyTuple: types.StateKeyTuple{EventTypeNID: typeNID, EventStateKeyNID: 1},
		EventNID:      eventNID,
	}
}

func memberNIDForKey(entries []types.StateEntry, stateKey types.EventStateKeyNID) types.EventNID {
	for _, e := range entries {
		if e.EventTypeNID == types.MRoomMemberNID && e.EventStateKeyNID == stateKey {
			return e.EventNID
		}
	}
	return 0
}

const (
	localKey  types.EventStateKeyNID = 65536
	remoteKey types.EventStateKeyNID = 170655
)

func TestReconcileResyncMembers(t *testing.T) {
	t.Run("local user invite is overridden by our join", func(t *testing.T) {
		// Remote /state predates our join: it carries our invite (NID 10).
		// Our old state has our actual join (NID 20).
		remote := []types.StateEntry{
			nonMemberEntry(1, 1),       // create
			memberEntry(localKey, 10),  // our pre-join invite (stale)
			memberEntry(remoteKey, 30), // remote creator join
		}
		old := []types.StateEntry{
			memberEntry(localKey, 20),  // our join
			memberEntry(remoteKey, 30), // remote creator join (same)
		}
		localNIDs := map[types.EventNID]bool{20: true}

		merged, kept := reconcileResyncMembers(remote, old, localNIDs)
		if got := memberNIDForKey(merged, localKey); got != 20 {
			t.Fatalf("local member should be our join (20), got %d", got)
		}
		if got := memberNIDForKey(merged, remoteKey); got != 30 {
			t.Fatalf("remote member should be unchanged (30), got %d", got)
		}
		if kept != 1 {
			t.Fatalf("expected 1 kept local member, got %d", kept)
		}
	})

	t.Run("non-local overlap is not overridden", func(t *testing.T) {
		remote := []types.StateEntry{memberEntry(remoteKey, 30)}
		old := []types.StateEntry{memberEntry(remoteKey, 40)}
		// 40 is not flagged local, so the remote state must win.
		merged, kept := reconcileResyncMembers(remote, old, map[types.EventNID]bool{})
		if got := memberNIDForKey(merged, remoteKey); got != 30 {
			t.Fatalf("remote member should keep remote state (30), got %d", got)
		}
		if kept != 0 {
			t.Fatalf("expected 0 kept, got %d", kept)
		}
	})

	t.Run("member absent from remote state is preserved", func(t *testing.T) {
		remote := []types.StateEntry{nonMemberEntry(1, 1)}
		old := []types.StateEntry{memberEntry(localKey, 20)}
		merged, kept := reconcileResyncMembers(remote, old, map[types.EventNID]bool{})
		if got := memberNIDForKey(merged, localKey); got != 20 {
			t.Fatalf("absent member should be preserved (20), got %d", got)
		}
		if kept != 1 {
			t.Fatalf("expected 1 kept, got %d", kept)
		}
	})
}

func TestOverlappingOldMembers(t *testing.T) {
	remote := []types.StateEntry{
		memberEntry(localKey, 10),
		memberEntry(remoteKey, 30),
	}
	old := []types.StateEntry{
		memberEntry(localKey, 20),  // differs from remote (10) -> overlap
		memberEntry(remoteKey, 30), // same as remote -> not an overlap
		nonMemberEntry(1, 1),       // non-member -> ignored
	}
	overlap := overlappingOldMembers(remote, old)
	if len(overlap) != 1 {
		t.Fatalf("expected 1 overlapping member, got %d", len(overlap))
	}
	if overlap[0].EventStateKeyNID != localKey || overlap[0].EventNID != 20 {
		t.Fatalf("unexpected overlap entry: %+v", overlap[0])
	}
}
