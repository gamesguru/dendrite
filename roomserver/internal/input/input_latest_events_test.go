// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.
package input

import (
	"testing"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

func memberKey(stateKey types.EventStateKeyNID) types.StateKeyTuple {
	return types.StateKeyTuple{EventTypeNID: types.MRoomMemberNID, EventStateKeyNID: stateKey}
}

func keysOf(entries []types.StateEntry) []types.StateKeyTuple {
	keys := make([]types.StateKeyTuple, len(entries))
	for i, e := range entries {
		keys[i] = e.StateKeyTuple
	}
	return keys
}

// A remote kick of the last local member arrives out-of-order after a
// partial-state resync. The delta drops the kicked user's own membership AND
// collaterally drops a remote member that the resync authoritatively fetched.
// The kicked user's own transition must be applied (so the membership row
// updates), while the collateral member is preserved (the resync protection).
func TestReconcileStateEpoch_AppliesKickOwnKeyKeepsCollateral(t *testing.T) {
	const kicked types.EventStateKeyNID = 7
	const collateral types.EventStateKeyNID = 9

	removed := []types.StateEntry{
		memberEntry(kicked, 100),     // kicked user's old join
		memberEntry(collateral, 101), // resync-authoritative remote member
	}
	added := []types.StateEntry{
		memberEntry(kicked, 200), // the kick (leave) event
	}

	keepRemoved, keepAdded := reconcileStateEpoch(memberKey(kicked), true, removed, added)

	if got := keysOf(keepRemoved); len(got) != 1 || got[0] != memberKey(kicked) {
		t.Fatalf("keepRemoved = %v, want only the kicked user's own membership", got)
	}
	if got := keysOf(keepAdded); len(got) != 1 || got[0] != memberKey(kicked) {
		t.Fatalf("keepAdded = %v, want only the kicked user's own membership", got)
	}
	if memberNIDForKey(keepRemoved, collateral) != 0 {
		t.Fatalf("collateral member must not be removed (resync state must be preserved)")
	}
}

// removesCollateralMembers is the NID-independent trigger for the resync epoch
// guard: it must fire only when a member event for someone OTHER than the
// triggering event's own state key is being dropped.
func TestRemovesCollateralMembers(t *testing.T) {
	const own types.EventStateKeyNID = 7
	const other types.EventStateKeyNID = 9

	t.Run("collateral member drop is detected", func(t *testing.T) {
		removed := []types.StateEntry{memberEntry(own, 100), memberEntry(other, 101)}
		if !removesCollateralMembers(memberKey(own), removed) {
			t.Fatal("expected collateral member removal to be detected")
		}
	})

	t.Run("only own membership removed is not collateral", func(t *testing.T) {
		// A user's own leave/kick removes only their own membership row.
		removed := []types.StateEntry{memberEntry(own, 100)}
		if removesCollateralMembers(memberKey(own), removed) {
			t.Fatal("dropping only the event's own membership must not count as collateral")
		}
	})

	t.Run("non-member state churn is not collateral", func(t *testing.T) {
		// e.g. a topic/name/power-level replacement, no membership involved.
		removed := []types.StateEntry{nonMemberEntry(2, 100), nonMemberEntry(3, 101)}
		if removesCollateralMembers(memberKey(own), removed) {
			t.Fatal("non-member removals must not count as collateral member drop")
		}
	})

	t.Run("non-state triggering event with member drop is collateral", func(t *testing.T) {
		// A message event (zero-value own key) whose out-of-order base state would
		// evict members: every dropped member differs from the zero key.
		removed := []types.StateEntry{memberEntry(own, 100), memberEntry(other, 101)}
		if !removesCollateralMembers(types.StateKeyTuple{}, removed) {
			t.Fatal("member drops under a non-state triggering event must be collateral")
		}
	})

	t.Run("empty delta is not collateral", func(t *testing.T) {
		if removesCollateralMembers(memberKey(own), nil) {
			t.Fatal("empty removed set must not be collateral")
		}
	})
}

// A genuinely out-of-order non-state event whose stale base would collaterally
// drop membership must be fully suppressed (the original guard behavior).
func TestReconcileStateEpoch_NonStateEventFullySuppressed(t *testing.T) {
	removed := []types.StateEntry{memberEntry(7, 100), memberEntry(9, 101)}
	added := []types.StateEntry{}

	keepRemoved, keepAdded := reconcileStateEpoch(types.StateKeyTuple{}, false, removed, added)

	if len(keepRemoved) != 0 || len(keepAdded) != 0 {
		t.Fatalf("non-state event must be fully suppressed, got removed=%v added=%v", keepRemoved, keepAdded)
	}
}

// A state event that does not itself author any of the dropped keys is treated
// as a collateral regression and fully suppressed.
func TestReconcileStateEpoch_UnrelatedStateEventFullySuppressed(t *testing.T) {
	removed := []types.StateEntry{memberEntry(7, 100), memberEntry(9, 101)}
	added := []types.StateEntry{}

	keepRemoved, keepAdded := reconcileStateEpoch(memberKey(42), true, removed, added)

	if len(keepRemoved) != 0 || len(keepAdded) != 0 {
		t.Fatalf("unrelated state event must be fully suppressed, got removed=%v added=%v", keepRemoved, keepAdded)
	}
}
