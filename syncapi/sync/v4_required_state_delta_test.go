// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func stateName(t *testing.T, ev *rstypes.HeaderedEvent) string {
	t.Helper()
	return gjson.GetBytes(ev.Content(), "name").String()
}

// TestMergeUniqueStateEvents_LastChangeInGapWins covers a slot that changed more
// than once while the room was out of view. The delta arrives chronologically,
// so keeping the first occurrence would ship the stale value - and because the
// next sync resumes above it, that value would never be corrected.
func TestMergeUniqueStateEvents_LastChangeInGapWins(t *testing.T) {
	t.Parallel()

	older := createMockStateStreamEvent("$old", "m.room.name", "", 60, `{"name":"old"}`).HeaderedEvent
	newer := createMockStateStreamEvent("$new", "m.room.name", "", 70, `{"name":"new"}`).HeaderedEvent

	merged := mergeUniqueStateEvents(nil, []*rstypes.HeaderedEvent{older, newer})

	require.Len(t, merged, 1, "both events occupy the same (type, state_key) slot")
	assert.Equal(t, "new", stateName(t, merged[0]),
		"the newest change in the gap is the one the client must end up with")
}

// TestMergeUniqueStateEvents_CurrentStateWinsOverDelta keeps the original
// intent: the current-state fetch is a point-in-time snapshot, so where the two
// fetches overlap (e.g. a $LAZY member who also changed) it stays authoritative.
func TestMergeUniqueStateEvents_CurrentStateWinsOverDelta(t *testing.T) {
	t.Parallel()

	current := createMockStateStreamEvent("$current", "m.room.name", "", 90, `{"name":"current"}`).HeaderedEvent
	stale := createMockStateStreamEvent("$stale", "m.room.name", "", 60, `{"name":"stale"}`).HeaderedEvent

	merged := mergeUniqueStateEvents([]*rstypes.HeaderedEvent{current}, []*rstypes.HeaderedEvent{stale})

	require.Len(t, merged, 1)
	assert.Equal(t, "current", stateName(t, merged[0]))
}

// TestMergeUniqueStateEvents_DistinctSlotsAreAllKept guards the dedup from
// collapsing unrelated events: different types, and different state keys within
// a type, are separate slots.
func TestMergeUniqueStateEvents_DistinctSlotsAreAllKept(t *testing.T) {
	t.Parallel()

	name := createMockStateStreamEvent("$name", "m.room.name", "", 60, `{"name":"n"}`).HeaderedEvent
	alice := createMockStateStreamEvent("$alice", "m.room.member", "@alice:localhost", 61, `{"membership":"join"}`).HeaderedEvent
	bob := createMockStateStreamEvent("$bob", "m.room.member", "@bob:localhost", 62, `{"membership":"join"}`).HeaderedEvent

	merged := mergeUniqueStateEvents(nil, []*rstypes.HeaderedEvent{name, alice, bob})

	assert.Len(t, merged, 3)
}

func TestRequiredStateTypes(t *testing.T) {
	t.Parallel()

	t.Run("collects the types the config can match", func(t *testing.T) {
		t.Parallel()

		got := requiredStateTypes(&types.RequiredStateConfig{
			Include: [][]string{{"m.room.power_levels", ""}, {"m.room.member", "$LAZY"}},
		})

		require.NotNil(t, got)
		assert.ElementsMatch(t, []string{"m.room.power_levels", "m.room.member"}, *got)
	})

	t.Run("a wildcard type means no filter is possible", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, requiredStateTypes(&types.RequiredStateConfig{
			Include: [][]string{{"m.room.name", ""}, {"*", "*"}},
		}), "a wildcard matches anything, so filtering would drop events that qualify")
	})

	t.Run("no usable patterns means no filter", func(t *testing.T) {
		t.Parallel()

		assert.Nil(t, requiredStateTypes(&types.RequiredStateConfig{
			Include: [][]string{{"malformed"}},
		}))
	})
}

// TestGetRequiredState_TruncatedDeltaFallsBackToCurrentState covers the scan
// limit. The query keeps the newest events in the range, so a delta that hits
// the cap is missing the OLDEST changes in the gap - precisely the ones the
// delta exists to recover. Current state is complete, so it is used instead.
func TestGetRequiredState_TruncatedDeltaFallsBackToCurrentState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!busy:localhost"
	userID := "@alice:localhost"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	snapshot := newMockSnapshot()

	// One more matching change than a single scan can return.
	events := make([]types.StreamEvent, 0, requiredStateDeltaLimit+1)
	for i := 0; i <= requiredStateDeltaLimit; i++ {
		events = append(events, createMockStateStreamEvent(
			fmt.Sprintf("$member%d", i), "m.room.member", fmt.Sprintf("@user%d:localhost", i),
			types.StreamPosition(60+i), `{"membership":"join"}`))
	}
	snapshot.SetRecentEvents(roomID, types.RecentEvents{Events: events})

	// Current state holds a name event that appears in no delta scan at all, so
	// seeing it in the result proves the fallback ran.
	snapshot.SetStateEvent(roomID, "m.room.name", "",
		createMockStateStreamEvent("$currentname", "m.room.name", "", 5, `{"name":"from current state"}`).HeaderedEvent)

	config := &types.RequiredStateConfig{
		Include: [][]string{{"m.room.name", ""}},
	}
	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomPreviously,
		LastToken: &types.StreamingToken{PDUPosition: 50},
	}

	requiredState, err := rp.getRequiredState(
		ctx, snapshot, roomID, userID, config,
		[]synctypes.ClientEvent{}, false, roomState,
		types.StreamingToken{PDUPosition: 5000},
	)
	require.NoError(t, err)

	require.Len(t, requiredState, 1)
	assert.Equal(t, "$currentname", requiredState[0].EventID,
		"a truncated delta must be replaced by current state, not shipped with holes in it")
}
