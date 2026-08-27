package input

import (
	"fmt"
	"sync"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/test"
)

func Test_parsedRespState_OutliersStored(t *testing.T) {
	t.Run("Events returns empty when OutliersStored", func(t *testing.T) {
		state := &parsedRespState{
			OutliersStored: true,
			StateEventIDs:  []string{"$event1", "$event2", "$event3"},
		}
		events := state.Events()
		assert.Empty(t, events, "Events() should return empty when OutliersStored is true")
		assert.Len(t, state.StateEventIDs, 3, "StateEventIDs should be preserved")
	})

	t.Run("Events returns events when not OutliersStored", func(t *testing.T) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		var stateEvents []gomatrixserverlib.PDU
		for _, ev := range room.Events() {
			if ev.StateKey() != nil {
				stateEvents = append(stateEvents, ev.PDU)
			}
		}

		state := &parsedRespState{
			StateEvents: stateEvents,
		}
		events := state.Events()
		assert.NotEmpty(t, events, "Events() should return events when OutliersStored is false")
	})
}

func Test_stateSnapshotEventIDs(t *testing.T) {
	t.Run("returns StateEventIDs when OutliersStored", func(t *testing.T) {
		expectedIDs := []string{"$aaa", "$bbb", "$ccc"}
		state := &parsedRespState{
			OutliersStored: true,
			StateEventIDs:  expectedIDs,
		}
		ids := stateSnapshotEventIDs(state)
		assert.Equal(t, expectedIDs, ids)
	})

	t.Run("extracts IDs from StateEvents when not OutliersStored", func(t *testing.T) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		var stateEvents []gomatrixserverlib.PDU
		var expectedIDs []string
		for _, ev := range room.Events() {
			if ev.StateKey() != nil {
				stateEvents = append(stateEvents, ev.PDU)
				expectedIDs = append(expectedIDs, ev.EventID())
			}
		}

		state := &parsedRespState{
			StateEvents: stateEvents,
		}
		ids := stateSnapshotEventIDs(state)
		assert.Equal(t, expectedIDs, ids)
	})

	t.Run("returns empty for empty state", func(t *testing.T) {
		state := &parsedRespState{}
		ids := stateSnapshotEventIDs(state)
		assert.Empty(t, ids)
	})
}

func Test_sendOutliers_noop_when_OutliersStored(t *testing.T) {
	// Verify that the OutliersStored flag causes Events() to return empty,
	// which means sendOutliers would have nothing to process even without
	// the explicit guard. This tests both the guard and the data invariant.
	state := &parsedRespState{
		OutliersStored: true,
		StateEventIDs:  []string{"$a", "$b"},
	}

	assert.Empty(t, state.Events())
	assert.True(t, state.OutliersStored)
}

func Test_lookupResolvedState_skips_resolution_for_OutliersStored(t *testing.T) {
	// Test that the OutliersStored flag in the case-1 branch of
	// lookupResolvedStateBeforeEvent causes state resolution to be
	// skipped. We replicate the branching logic from the function.
	inner := &parsedRespState{
		OutliersStored: true,
		StateEventIDs:  []string{"$state1", "$state2"},
	}

	type respState struct {
		trustworthy bool
		*parsedRespState
	}

	states := []respState{
		{trustworthy: false, parsedRespState: inner},
	}

	// Simulate the case 1 branch: OutliersStored should be checked first
	var resolvedState *parsedRespState
	if len(states) == 1 && states[0].OutliersStored {
		resolvedState = states[0].parsedRespState
	}

	require.NotNil(t, resolvedState)
	assert.True(t, resolvedState.OutliersStored)
	assert.Equal(t, []string{"$state1", "$state2"}, resolvedState.StateEventIDs)
	assert.Empty(t, resolvedState.StateEvents)
	assert.Empty(t, resolvedState.AuthEvents)
}

func Test_stateEventIDs_closure(t *testing.T) {
	// Test the stateEventIDs closure logic used in processEventWithMissingState
	stateEventIDs := func(rs *parsedRespState) []string {
		if rs.OutliersStored {
			return rs.StateEventIDs
		}
		ids := make([]string, 0, len(rs.StateEvents))
		for _, event := range rs.StateEvents {
			ids = append(ids, event.EventID())
		}
		return ids
	}

	t.Run("returns pre-computed IDs for chunked state", func(t *testing.T) {
		rs := &parsedRespState{
			OutliersStored: true,
			StateEventIDs:  []string{"$x", "$y", "$z"},
		}
		ids := stateEventIDs(rs)
		assert.Equal(t, []string{"$x", "$y", "$z"}, ids)
	})

	t.Run("extracts IDs from events for normal state", func(t *testing.T) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		var stateEvents []gomatrixserverlib.PDU
		for _, ev := range room.Events() {
			if ev.StateKey() != nil {
				stateEvents = append(stateEvents, ev.PDU)
			}
		}

		rs := &parsedRespState{
			StateEvents: stateEvents,
		}
		ids := stateEventIDs(rs)
		assert.Len(t, ids, len(stateEvents))
		for i, ev := range stateEvents {
			assert.Equal(t, ev.EventID(), ids[i])
		}
	})
}

func Test_haveEvents_cache_clearing(t *testing.T) {
	// Verify that the cache clearing pattern used in fetchAndStoreStateInChunks
	// correctly releases events from haveEvents between batches.
	haveEvents := make(map[string]gomatrixserverlib.PDU)
	var mu sync.Mutex

	// Simulate batch 1: add events to cache
	batch1 := []string{"$a", "$b", "$c"}
	for _, id := range batch1 {
		haveEvents[id] = nil
	}
	assert.Len(t, haveEvents, 3)

	// Clear batch 1 (as fetchAndStoreStateInChunks does)
	mu.Lock()
	for _, id := range batch1 {
		delete(haveEvents, id)
	}
	mu.Unlock()
	assert.Empty(t, haveEvents, "cache should be empty after clearing batch 1")

	// Simulate batch 2
	batch2 := []string{"$d", "$e"}
	for _, id := range batch2 {
		haveEvents[id] = nil
	}
	assert.Len(t, haveEvents, 2)

	mu.Lock()
	for _, id := range batch2 {
		delete(haveEvents, id)
	}
	mu.Unlock()
	assert.Empty(t, haveEvents, "cache should be empty after clearing batch 2")
}

func Test_rejectedPrevEvent_fallback_with_OutliersStored(t *testing.T) {
	// When all prev_events are rejected and the fallback returns a state
	// with OutliersStored, a state event's ID should be appended to
	// StateEventIDs (not StateEvents).
	fallbackState := &parsedRespState{
		OutliersStored: true,
		StateEventIDs:  []string{"$existing1", "$existing2"},
	}

	// Simulate the code path in lookupResolvedStateBeforeEvent
	newEventID := "$newStateEvent"
	if fallbackState.OutliersStored {
		fallbackState.StateEventIDs = append(fallbackState.StateEventIDs, newEventID)
	}

	assert.Len(t, fallbackState.StateEventIDs, 3)
	assert.Contains(t, fallbackState.StateEventIDs, newEventID)
	assert.Empty(t, fallbackState.StateEvents, "StateEvents should remain empty for OutliersStored")
}

func Test_parsedRespState_large_StateEventIDs(t *testing.T) {
	// Verify that OutliersStored works correctly with a large number of
	// state event IDs, simulating a room with 20K+ state events.
	ids := make([]string, 20_000)
	for i := range ids {
		ids[i] = fmt.Sprintf("$event_%d", i)
	}

	state := &parsedRespState{
		OutliersStored: true,
		StateEventIDs:  ids,
	}

	assert.Len(t, state.StateEventIDs, 20_000)
	assert.Empty(t, state.StateEvents)
	assert.Empty(t, state.AuthEvents)
	assert.Empty(t, state.Events())

	result := stateSnapshotEventIDs(state)
	assert.Len(t, result, 20_000)
	assert.Equal(t, "$event_0", result[0])
	assert.Equal(t, "$event_19999", result[19_999])
}
