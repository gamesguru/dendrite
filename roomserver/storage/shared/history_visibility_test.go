package shared_test

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// storeHistoryEvent stores an event without involving the room event pipeline,
// keeping this test focused on the history visibility read path.
func storeHistoryEvent(
	ctx context.Context, t *testing.T, db storage.Database, roomInfo *types.RoomInfo, event gomatrixserverlib.PDU,
) types.EventNID {
	t.Helper()
	typeNID, err := db.GetOrCreateEventTypeNID(ctx, event.Type())
	require.NoError(t, err)
	keyNID, err := db.GetOrCreateEventStateKeyNID(ctx, event.StateKey())
	require.NoError(t, err)
	nid, _, err := db.StoreEvent(ctx, event, roomInfo, typeNID, keyNID, nil, false)
	require.NoError(t, err)
	return nid
}

// A single event without a persisted before-state snapshot used to fail the
// whole membership lookup, so the caller hid the entire history page. The
// other events in the page must still resolve, and only the event with no
// recoverable state falls back to the conservative leave default.
func TestMembershipForHistoryVisibilitySkipsEventWithoutState(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateFullDatabase(t, dbType) //nolint:contextcheck
		defer closeDB()

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		createEvent := room.Events()[0]
		require.Equal(t, spec.MRoomCreate, createEvent.Type())

		roomInfo, err := db.GetOrCreateRoomInfo(ctx, createEvent.PDU)
		require.NoError(t, err)

		joinEvent := room.CreateEvent(t, alice, "m.room.member", map[string]any{
			"membership":  "join",
			"displayname": "Alice",
		}, test.WithStateKey(alice.ID))
		joinNID := storeHistoryEvent(ctx, t, db, roomInfo, joinEvent.PDU)
		aliceNID, err := db.GetOrCreateEventStateKeyNID(ctx, joinEvent.StateKey())
		require.NoError(t, err)

		// The snapshot carries Alice's join membership for events that have
		// their state persisted.
		stateNID, err := db.AddState(ctx, roomInfo.RoomNID, []types.StateEntry{{
			EventTypeNID:     types.MRoomMemberNID,
			EventStateKeyNID: aliceNID,
			EventNID:         joinNID,
		}})
		require.NoError(t, err)

		withState := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "with state"})
		withoutState := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "without state"})
		withStateNID := storeHistoryEvent(ctx, t, db, roomInfo, withState.PDU)
		withoutStateNID := storeHistoryEvent(ctx, t, db, roomInfo, withoutState.PDU)
		require.NoError(t, db.SetState(ctx, withStateNID, stateNID))
		require.NoError(t, db.SetState(ctx, withoutStateNID, 0))

		got, err := db.GetMembershipForHistoryVisibility(
			ctx, aliceNID, roomInfo, withState.EventID(), withoutState.EventID(),
		)
		require.NoError(t, err)
		require.Contains(t, got, withState.EventID())
		require.NotNil(t, got[withState.EventID()])
		membership, err := got[withState.EventID()].Membership()
		require.NoError(t, err)
		require.Equal(t, spec.Join, membership)
		require.NotContains(t, got, withoutState.EventID())
	})
}
