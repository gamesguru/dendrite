package shared_test

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type historicalStateAPI struct {
	api.SyncRoomserverAPI
	event    *rstypes.HeaderedEvent
	requests []api.QueryEventsByIDRequest
}

func (a *historicalStateAPI) QueryEventsByID(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
	a.requests = append(a.requests, *req)
	res.Events = []*rstypes.HeaderedEvent{a.event}
	return nil
}

func (a *historicalStateAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	sender := spec.SenderID(userID.String())
	return &sender, nil
}

func TestHistoricalStateAfterReplacement(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, cleanup := newSyncDB(t, dbType)
		defer cleanup()
		ctx := t.Context()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		write := func(event *rstypes.HeaderedEvent, adds []*rstypes.HeaderedEvent, remove []string) types.StreamPosition {
			var ids []string
			for _, added := range adds {
				ids = append(ids, added.EventID())
			}
			pos, err := db.WriteEvent(ctx, event, adds, ids, remove, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
			return pos
		}
		var start types.StreamPosition
		for _, ev := range room.Events() {
			start = write(ev, []*rstypes.HeaderedEvent{ev}, nil)
		}
		old := room.CreateEvent(t, alice, "m.space.child", map[string]any{"via": []string{"example.com"}}, test.WithStateKey("!child:example.com"))
		// State received with a timeline event is stored only in current state.
		trigger := room.CreateEvent(t, alice, "m.room.message", map[string]string{"body": "state snapshot"})
		end := write(trigger, []*rstypes.HeaderedEvent{old}, nil)
		current := room.CreateEvent(t, alice, "m.space.child", map[string]any{}, test.WithStateKey("!child:example.com"))
		later := room.CreateEvent(t, alice, "m.room.message", map[string]string{"body": "new state"})
		latest := write(later, []*rstypes.HeaderedEvent{current}, []string{old.EventID()})
		rs := &historicalStateAPI{event: old}
		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, snapshot.Rollback()) }()
		device := &userapi.Device{UserID: alice.ID, ID: "test"}
		filter := synctypes.DefaultStateFilter()
		deltas, _, err := snapshot.GetStateDeltas(ctx, device, types.Range{From: start, To: end}, alice.ID, &filter, rs)
		require.NoError(t, err)
		require.Len(t, deltas, 1)
		require.Len(t, deltas[0].StateEvents, 1)
		require.Equal(t, old.EventID(), deltas[0].StateEvents[0].EventID())
		require.Len(t, rs.requests, 1)
		require.Equal(t, room.ID, rs.requests[0].RoomID)
		require.Equal(t, []string{old.EventID()}, rs.requests[0].EventIDs)
		require.True(t, rs.requests[0].ExcludeRejected)
		// A delta whose state is still local needs no roomserver fallback.
		currentDeltas, _, err := snapshot.GetStateDeltas(ctx, device, types.Range{From: end, To: latest}, alice.ID, &filter, rs)
		require.NoError(t, err)
		require.Len(t, currentDeltas, 1)
		require.Len(t, currentDeltas[0].StateEvents, 1)
		require.Equal(t, current.EventID(), currentDeltas[0].StateEvents[0].EventID())
		require.Len(t, rs.requests, 1)
		// Full-state sync uses the same recovery path while returning current
		// state for rooms the user is still joined to.
		full, _, err := snapshot.GetStateDeltasForFullStateSync(ctx, device, types.Range{From: start, To: end}, alice.ID, &filter, rs)
		require.NoError(t, err)
		require.Len(t, full, 1)
		require.Len(t, rs.requests, 2)
		// Recovery cannot rewind current state or create a new sync position.
		state, err := snapshot.GetStateEvent(ctx, room.ID, "m.space.child", "!child:example.com")
		require.NoError(t, err)
		require.Equal(t, current.EventID(), state.EventID())
		pos, err := snapshot.MaxStreamPositionForPDUs(ctx)
		require.NoError(t, err)
		require.Equal(t, latest, pos)
	})
}
