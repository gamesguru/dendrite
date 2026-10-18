package streams

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// failoverSnapshot serves the start of an incremental sync for one room, then
// loses its database connection, as a sync does when the PostgreSQL primary
// fails while it builds the response.
type failoverSnapshot struct {
	storage.DatabaseTransaction
	roomID string
	events []*rstypes.HeaderedEvent
}

func (s failoverSnapshot) GetStateDeltas(context.Context, *userapi.Device, types.Range, string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, []string, error) {
	return []types.StateDelta{{RoomID: s.roomID, Membership: "join"}}, []string{s.roomID}, nil
}

func (s failoverSnapshot) IgnoresForUser(context.Context, string) (*types.IgnoredUsers, error) {
	return &types.IgnoredUsers{}, nil
}

func (s failoverSnapshot) RecentEvents(context.Context, []string, types.Range, *synctypes.RoomEventFilter, bool, bool) (map[string]types.RecentEvents, error) {
	streamEvents := make([]types.StreamEvent, len(s.events))
	for i, ev := range s.events {
		streamEvents[i] = types.StreamEvent{HeaderedEvent: ev, StreamPosition: types.StreamPosition(11 + i)}
	}
	return map[string]types.RecentEvents{s.roomID: {Events: streamEvents}}, nil
}

func (s failoverSnapshot) StreamEventsToEvents(_ context.Context, _ *userapi.Device, in []types.StreamEvent, _ roomserverAPI.SyncRoomserverAPI) []*rstypes.HeaderedEvent {
	out := make([]*rstypes.HeaderedEvent, len(in))
	for i := range in {
		out[i] = in[i].HeaderedEvent
	}
	return out
}

func (s failoverSnapshot) UnPartialStatedRoomsInRange(context.Context, string, types.Range) ([]string, error) {
	return nil, fmt.Errorf("unable to query un-partial-stated rooms: %w", driver.ErrBadConn)
}

type noPartialStateRooms struct {
	roomserverAPI.SyncRoomserverAPI
}

func (noPartialStateRooms) GetPartialStateRoomIDs(context.Context) ([]string, error) {
	return nil, nil
}

// A sync that loses the database while building a room must not answer with
// a position past that room's events: the client would never be sent them.
func TestIncrementalSyncStaysBehindEventsTheDatabaseLost(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "sent just before the failover"})
	snapshot := failoverSnapshot{roomID: room.ID, events: []*rstypes.HeaderedEvent{message}}
	provider := &PDUStreamProvider{rsAPI: noPartialStateRooms{}}
	req := &types.SyncRequest{
		Context:  t.Context(),
		Device:   &userapi.Device{UserID: alice.ID},
		Rooms:    map[string]string{},
		Response: types.NewResponse(),
		Filter:   synctypes.DefaultFilter(),
		Log:      logrus.NewEntry(logrus.StandardLogger()),
		// A room delta records membership changes here.
		MembershipChanges: map[string]struct{}{},
	}

	from, to := types.StreamPosition(10), types.StreamPosition(20)
	require.Equal(t, from, provider.IncrementalSync(t.Context(), snapshot, req, from, to))
}
