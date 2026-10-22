package streams

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// departingClientSnapshot serves an incremental sync for one room whose
// client goes away while the room's timeline is being loaded, and fails the
// test if the sync carries on building the room afterwards.
type departingClientSnapshot struct {
	failoverSnapshot
	t      *testing.T
	cancel context.CancelCauseFunc
	gone   *atomic.Bool
}

func (s departingClientSnapshot) StreamEventsToEvents(ctx context.Context, device *userapi.Device, in []types.StreamEvent, rsAPI roomserverAPI.SyncRoomserverAPI) []*rstypes.HeaderedEvent {
	s.gone.Store(true)
	s.cancel(context.Canceled)
	return s.failoverSnapshot.StreamEventsToEvents(ctx, device, in, rsAPI)
}

// UnPartialStatedRoomsInRange is asked once before any room is built, to find the
// rooms that finished a resync, and again while a room is built. Only the latter,
// after the client has gone, is wrong.
func (s departingClientSnapshot) UnPartialStatedRoomsInRange(context.Context, string, types.Range) ([]string, error) {
	if s.gone.Load() {
		s.t.Error("kept building the room after the client went away")
	}
	return nil, nil
}

// A room with a large state takes a while to build, so a sync must stop
// building it once the client has gone, not only between rooms.
func TestIncrementalSyncStopsBuildingARoomOnceTheClientLeaves(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "sent as the client left"})
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	snapshot := departingClientSnapshot{
		failoverSnapshot: failoverSnapshot{roomID: room.ID, events: []*rstypes.HeaderedEvent{message}},
		t:                t,
		cancel:           cancel,
		gone:             &atomic.Bool{},
	}
	provider := &PDUStreamProvider{rsAPI: noPartialStateRooms{}}
	req := &types.SyncRequest{
		Context:           ctx,
		Device:            &userapi.Device{UserID: alice.ID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            synctypes.DefaultFilter(),
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
	}

	from, to := types.StreamPosition(10), types.StreamPosition(20)
	require.Equal(t, from, provider.IncrementalSync(ctx, snapshot, req, from, to))
	require.Empty(t, req.Response.Rooms.Join)
}
