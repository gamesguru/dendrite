package streams

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// unresolvedMembershipSnapshot serves one joined room and reports the
// requesting user's current membership, but has no matching history visibility
// answer from the roomserver.
type unresolvedMembershipSnapshot struct {
	failoverSnapshot
}

func (unresolvedMembershipSnapshot) UnPartialStatedRoomsInRange(context.Context, string, types.Range) ([]string, error) {
	return nil, nil
}

func (unresolvedMembershipSnapshot) SelectMembershipForUser(context.Context, string, string, int64) (string, int64, error) {
	return spec.Join, 0, nil
}

func (unresolvedMembershipSnapshot) BackwardExtremitiesForRoom(context.Context, string) (map[string][]string, error) {
	return nil, nil
}

func (unresolvedMembershipSnapshot) GetBackwardTopologyPos(context.Context, []*rstypes.HeaderedEvent) (types.TopologyToken, error) {
	return types.TopologyToken{}, nil
}

type unresolvedMembershipRoomserver struct {
	noPartialStateRooms
	err error
}

func (r unresolvedMembershipRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (r unresolvedMembershipRoomserver) QueryMembershipAtEvent(context.Context, spec.RoomID, []string, spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	return nil, r.err
}

// A membership error that the database layer will report again on retry must
// leave the sync at its old position. Advancing would omit every event whose
// membership could not be resolved, and no later sync would ever deliver them.
func TestIncrementalSyncStaysBehindEventsWithUnresolvedMembership(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "membership could not be resolved"})
	snapshot := unresolvedMembershipSnapshot{
		failoverSnapshot: failoverSnapshot{roomID: room.ID, events: []*rstypes.HeaderedEvent{message}},
	}
	provider := &PDUStreamProvider{rsAPI: unresolvedMembershipRoomserver{err: errors.New("membership lookup failed")}}
	req := &types.SyncRequest{
		Context:           t.Context(),
		Device:            &userapi.Device{UserID: alice.ID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            synctypes.DefaultFilter(),
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
	}

	from, to := types.StreamPosition(10), types.StreamPosition(20)
	require.Equal(t, from, provider.IncrementalSync(t.Context(), snapshot, req, from, to))
	require.Empty(t, req.Response.Rooms.Join)
}

// The membership cap's own deadline is a local bound, not a persistent
// failure. A saturated pool must not leave the client on the same next_batch;
// the conservatively filtered page is served and the position advances.
func TestIncrementalSyncAdvancesPastTimedOutMembershipLookup(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "lookup timed out"})
	snapshot := unresolvedMembershipSnapshot{
		failoverSnapshot: failoverSnapshot{roomID: room.ID, events: []*rstypes.HeaderedEvent{message}},
	}
	lookupErr := fmt.Errorf("roomserver lookup: %w", context.DeadlineExceeded)
	provider := &PDUStreamProvider{rsAPI: unresolvedMembershipRoomserver{err: lookupErr}}
	req := &types.SyncRequest{
		Context:           t.Context(),
		Device:            &userapi.Device{UserID: alice.ID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            synctypes.DefaultFilter(),
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
	}

	from, to := types.StreamPosition(10), types.StreamPosition(20)
	require.Equal(t, to, provider.IncrementalSync(t.Context(), snapshot, req, from, to))
	require.Empty(t, req.Response.Rooms.Join, "the withheld events are not delivered")
}

var _ roomserverAPI.SyncRoomserverAPI = unresolvedMembershipRoomserver{}
