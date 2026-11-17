package routing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rsTypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// roomVersionErrorRoomserver answers the room existence check with err.
type roomVersionErrorRoomserver struct {
	api.SyncRoomserverAPI
	queriedRoomID string
	err           error
}

func (r *roomVersionErrorRoomserver) QueryRoomVersionForRoom(_ context.Context, roomID string) (gomatrixserverlib.RoomVersion, error) {
	r.queriedRoomID = roomID
	return "", r.err
}

func adminRepairRequest(t *testing.T, roomID string) *http.Request {
	t.Helper()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("roomID", roomID)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func TestAdminRepairUnsignedUnknownRoom(t *testing.T) {
	const roomID = "!missing:example.org"
	rsAPI := &roomVersionErrorRoomserver{err: rsTypes.ErrRoomNotFound}

	res := AdminRepairUnsigned(adminRepairRequest(t, roomID), nil, rsAPI, &userapi.Device{UserID: "@admin:example.org"})

	require.Equal(t, roomID, rsAPI.queriedRoomID)
	require.Equal(t, http.StatusNotFound, res.Code)
}

func TestAdminRepairUnsignedRoomVersionError(t *testing.T) {
	const roomID = "!broken:example.org"
	rsAPI := &roomVersionErrorRoomserver{err: errors.New("roomserver unavailable")}

	res := AdminRepairUnsigned(adminRepairRequest(t, roomID), nil, rsAPI, &userapi.Device{UserID: "@admin:example.org"})

	require.Equal(t, roomID, rsAPI.queriedRoomID)
	require.Equal(t, http.StatusInternalServerError, res.Code)
}

func TestRepairUnsignedAllRoomsSumsReportsAndSurvivesFailures(t *testing.T) {
	reports := map[string]*internal.PrevStateRepairReport{
		"!a:example.org": {RoomID: "!a:example.org", Scanned: 10, StateEvents: 8, Redacted: 1, AlreadyHad: 2,
			Repairable: 4, Repaired: 4, NoPredecessor: 1, LookupUnavailable: 0, WriteSkipped: 1, Errors: 0,
			Samples: []internal.PrevStateRepairSample{{EventID: "$x"}}},
		"!c:example.org": {RoomID: "!c:example.org", Scanned: 5, StateEvents: 5, AlreadyHad: 1,
			Repairable: 0, Repaired: 0, NoPredecessor: 2, LookupUnavailable: 2, Errors: 3},
	}
	var asked []string
	repair := func(_ context.Context, roomID string, apply bool) (*internal.PrevStateRepairReport, error) {
		asked = append(asked, roomID)
		require.True(t, apply)
		if roomID == "!b:example.org" {
			return nil, errors.New("roomserver unavailable")
		}
		return reports[roomID], nil
	}

	res := repairUnsignedAllRooms(context.Background(),
		[]string{"!a:example.org", "!b:example.org", "!c:example.org"}, true, repair)

	require.Equal(t, []string{"!a:example.org", "!b:example.org", "!c:example.org"}, asked,
		"every room is attempted, in order, and one failure does not stop the others")
	require.True(t, res.Apply)
	require.Len(t, res.Rooms, 2)
	require.Nil(t, res.Rooms[0].Samples, "per-room samples are dropped from the all-rooms response")
	require.Equal(t, []repairUnsignedAllFailure{{RoomID: "!b:example.org", Error: "roomserver unavailable"}}, res.Failed)
	require.Equal(t, repairUnsignedAllTotals{
		Rooms: 2, Failed: 1, Scanned: 15, StateEvents: 13, Redacted: 1, AlreadyHadPrevContent: 3,
		Repairable: 4, Repaired: 4, NoPredecessor: 3, LookupUnavailable: 2, WriteSkipped: 1, Errors: 3,
	}, res.Totals)
}

func TestRepairUnsignedAllRoomsStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	repair := func(_ context.Context, roomID string, _ bool) (*internal.PrevStateRepairReport, error) {
		calls++
		cancel() // the client goes away after the first room
		return &internal.PrevStateRepairReport{RoomID: roomID}, nil
	}

	res := repairUnsignedAllRooms(ctx, []string{"!a:x", "!b:x", "!c:x"}, false, repair)

	require.Equal(t, 1, calls, "no room is started once the context has ended")
	require.Len(t, res.Rooms, 1)
	require.Len(t, res.Failed, 2)
	require.Contains(t, res.Failed[0].Error, "not attempted")
}
