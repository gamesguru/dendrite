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
