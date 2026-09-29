package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrix-org/gomatrix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type peekRoomserverAPI struct {
	api.ClientRoomserverAPI
	err error
	req api.PerformPeekRequest
}

func (a *peekRoomserverAPI) PerformPeek(_ context.Context, req *api.PerformPeekRequest) (string, error) {
	a.req = *req
	return req.RoomIDOrAlias, a.err
}

// Every peek response must be a Matrix JSON body, including failures that
// never reached a remote server.
func TestPeekRoomResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		wantCode int
		wantErr  string
	}{
		{name: "success", wantCode: http.StatusOK},
		{name: "invalid room", err: api.ErrInvalidID{Err: errors.New("room ID is invalid")}, wantCode: http.StatusBadRequest, wantErr: "M_UNKNOWN"},
		{name: "not world-readable", err: api.ErrNotAllowed{Err: errors.New("room is not world-readable")}, wantCode: http.StatusForbidden, wantErr: "M_FORBIDDEN"},
		{
			name:     "remote Matrix error",
			err:      &gomatrix.HTTPError{Code: http.StatusNotFound, Message: `{"errcode":"M_NOT_FOUND","error":"Room not found"}`},
			wantCode: http.StatusNotFound, wantErr: "M_NOT_FOUND",
		},
		{
			name:     "local federation failure",
			err:      &gomatrix.HTTPError{Message: "r.federation.Peek: json: cannot unmarshal object"},
			wantCode: http.StatusBadGateway, wantErr: "M_UNKNOWN",
		},
		{
			name:     "remote plain-text error",
			err:      &gomatrix.HTTPError{Code: http.StatusBadGateway, Message: "upstream unavailable"},
			wantCode: http.StatusBadGateway, wantErr: "M_UNKNOWN",
		},
		{name: "internal error", err: errors.New("database unavailable"), wantCode: http.StatusInternalServerError, wantErr: "M_UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rsAPI := &peekRoomserverAPI{err: tc.err}
			req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/peek/!room:test?server_name=remote.test", nil)
			res := PeekRoomByIDOrAlias(req, &userapi.Device{UserID: "@alice:test", ID: "DEVICE"}, rsAPI, "!room:test")
			assert.Equal(t, tc.wantCode, res.Code)
			assert.Equal(t, []string{"remote.test"}, serverNameStrings(rsAPI.req))

			body, err := json.Marshal(res.JSON)
			require.NoError(t, err, "the response body must encode as JSON")
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			if tc.wantErr == "" {
				assert.Equal(t, "!room:test", decoded["room_id"])
			} else {
				assert.Equal(t, tc.wantErr, decoded["errcode"], "body %s", body)
			}
		})
	}
}

func serverNameStrings(req api.PerformPeekRequest) []string {
	names := make([]string, 0, len(req.ServerNames))
	for _, name := range req.ServerNames {
		names = append(names, string(name))
	}
	return names
}
