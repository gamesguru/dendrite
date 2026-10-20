package routing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type mutualRoomsFailingAPI struct {
	api.ClientRoomserverAPI
	failUser string
}

func (a mutualRoomsFailingAPI) QueryRoomsForUser(_ context.Context, user spec.UserID, _ string) ([]spec.RoomID, error) {
	if user.String() == a.failUser {
		return nil, errors.New("database unavailable")
	}
	return nil, nil
}

func TestMutualRoomsInvalidTokens(t *testing.T) {
	h := newMutualRoomsHandler(nil)
	device := &userapi.Device{UserID: "@alice:test"}
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"expired", `{"caller":"@alice:test","target":"@bob:test","after":"!room:test","expires":1}`},
		{"invalid JSON", "{"},
		{"invalid base64", "%"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := base64.RawURLEncoding.EncodeToString([]byte(tc.payload))
			if tc.name == "invalid base64" {
				payload = tc.payload
			}
			token := payload + "." + base64.RawURLEncoding.EncodeToString(h.signature(payload))
			req := httptest.NewRequest(http.MethodGet, "/?user_id=@bob:test&from="+url.QueryEscape(token), nil)
			response := h.get(req, device)
			require.Equal(t, http.StatusBadRequest, response.Code)
			err, ok := response.JSON.(spec.MatrixError)
			require.True(t, ok)
			require.Equal(t, spec.ErrorInvalidParam, err.ErrCode)
		})
	}
}

func TestMutualRoomsTokenLifetime(t *testing.T) {
	h := newMutualRoomsHandler(mutualRoomsFailingAPI{})
	// A token remains valid more than the specified minimum ten minutes.
	cursor := mutualRoomsCursor{Caller: "@alice:test", Target: "@bob:test", After: "!room:test", Expires: time.Now().Add(mutualRoomsTokenTTL - 11*time.Minute).Unix()}
	data, err := json.Marshal(cursor)
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(data)
	token := payload + "." + base64.RawURLEncoding.EncodeToString(h.signature(payload))
	req := httptest.NewRequest(http.MethodGet, "/?user_id=@bob:test&from="+url.QueryEscape(token), nil)
	require.Equal(t, http.StatusOK, h.get(req, &userapi.Device{UserID: cursor.Caller}).Code)
}

func TestMutualRoomsStorageErrors(t *testing.T) {
	for _, user := range []string{"@alice:test", "@bob:test"} {
		t.Run(user, func(t *testing.T) {
			h := newMutualRoomsHandler(mutualRoomsFailingAPI{failUser: user})
			req := httptest.NewRequest(http.MethodGet, "/?user_id=@bob:test", nil)
			require.Equal(t, http.StatusInternalServerError, h.get(req, &userapi.Device{UserID: "@alice:test"}).Code)
		})
	}
}

func TestMutualRoomsMalformedQuery(t *testing.T) {
	h := newMutualRoomsHandler(nil)
	for _, query := range []string{"user_id=@bob:test&from=%ZZ", "user_id=@bob:test&from=a&from=b", "user_id=@bob:test&user_id=@charlie:test"} {
		t.Run(query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/?"+query, nil)
			require.Equal(t, http.StatusBadRequest, h.get(req, &userapi.Device{UserID: "@alice:test"}).Code)
		})
	}
}
