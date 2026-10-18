// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/setup/config"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// The MSC4354 sticky duration query parameter puts the msc4354_sticky key
// into the event, and malformed durations are rejected.
func TestSendEventStickyDuration(t *testing.T) {
	roomID, userID := "!sticky:domain", "@sticky:domain"
	version := gomatrixserverlib.RoomVersionV10
	state, err := createEvents([]string{
		fmt.Sprintf(`{"type":"m.room.create","state_key":"","room_id":"%s","sender":"%s","content":{"creator":"%s","room_version":"10"}}`, roomID, userID, userID),
		fmt.Sprintf(`{"type":"m.room.member","state_key":"%s","room_id":"%s","sender":"%s","content":{"membership":"join"}}`, userID, roomID, userID),
	}, version)
	require.NoError(t, err)
	device := &uapi.Device{UserID: userID, AccessToken: "test-token"}

	send := func(t *testing.T, query string, stateKey *string) (*sendEventTestRoomserverAPI, int, string) {
		t.Helper()
		rs := &sendEventTestRoomserverAPI{t: t, roomIDStr: roomID, roomVersion: version, roomState: state}
		req, reqErr := http.NewRequestWithContext(t.Context(), "PUT", "https://domain/_matrix/client/v3/rooms/room/send/m.rtc.member/txn?"+query, strings.NewReader(`{"msc4354_sticky_key":"DEVICE"}`))
		require.NoError(t, reqErr)
		resp := SendEvent(req, device, roomID, "m.rtc.member", nil, stateKey, &config.ClientAPI{}, rs, nil, nil)
		errcode := ""
		if e, ok := resp.JSON.(spec.MatrixError); ok {
			errcode = string(e.ErrCode)
		}
		return rs, resp.Code, errcode
	}

	t.Run("sticky event", func(t *testing.T) {
		rs, code, _ := send(t, "org.matrix.msc4354.sticky_duration_ms=600000", nil)
		require.Equal(t, http.StatusOK, code)
		require.Len(t, rs.savedInputRoomEvents, 1)
		ev := rs.savedInputRoomEvents[0].Event
		require.Equal(t, int64(600000), gjson.GetBytes(ev.JSON(), "msc4354_sticky.duration_ms").Int())
		now := time.Now()
		require.True(t, ev.IsSticky(now, now))
		// The key is part of the signed event, outside its content.
		require.False(t, gjson.GetBytes(ev.Content(), "msc4354_sticky").Exists())
	})

	t.Run("durations above an hour are kept and capped when sticky", func(t *testing.T) {
		rs, code, _ := send(t, "org.matrix.msc4354.sticky_duration_ms=7200000", nil)
		require.Equal(t, http.StatusOK, code)
		ev := rs.savedInputRoomEvents[0].Event
		require.Equal(t, int64(7200000), gjson.GetBytes(ev.JSON(), "msc4354_sticky.duration_ms").Int())
		received := ev.OriginServerTS().Time()
		require.Equal(t, received.Add(time.Hour), ev.StickyEndTime(received))
	})

	t.Run("no parameter", func(t *testing.T) {
		rs, code, _ := send(t, "", nil)
		require.Equal(t, http.StatusOK, code)
		require.False(t, gjson.GetBytes(rs.savedInputRoomEvents[0].Event.JSON(), "msc4354_sticky").Exists())
	})

	t.Run("state events ignore the parameter", func(t *testing.T) {
		stateKey := "key"
		rs, code, _ := send(t, "org.matrix.msc4354.sticky_duration_ms=600000", &stateKey)
		require.Equal(t, http.StatusOK, code)
		require.False(t, gjson.GetBytes(rs.savedInputRoomEvents[0].Event.JSON(), "msc4354_sticky").Exists())
	})

	for _, value := range []string{"-1", "soon", "1.5", ""} {
		t.Run("invalid duration "+value, func(t *testing.T) {
			rs, code, errcode := send(t, "org.matrix.msc4354.sticky_duration_ms="+value, nil)
			require.Equal(t, http.StatusBadRequest, code)
			require.Equal(t, "M_INVALID_PARAM", errcode)
			require.Empty(t, rs.savedInputRoomEvents)
		})
	}
}
