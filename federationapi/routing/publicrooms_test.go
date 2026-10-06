// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

// directoryRoomserver publishes rooms of several types; !space:test has the
// most members, then !plain:test.
type directoryRoomserver struct {
	roomserverAPI.FederationRoomserverAPI
}

func (directoryRoomserver) QueryPublishedRooms(_ context.Context, _ *roomserverAPI.QueryPublishedRoomsRequest, res *roomserverAPI.QueryPublishedRoomsResponse) error {
	res.RoomIDs = []string{"!plain:test", "!space:test", "!custom:test", "!empty:test"}
	return nil
}

func (directoryRoomserver) QueryBulkStateContent(_ context.Context, req *roomserverAPI.QueryBulkStateContentRequest, res *roomserverAPI.QueryBulkStateContentResponse) error {
	content := map[string]string{"!plain:test": `{}`, "!space:test": `{"type":"m.space"}`, "!custom:test": `{"type":"org.example.custom"}`, "!empty:test": `{"type":""}`}
	members := map[string]int{"!space:test": 3, "!plain:test": 2}
	res.Rooms = make(map[string]map[gomatrixserverlib.StateKeyTuple]string)
	for _, roomID := range req.RoomIDs {
		state := map[gomatrixserverlib.StateKeyTuple]string{
			{EventType: spec.MRoomCreate}: content[roomID],
			{EventType: spec.MRoomName}:   "Directory " + roomID,
		}
		for i := range members[roomID] {
			state[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: string(rune('a' + i))}] = "join"
		}
		res.Rooms[roomID] = state
	}
	return nil
}

func federationPublicRooms(t *testing.T, body string) publicRoomsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/_matrix/federation/v1/publicRooms", strings.NewReader(body))
	res := GetPostPublicRooms(req, directoryRoomserver{})
	require.Equal(t, http.StatusOK, res.Code)
	raw, err := json.Marshal(res.JSON)
	require.NoError(t, err)
	var response publicRoomsResponse
	require.NoError(t, json.Unmarshal(raw, &response))
	return response
}

func roomIDs(rooms []roomserverAPI.PublicRoom) []string {
	ids := make([]string, 0, len(rooms))
	for _, room := range rooms {
		ids = append(ids, room.RoomID)
	}
	return ids
}

func TestFederationPublicRoomsRoomTypes(t *testing.T) {
	for _, tc := range []struct {
		filter string
		want   []string
	}{
		{`{}`, []string{"!space:test", "!plain:test", "!custom:test", "!empty:test"}},
		{`{"room_types":[]}`, []string{}},
		{`{"room_types":[null]}`, []string{"!plain:test"}},
		{`{"room_types":["m.space"]}`, []string{"!space:test"}},
		{`{"room_types":[""]}`, []string{"!empty:test"}},
		{`{"room_types":[null,"m.space"]}`, []string{"!space:test", "!plain:test"}},
		{`{"generic_search_term":"CUSTOM"}`, []string{"!custom:test"}},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			response := federationPublicRooms(t, `{"filter":`+tc.filter+`}`)
			require.Equal(t, tc.want, roomIDs(response.Chunk))
			require.Equal(t, len(tc.want), response.TotalRoomCountEstimate)
		})
	}

	// Rooms carry their type; untyped rooms have none.
	response := federationPublicRooms(t, `{}`)
	for _, room := range response.Chunk {
		switch room.RoomID {
		case "!space:test":
			require.Equal(t, "m.space", *room.RoomType)
		case "!empty:test":
			require.Empty(t, *room.RoomType)
		case "!plain:test":
			require.Nil(t, room.RoomType)
		}
	}
}

func TestFederationPublicRoomsFilteredPagination(t *testing.T) {
	filter := `"filter":{"room_types":[null,"m.space","org.example.custom"]}`
	first := federationPublicRooms(t, `{"limit":2,`+filter+`}`)
	require.Equal(t, []string{"!space:test", "!plain:test"}, roomIDs(first.Chunk))
	require.Equal(t, 3, first.TotalRoomCountEstimate)
	require.Equal(t, "2", first.NextBatch)
	require.Empty(t, first.PrevBatch)

	second := federationPublicRooms(t, `{"limit":2,"since":"2",`+filter+`}`)
	require.Equal(t, []string{"!custom:test"}, roomIDs(second.Chunk))
	require.Empty(t, second.NextBatch)
	require.Equal(t, "0", second.PrevBatch)
}
