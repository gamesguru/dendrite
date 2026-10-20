package routing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

func pubRoom(name string) roomserverAPI.PublicRoom {
	return roomserverAPI.PublicRoom{PublicRoom: fclient.PublicRoom{Name: name}}
}

func TestSliceInto(t *testing.T) {
	slice := []roomserverAPI.PublicRoom{
		pubRoom("a"), pubRoom("b"), pubRoom("c"), pubRoom("d"), pubRoom("e"), pubRoom("f"), pubRoom("g"),
	}
	limit := int64(3)
	testCases := []struct {
		since      int64
		wantPrev   int
		wantNext   int
		wantSubset []roomserverAPI.PublicRoom
	}{
		{
			since:      0,
			wantPrev:   -1,
			wantNext:   3,
			wantSubset: slice[0:3],
		},
		{
			since:      3,
			wantPrev:   0,
			wantNext:   6,
			wantSubset: slice[3:6],
		},
		{since: 9, wantPrev: 6, wantNext: -1, wantSubset: slice[7:7]},
		{
			since:      6,
			wantPrev:   3,
			wantNext:   -1,
			wantSubset: slice[6:7],
		},
	}
	for _, tc := range testCases {
		subset, prev, next := sliceInto(slice, tc.since, limit)
		if !reflect.DeepEqual(subset, tc.wantSubset) {
			t.Errorf("returned subset is wrong, got %v want %v", subset, tc.wantSubset)
		}
		if prev != tc.wantPrev {
			t.Errorf("returned prev is wrong, got %d want %d", prev, tc.wantPrev)
		}
		if next != tc.wantNext {
			t.Errorf("returned next is wrong, got %d want %d", next, tc.wantNext)
		}
	}
}

// directoryRoomserver supplies real create-event content to the directory builder.
type directoryRoomserver struct {
	roomserverAPI.ClientRoomserverAPI
}

func (directoryRoomserver) QueryPublishedRooms(_ context.Context, _ *roomserverAPI.QueryPublishedRoomsRequest, res *roomserverAPI.QueryPublishedRoomsResponse) error {
	res.RoomIDs = []string{"!plain:test", "!space:test", "!custom:test", "!empty:test"}
	return nil
}

func (directoryRoomserver) QueryBulkStateContent(_ context.Context, req *roomserverAPI.QueryBulkStateContentRequest, res *roomserverAPI.QueryBulkStateContentResponse) error {
	content := []string{`{}`, `{"type":"m.space"}`, `{"type":"org.example.custom"}`, `{"type":""}`}
	res.Rooms = make(map[string]map[gomatrixserverlib.StateKeyTuple]string)
	for i, roomID := range req.RoomIDs {
		state := make(map[gomatrixserverlib.StateKeyTuple]string)
		for _, tuple := range req.StateTuples {
			switch tuple.EventType {
			case spec.MRoomCreate:
				state[tuple] = content[i]
			case spec.MRoomName:
				state[tuple] = "Directory " + roomID
			case spec.MRoomTopic:
				state[tuple] = "Some TOPIC"
			case spec.MRoomCanonicalAlias:
				state[tuple] = "#alias:test"
			}
		}
		res.Rooms[roomID] = state
	}
	return nil
}

func TestPublicRoomsRoomTypes(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{"omitted", `{}`, []string{"!custom:test", "!empty:test", "!plain:test", "!space:test"}},
		{"null filter", `{"room_types":null}`, []string{"!custom:test", "!empty:test", "!plain:test", "!space:test"}},
		{"empty list", `{"room_types":[]}`, []string{}},
		{"untyped", `{"room_types":[null]}`, []string{"!plain:test"}},
		{"spaces", `{"room_types":["m.space"]}`, []string{"!space:test"}},
		{"custom", `{"room_types":["org.example.custom"]}`, []string{"!custom:test"}},
		{"empty string", `{"room_types":[""]}`, []string{"!empty:test"}},
		{"mixed", `{"room_types":[null,"m.space"]}`, []string{"!plain:test", "!space:test"}},
		{"unknown", `{"room_types":["unknown"]}`, []string{}},
		{"case sensitive types", `{"room_types":["M.SPACE"]}`, []string{}},
		{"search name", `{"room_types":[null,"m.space"],"generic_search_term":"SPACE"}`, []string{"!space:test"}},
		{"search topic", `{"room_types":["m.space"],"generic_search_term":"topic"}`, []string{"!space:test"}},
		{"search alias", `{"room_types":["m.space"],"generic_search_term":"ALIAS"}`, []string{"!space:test"}},
		{"no search match", `{"room_types":["m.space"],"generic_search_term":"absent"}`, []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/publicRooms", strings.NewReader(`{"filter":`+tc.filter+`}`))
			res := GetPostPublicRooms(req, directoryRoomserver{}, nil, nil, &cfg.ClientAPI)
			require.Equal(t, http.StatusOK, res.Code)
			body, err := json.Marshal(res.JSON)
			require.NoError(t, err)
			var response publicRoomsResponse
			require.NoError(t, json.Unmarshal(body, &response))
			got := make([]string, 0, len(response.Chunk))
			for _, room := range response.Chunk {
				got = append(got, room.RoomID)
				if room.RoomID == "!empty:test" {
					require.NotNil(t, room.RoomType)
					require.Empty(t, *room.RoomType)
				}
			}
			sort.Strings(got)
			require.Equal(t, tc.want, got)
			require.Equal(t, len(tc.want), response.TotalRoomCountEstimate)
		})
	}
}

func TestPublicRoomsFilteredPagination(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	seen := make(map[string]bool)
	since := ""
	for page := 0; page < 2; page++ {
		body := `{"limit":1,"since":"` + since + `","filter":{"room_types":[null,"m.space"],"generic_search_term":"directory"}}`
		req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/publicRooms", strings.NewReader(body))
		res := GetPostPublicRooms(req, directoryRoomserver{}, nil, nil, &cfg.ClientAPI)
		require.Equal(t, http.StatusOK, res.Code)
		response, ok := res.JSON.(*publicRoomsResponse)
		require.True(t, ok)
		require.Len(t, response.Chunk, 1)
		require.Equal(t, 2, response.TotalRoomCountEstimate)
		seen[response.Chunk[0].RoomID] = true
		since = response.NextBatch
		if page == 0 {
			require.Equal(t, "T1", since)
			require.Empty(t, response.PrevBatch)
		} else {
			require.Empty(t, since)
			require.Equal(t, "T0", response.PrevBatch)
		}
	}
	require.Equal(t, map[string]bool{"!plain:test": true, "!space:test": true}, seen)
	// Filtering must not mutate the shared cache used by subsequent requests.
	require.Len(t, getPublicRoomsFromCache(), 4)
}

type directoryFederation struct {
	fclient.FederationClient
	t          *testing.T
	wantFilter string
	err        error
	called     bool
}

func (f *directoryFederation) DoRequestAndParseResponse(_ context.Context, req *http.Request, result any) error {
	f.called = true
	require.Equal(f.t, http.MethodPost, req.Method)
	require.Equal(f.t, "remote.test", req.URL.Host)
	require.Equal(f.t, "/_matrix/federation/v1/publicRooms", req.URL.Path)
	require.Contains(f.t, req.Header.Get("Authorization"), "X-Matrix")
	defer req.Body.Close()
	var body map[string]json.RawMessage
	require.NoError(f.t, json.NewDecoder(req.Body).Decode(&body))
	require.JSONEq(f.t, f.wantFilter, string(body["filter"]))
	require.JSONEq(f.t, `"Topaque"`, string(body["since"]))
	require.JSONEq(f.t, `7`, string(body["limit"]))
	require.JSONEq(f.t, `true`, string(body["include_all_networks"]))
	require.NotContains(f.t, body, "server")
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(`{"chunk":[{"room_id":"!remote:remote.test","room_type":"m.space"}],"next_batch":"Tnext","total_room_count_estimate":12}`), result)
}

func TestPublicRoomsRemoteRoomTypes(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	// The remote returns a space whatever the filter; it is dropped unless the
	// filter admits spaces, for servers that ignore room_types.
	for roomFilter, wantSpace := range map[string]bool{
		`{}`:                         true,
		`{"room_types":[]}`:          false,
		`{"room_types":[null]}`:      false,
		`{"room_types":["m.space"]}`: true,
		`{"room_types":[null,"m.space","org.example.custom",""],"generic_search_term":"hello"}`: true,
	} {
		t.Run(roomFilter, func(t *testing.T) {
			fed := &directoryFederation{t: t, wantFilter: roomFilter}
			req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/publicRooms?server=remote.test", strings.NewReader(`{"limit":7,"since":"Topaque","include_all_networks":true,"filter":`+roomFilter+`}`))
			res := GetPostPublicRooms(req, nil, nil, fed, &cfg.ClientAPI)
			require.True(t, fed.called)
			require.Equal(t, http.StatusOK, res.Code)
			body, err := json.Marshal(res.JSON)
			require.NoError(t, err)
			require.Contains(t, string(body), `"next_batch":"Tnext"`)
			if wantSpace {
				require.Contains(t, string(body), `"room_id":"!remote:remote.test"`)
			} else {
				require.NotContains(t, string(body), `"room_id":"!remote:remote.test"`)
			}
		})
	}
	t.Run("remote error", func(t *testing.T) {
		fed := &directoryFederation{t: t, wantFilter: `{}`, err: errors.New("remote unavailable")}
		req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/publicRooms?server=remote.test", strings.NewReader(`{"limit":7,"since":"Topaque","include_all_networks":true}`))
		res := GetPostPublicRooms(req, nil, nil, fed, &cfg.ClientAPI)
		require.True(t, fed.called)
		require.Equal(t, http.StatusInternalServerError, res.Code)
	})
}
