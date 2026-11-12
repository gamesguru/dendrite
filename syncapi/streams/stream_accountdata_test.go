package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type pendingAccountDataSnapshot struct{ storage.DatabaseTransaction }

func (pendingAccountDataSnapshot) GetAccountDataInRange(_ context.Context, _ string, _ types.Range, filter *synctypes.EventFilter) (map[string][]string, types.StreamPosition, error) {
	// Room data has been consumed, but global push rules are still pending.
	data := map[string][]string{}
	if accountDataTypeMatches("m.tag", filter) {
		data["!joined:test"] = []string{"m.tag"}
		data["!other:test"] = []string{"m.tag"}
	}
	return data, 0, nil
}

type pendingAccountDataUserAPI struct{ userapi.SyncUserAPI }

func (pendingAccountDataUserAPI) QueryAccountData(_ context.Context, req *userapi.QueryAccountDataRequest, res *userapi.QueryAccountDataResponse) error {
	res.GlobalAccountData = map[string]json.RawMessage{
		"m.push_rules": json.RawMessage(`{"global":{"room":[]}}`),
		"deleted":      json.RawMessage(`{}`),
		"m.direct":     json.RawMessage(`{"@bob:test":["!room:test"]}`),
	}
	res.RoomAccountData = map[string]map[string]json.RawMessage{
		"!joined:test": {"m.tag": json.RawMessage(`{"tags":{}}`)},
		"!other:test":  {"m.tag": json.RawMessage(`{"tags":{}}`)},
	}
	return nil
}

func TestCompleteAccountDataBeforeConsumer(t *testing.T) {
	for _, tc := range []struct {
		name             string
		include, exclude *[]string
		wantPushRules    bool
		wantRoomData     bool
	}{
		{name: "unfiltered", wantPushRules: true, wantRoomData: true},
		{name: "wildcard", include: stringList("m.*"), wantPushRules: true, wantRoomData: true},
		{name: "excluded", exclude: stringList("m.push_*"), wantRoomData: true},
		{name: "empty inclusion", include: stringList()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &types.SyncRequest{
				Device:   &userapi.Device{UserID: "@alice:test"},
				Rooms:    map[string]string{"!joined:test": "join"},
				Response: types.NewResponse(), Filter: synctypes.DefaultFilter(), Log: logrus.NewEntry(logrus.New()),
			}
			req.Filter.AccountData.Types, req.Filter.AccountData.NotTypes = tc.include, tc.exclude
			p := AccountDataStreamProvider{userAPI: pendingAccountDataUserAPI{}}
			pos := p.CompleteSync(t.Context(), pendingAccountDataSnapshot{}, req)
			require.Zero(t, pos, "pending notifications must remain available to incremental sync")
			if tc.wantPushRules {
				require.Len(t, req.Response.AccountData.Events, 1)
				require.Equal(t, "m.push_rules", req.Response.AccountData.Events[0].Type)
				require.JSONEq(t, `{"global":{"room":[]}}`, string(req.Response.AccountData.Events[0].Content))
			} else {
				require.Empty(t, req.Response.AccountData.Events)
			}
			require.NotContains(t, req.Response.Rooms.Join, "!other:test")
			if !tc.wantRoomData {
				require.NotContains(t, req.Response.Rooms.Join, "!joined:test")
				return
			}
			require.Contains(t, req.Response.Rooms.Join, "!joined:test")
			roomData := req.Response.Rooms.Join["!joined:test"].AccountData
			require.NotNil(t, roomData)
			require.Len(t, roomData.Events, 1)
			require.Equal(t, "m.tag", roomData.Events[0].Type)
			require.JSONEq(t, `{"tags":{}}`, string(roomData.Events[0].Content))
		})
	}
}

func stringList(values ...string) *[]string { return &values }

type globalAccountDataSnapshot struct{ pendingAccountDataSnapshot }

func (globalAccountDataSnapshot) GetAccountDataInRange(_ context.Context, _ string, _ types.Range, filter *synctypes.EventFilter) (map[string][]string, types.StreamPosition, error) {
	if filter.Limit == 0 {
		return nil, 0, nil
	}
	return map[string][]string{"": {"m.direct"}}, 1, nil
}

func TestCompleteAccountDataPushRulesRespectsLimit(t *testing.T) {
	for _, limit := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			req := &types.SyncRequest{
				Device:   &userapi.Device{UserID: "@alice:test"},
				Response: types.NewResponse(), Filter: synctypes.DefaultFilter(), Log: logrus.NewEntry(logrus.New()),
			}
			req.Filter.AccountData.Limit = limit
			p := AccountDataStreamProvider{userAPI: pendingAccountDataUserAPI{}}
			p.CompleteSync(t.Context(), globalAccountDataSnapshot{}, req)
			require.Len(t, req.Response.AccountData.Events, limit)
			if limit > 0 {
				require.Equal(t, "m.direct", req.Response.AccountData.Events[0].Type)
			}
			if limit == 2 {
				require.Equal(t, "m.push_rules", req.Response.AccountData.Events[1].Type)
			}
		})
	}
}
