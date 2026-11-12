package streams

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type pendingAccountDataSnapshot struct{ storage.DatabaseTransaction }

func (pendingAccountDataSnapshot) GetAccountDataInRange(context.Context, string, types.Range, *synctypes.EventFilter) (map[string][]string, types.StreamPosition, error) {
	// The user has tags in a room they are in and one they are not in; the
	// consumer has not recorded any global account data yet.
	return map[string][]string{
		"!joined:test": {"m.tag"},
		"!other:test":  {"m.tag"},
	}, 0, nil
}

type pendingAccountDataUserAPI struct{ userapi.SyncUserAPI }

func (pendingAccountDataUserAPI) QueryAccountData(_ context.Context, req *userapi.QueryAccountDataRequest, res *userapi.QueryAccountDataResponse) error {
	res.GlobalAccountData = map[string]json.RawMessage{
		"m.push_rules": json.RawMessage(`{"global":{"room":[]}}`),
		"deleted":      json.RawMessage(`{}`),
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
	}{
		{name: "unfiltered", wantPushRules: true},
		{name: "wildcard", include: stringList("m.*"), wantPushRules: true},
		{name: "excluded", exclude: stringList("m.push_*")},
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
			require.Contains(t, req.Response.Rooms.Join, "!joined:test")
			joined := req.Response.Rooms.Join["!joined:test"]
			require.NotNil(t, joined.AccountData)
			require.Len(t, joined.AccountData.Events, 1)
			require.Equal(t, "m.tag", joined.AccountData.Events[0].Type)
			require.NotContains(t, req.Response.Rooms.Join, "!other:test")
		})
	}
}

func stringList(values ...string) *[]string { return &values }
