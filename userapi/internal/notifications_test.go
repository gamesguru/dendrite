// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

func TestQueryNotifications(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		conn, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		db, err := storage.NewUserDatabase(ctx, sqlutil.NewConnectionManager(nil, config.DatabaseOptions{}), &config.DatabaseOptions{ConnectionString: config.DataSource(conn)}, "localhost", 4, 0, api.DefaultLoginTokenLifetime, "_server")
		require.NoError(t, err)
		userAPI := &UserInternalAPI{DB: db}
		for i := 1; i <= 5; i++ {
			eventID := fmt.Sprintf("$event%d", i)
			n := &api.Notification{RoomID: "!room:localhost", TS: spec.Timestamp(i), Event: synctypes.ClientEvent{EventID: eventID, Content: spec.RawJSON(`{}`)}}
			require.NoError(t, db.InsertNotification(ctx, "alice", "localhost", eventID, uint64(i), map[string]any{"highlight": i%2 == 0}, n))
		}
		// Other users and virtual hosts must never appear in Alice's results.
		for _, owner := range []struct {
			localpart string
			server    spec.ServerName
		}{{"bob", "localhost"}, {"alice", "elsewhere"}} {
			require.NoError(t, db.InsertNotification(ctx, owner.localpart, owner.server, "$other", 6, nil, &api.Notification{RoomID: "!room:localhost", Event: synctypes.ClientEvent{Content: spec.RawJSON(`{}`)}}))
		}
		_, err = db.SetNotificationsRead(ctx, "alice", "localhost", "!room:localhost", 2, true)
		require.NoError(t, err)
		for _, tc := range []struct {
			name, only string
			ids        []string
		}{
			{"all", "", []string{"$event1", "$event2", "$event3", "$event4", "$event5"}},
			{"highlights", "highlight", []string{"$event2", "$event4"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var res api.QueryNotificationsResponse
				var ids []string
				for page := 0; ; page++ {
					require.Less(t, page, 10, "pagination must terminate")
					require.NoError(t, userAPI.QueryNotifications(ctx, &api.QueryNotificationsRequest{Localpart: "alice", ServerName: "localhost", From: res.NextToken, Limit: 1, Only: tc.only}, &res))
					for _, n := range res.Notifications {
						ids = append(ids, n.Event.EventID)
						require.Equal(t, n.TS <= 2, n.Read)
					}
					if res.NextToken == "" {
						break
					}
				}
				require.Equal(t, tc.ids, ids)
				encoded, err := json.Marshal(res)
				require.NoError(t, err)
				require.JSONEq(t, `{"notifications":[]}`, string(encoded))
			})
		}
		count, err := db.GetNotificationCount(ctx, "alice", "localhost", tables.AllNotifications)
		require.NoError(t, err)
		require.EqualValues(t, 3, count, "badge counts still exclude read notifications")
	})
}
