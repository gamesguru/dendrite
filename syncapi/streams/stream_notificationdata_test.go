package streams

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func TestThreadNotificationCounts(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		conn, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		db, err := storage.NewSyncServerDatasource(ctx, sqlutil.NewConnectionManager(nil, config.DatabaseOptions{}), &config.DatabaseOptions{ConnectionString: config.DataSource(conn)})
		require.NoError(t, err)
		const user, threaded, legacy, quiet = "@alice:test", "!threaded:test", "!legacy:test", "!quiet:test"
		_, err = db.UpsertRoomUnreadNotificationCounts(ctx, user, &eventutil.NotificationData{
			RoomID: threaded, UnreadNotificationCount: 4, UnreadHighlightCount: 1,
			Threads: map[string]eventutil.ThreadNotificationCounts{
				eventutil.MainThread: {UnreadNotificationCount: 1},
				"$root":              {UnreadNotificationCount: 3, UnreadHighlightCount: 1},
			},
		})
		require.NoError(t, err)
		// Counts from a producer that does not count by thread.
		_, err = db.UpsertRoomUnreadNotificationCounts(ctx, user, &eventutil.NotificationData{RoomID: legacy, UnreadNotificationCount: 2})
		require.NoError(t, err)

		sync := func(byThread bool) map[string]json.RawMessage {
			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Rollback() //nolint:errcheck
			req := &types.SyncRequest{
				Device:   &userapi.Device{UserID: user},
				Rooms:    map[string]string{threaded: "join", legacy: "join", quiet: "join"},
				Response: types.NewResponse(),
				Filter:   synctypes.DefaultFilter(),
			}
			req.Filter.Room.Timeline.UnreadThreadNotifications = byThread
			for _, roomID := range []string{threaded, legacy, quiet} {
				jr := types.NewJoinResponse()
				jr.Timeline.Events = []synctypes.ClientEvent{{EventID: "$event", Content: []byte(`{}`)}}
				req.Response.Rooms.Join[roomID] = jr
			}
			provider := &NotificationDataStreamProvider{DefaultStreamProvider{DB: db}}
			provider.IncrementalSync(ctx, snapshot, req, 0, 0)
			rooms := map[string]json.RawMessage{}
			for roomID, jr := range req.Response.Rooms.Join {
				raw, err := json.Marshal(jr)
				require.NoError(t, err)
				rooms[roomID] = raw
			}
			return rooms
		}
		withoutThreads := sync(false)
		require.JSONEq(t, `{"highlight_count":1,"notification_count":4}`, field(t, withoutThreads[threaded], "unread_notifications"))
		require.Empty(t, field(t, withoutThreads[threaded], "unread_thread_notifications"))

		byThread := sync(true)
		require.JSONEq(t, `{"highlight_count":0,"notification_count":1}`, field(t, byThread[threaded], "unread_notifications"))
		require.JSONEq(t, `{"$root":{"highlight_count":1,"notification_count":3}}`, field(t, byThread[threaded], "unread_thread_notifications"))
		require.JSONEq(t, `{"highlight_count":0,"notification_count":2}`, field(t, byThread[legacy], "unread_notifications"))
		require.Empty(t, field(t, byThread[legacy], "unread_thread_notifications"))
		require.JSONEq(t, `{"highlight_count":0,"notification_count":0}`, field(t, byThread[quiet], "unread_notifications"))
	})
}

func field(t *testing.T, raw json.RawMessage, name string) string {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	return string(fields[name])
}
