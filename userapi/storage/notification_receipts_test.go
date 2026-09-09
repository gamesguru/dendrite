package storage_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage"
	"codefloe.com/pat-s/zendrite/userapi/storage/shared"
)

func TestThreadedNotificationReceipts(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()
		for i, thread := range []string{"main", "$a", "$b", "$a", "main", "$b"} {
			eventID := fmt.Sprintf("$event%d", i)
			require.NoError(t, db.InsertNotification(ctx, "alice", "localhost", eventID, uint64(i+1), map[string]any{"highlight": true}, &api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
			_, err := db.StoreNotificationEvent(ctx, "!room:test", eventID, thread, uint64(i+1))
			require.NoError(t, err)
		}
		count := func(want int64) {
			total, highlights, err := db.GetRoomNotificationCounts(ctx, "alice", "localhost", "!room:test")
			require.NoError(t, err)
			require.Equal(t, want, total)
			require.Equal(t, want, highlights)
		}
		count(6)
		changed, err := db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$event3", "$a")
		require.NoError(t, err)
		require.True(t, changed)
		count(4)
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$event0", "main")
		require.NoError(t, err)
		count(3)
		// Moving a public receipt behind a private receipt must not restore unread state.
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$event1", "$a")
		require.NoError(t, err)
		count(3)
		// An event without a notification still supplies a read-up-to position.
		_, err = db.StoreNotificationEvent(ctx, "!room:test", "$own-event", "main", 7)
		require.NoError(t, err)
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$own-event", "main")
		require.NoError(t, err)
		count(2)
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$event2", "")
		require.NoError(t, err)
		count(1)
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$missing", "")
		require.NoError(t, err)
		count(1)
		// Deferred receipts survive until event processing supplies the boundary.
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$future", "$b")
		require.NoError(t, err)
		count(1)
		users, err := db.StoreNotificationEvent(ctx, "!room:test", "$future", "$b", 8)
		require.NoError(t, err)
		require.Equal(t, []string{"@alice:localhost"}, users)
		count(0)
	})
}

func TestNotificationReceiptUpgradeAndRestart(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		conn, cleanup := test.PrepareDBConnectionString(t, backend)
		defer cleanup()
		open := func() *shared.Database {
			cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
			store, err := storage.NewUserDatabase(t.Context(), cm, &config.DatabaseOptions{ConnectionString: config.DataSource(conn)}, "localhost", bcrypt.MinCost, openIDLifetimeMS, loginTokenLifetime, "_server")
			require.NoError(t, err)
			db, ok := store.(*shared.Database)
			require.True(t, ok)
			return db
		}
		db := open()
		for i, content := range []string{
			`{}`, `{ "m.relates_to": {"rel_type":"m.thread", "event_id":"$root"} }`,
			`{ "m.relates_to": {"rel_type":"m.annotation", "event_id":"$event1"} }`,
		} {
			require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", fmt.Sprintf("$event%d", i), uint64(i+1), nil,
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(content)}}))
		}
		// Recreate a database from before the new migration, retaining old notifications.
		_, err := db.DB.ExecContext(t.Context(), `DROP TABLE userapi_notification_events;
   DROP TABLE userapi_pending_notification_receipts;
   DROP TABLE userapi_notification_receipt_locks;
   DELETE FROM db_migrations WHERE version = 'userapi: notification event threads';`)
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$event2", "$root")
		require.NoError(t, err)
		total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.EqualValues(t, 1, total)
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$future", "main")
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		defer db.DB.Close()
		total, _, err = db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.EqualValues(t, 1, total)
		users, err := db.StoreNotificationEvent(t.Context(), "!room:test", "$future", "main", 4)
		require.NoError(t, err)
		require.Equal(t, []string{"@alice:localhost"}, users)
		total, _, err = db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.Zero(t, total)
	})
}

func TestNotificationReceiptConcurrentEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()
		for i := range 10 {
			eventID := fmt.Sprintf("$concurrent%d", i)
			require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", eventID, uint64(i+1), nil,
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
			start := make(chan struct{})
			results := make(chan error, 2)
			go func() {
				<-start
				_, err := db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", eventID, "$thread")
				results <- err
			}()
			go func() {
				<-start
				_, err := db.StoreNotificationEvent(t.Context(), "!room:test", eventID, "$thread", uint64(i+1))
				results <- err
			}()
			close(start)
			require.NoError(t, <-results)
			require.NoError(t, <-results)
			total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
			require.NoError(t, err)
			require.Zero(t, total)
		}
	})
}
