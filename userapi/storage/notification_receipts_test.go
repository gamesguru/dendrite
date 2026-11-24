package storage_test

import (
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
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
			_, err := db.StoreNotificationEvent(ctx, "!room:test", eventID, thread, uint64(i+1), int64(i+1), 0)
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
		_, err = db.StoreNotificationEvent(ctx, "!room:test", "$own-event", "main", 7, 7, 0)
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
		users, err := db.StoreNotificationEvent(ctx, "!room:test", "$future", "$b", 8, 8, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"@alice:localhost"}, users)
		count(0)
	})
}

func TestLateNotificationBehindReceiptIsRead(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()

		// The receipt event is newer in the room (origin 200), but is written
		// to the output stream before the older event's delayed notification.
		_, err := db.StoreNotificationEvent(t.Context(), "!room:test", "$receipt", "main", 100, 200, 200)
		require.NoError(t, err)
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$receipt", "")
		require.NoError(t, err)

		insert := func(eventID string, pos int) {
			require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", eventID, uint64(pos), nil,
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
		}
		count := func(want int64) {
			t.Helper()
			total, _, countErr := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
			require.NoError(t, countErr)
			require.Equal(t, want, total)
		}

		// The late event is older in the timeline (origin 100) but has a later
		// notification id and output stream position than the receipt event.
		insert("$late", 1)
		users, err := db.StoreNotificationEvent(t.Context(), "!room:test", "$late", "main", 200, 100, 100)
		require.NoError(t, err)
		require.Equal(t, []string{"@alice:localhost"}, users)
		count(0)

		// An event genuinely after the receipt stays unread.
		insert("$after", 2)
		users, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$after", "main", 300, 300, 300)
		require.NoError(t, err)
		require.Empty(t, users)
		count(1)
	})
}

func TestLateNotificationReceiptUsesRoomOrder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		originTS []spec.Timestamp
	}{
		{name: "equal timestamps", originTS: []spec.Timestamp{100, 100, 100, 100}},
		{name: "reversed timestamps", originTS: []spec.Timestamp{300, 100, 400, 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
				db, cleanup := mustCreateUserDatabase(t, backend)
				defer cleanup()
				roomID := "!room:test"
				add := func(eventID string, streamPos uint64, depth int64, originTS spec.Timestamp) {
					t.Helper()
					require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", eventID, streamPos, nil,
						&api.Notification{RoomID: roomID, Event: synctypes.ClientEvent{Content: []byte(`{}`), OriginServerTS: originTS}}))
					_, err := db.StoreNotificationEvent(t.Context(), roomID, eventID, "main", streamPos, depth, int64(originTS))
					require.NoError(t, err)
				}

				// The receipt's room-order position is (10, 20). The other events
				// arrive later in the notification stream, but their timestamps
				// must not decide whether they fall before or after this boundary.
				add("$receipt", 20, 10, tc.originTS[0])
				_, err := db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", roomID, "$receipt", "")
				require.NoError(t, err)
				add("$before", 21, 9, tc.originTS[1])
				add("$same-depth-before", 19, 10, tc.originTS[2])
				add("$after", 23, 11, tc.originTS[3])

				total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", roomID)
				require.NoError(t, err)
				require.EqualValues(t, 1, total, "only the event after the receipt should remain unread")
			})
		})
	}
}

func TestNotificationReceiptHistoricalRowsFollowOriginOrder(t *testing.T) {
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
		insert := func(db *shared.Database, eventID string, pos int, ts spec.Timestamp) {
			t.Helper()
			require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", eventID, uint64(pos), nil,
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`), OriginServerTS: ts}}))
		}

		db := open()
		insert(db, "$old", 1, 100)
		insert(db, "$old-after", 2, 350)
		// Recreate the state of a database from before the room-order
		// migration, so the notification events are rebuilt from the
		// notifications and their depths are marked unknown.
		_, err := db.DB.ExecContext(t.Context(), `DROP TABLE userapi_notification_events;
   DROP TABLE userapi_pending_notification_receipts;
   DROP TABLE userapi_notification_receipt_locks;
   DROP TABLE userapi_notification_receipt_positions;
   DELETE FROM db_migrations WHERE version IN (
     'userapi: notification event threads',
     'userapi: notification event origin timestamp',
     'userapi: notification receipt positions',
			 'userapi: notification room order',
     'userapi: notification room order recovery');`)
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		defer func() { require.NoError(t, db.DB.Close()) }()

		var depth int64
		require.NoError(t, db.DB.QueryRowContext(t.Context(), `SELECT room_depth FROM userapi_notification_events
   WHERE room_id = '!room:test' AND event_id = '$old'`).Scan(&depth))
		require.EqualValues(t, -1, depth, "historical rows must not claim the start of the room")

		// A later event carries a real room depth. Historical timestamps cannot
		// determine which side of this receipt those events occupy.
		insert(db, "$receipt", 3, 300)
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$receipt", "main", 3, 10, 300)
		require.NoError(t, err)
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$receipt", "")
		require.NoError(t, err)
		// Until an event is reprocessed its depth is unknown, so the receipt
		// covers it by stream position (the only order legacy rows have). That
		// already marked $old-after read; reprocessing cannot unread it.
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$old", "main", 1, 8, 100)
		require.NoError(t, err)
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$old-after", "main", 2, 12, 350)
		require.NoError(t, err)

		insert(db, "$after", 4, 400)
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$after", "main", 4, 11, 400)
		require.NoError(t, err)

		total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.EqualValues(t, 1, total, "legacy rows are covered by stream position; only the later event stays unread")
	})
}

// After an upgrade, retained notification events have no room depth until they
// are reprocessed. A receipt on a newer event must still clear those older
// notifications instead of leaving their unread badge stuck.
func TestNotificationReceiptClearsLegacyEventsAfterUpgrade(t *testing.T) {
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
		insert := func(db *shared.Database, eventID string, pos int) {
			t.Helper()
			require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", eventID, uint64(pos), nil,
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
		}

		db := open()
		insert(db, "$old", 1)
		insert(db, "$older", 2)
		// Recreate a pre-upgrade database, so the retained events have an
		// unknown room depth and their order cannot be reconstructed.
		_, err := db.DB.ExecContext(t.Context(), `DROP TABLE userapi_notification_events;
   DROP TABLE userapi_pending_notification_receipts;
   DROP TABLE userapi_notification_receipt_locks;
   DROP TABLE userapi_notification_receipt_positions;
   DELETE FROM db_migrations WHERE version IN (
     'userapi: notification event threads',
     'userapi: notification event origin timestamp',
     'userapi: notification receipt positions',
     'userapi: notification room order',
     'userapi: notification room order recovery');`)
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		defer func() { require.NoError(t, db.DB.Close()) }()

		insert(db, "$new", 3)
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$new", "main", 3, 10, 300)
		require.NoError(t, err)
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$new", "")
		require.NoError(t, err)

		total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.Zero(t, total, "the newer receipt clears the retained notifications")
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
				&api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(content), OriginServerTS: spec.Timestamp(100 + i)}}))
		}
		// Recreate a database from before the new migrations, retaining old notifications.
		_, err := db.DB.ExecContext(t.Context(), `DROP TABLE userapi_notification_events;
   DROP TABLE userapi_pending_notification_receipts;
   DROP TABLE userapi_notification_receipt_locks;
   DROP TABLE userapi_notification_receipt_positions;
   DELETE FROM db_migrations WHERE version IN (
     'userapi: notification event threads',
     'userapi: notification event origin timestamp',
     'userapi: notification receipt positions',
	     'userapi: notification room order',
     'userapi: notification room order recovery');`)
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		var originTS int64
		require.NoError(t, db.DB.QueryRowContext(t.Context(), `SELECT origin_ts FROM userapi_notification_events WHERE room_id = '!room:test' AND event_id = '$event2'`).Scan(&originTS))
		require.EqualValues(t, 102, originTS)
		// The receipt's event has no room depth yet, so it can only clear
		// itself. The older events stay unread until their depths recover.
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$event2", "$root")
		require.NoError(t, err)
		total, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.EqualValues(t, 2, total)
		_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", "!room:test", "$future", "main")
		require.NoError(t, err)
		require.NoError(t, db.DB.Close())
		db = open()
		defer func() { require.NoError(t, db.DB.Close()) }()
		// Reprocessing recovers the room order the migration could not
		// reconstruct. The recovered $root receipt then clears $event1 too.
		for i := 0; i < 3; i++ {
			_, err = db.StoreNotificationEvent(t.Context(), "!room:test", fmt.Sprintf("$event%d", i), map[int]string{0: "main", 1: "$root", 2: "$root"}[i], uint64(i+1), int64(i+1), int64(100+i))
			require.NoError(t, err)
		}
		total, _, err = db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.EqualValues(t, 1, total)
		users, err := db.StoreNotificationEvent(t.Context(), "!room:test", "$future", "main", 4, 4, 0)
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
				_, err := db.StoreNotificationEvent(t.Context(), "!room:test", eventID, "$thread", uint64(i+1), int64(i+1), 0)
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

func TestDeleteNotificationsForEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()
		insert := func(localpart, roomID, eventID string, pos int, highlight bool) {
			require.NoError(t, db.InsertNotification(ctx, localpart, "localhost", eventID, uint64(pos), map[string]any{"highlight": highlight}, &api.Notification{RoomID: roomID, Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
		}
		insert("alice", "!room:test", "$rejected", 1, true)
		insert("bob", "!room:test", "$rejected", 1, false)
		insert("alice", "!room:test", "$kept", 2, false)
		insert("alice", "!other:test", "$rejected", 1, true)
		_, err := db.StoreNotificationEvent(ctx, "!room:test", "$rejected", "$thread", 1, 1, 0)
		require.NoError(t, err)
		_, err = db.StoreNotificationEvent(ctx, "!room:test", "$kept", "main", 2, 2, 0)
		require.NoError(t, err)

		users, err := db.DeleteNotificationsForEvent(ctx, "!room:test", "$rejected")
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"@alice:localhost", "@bob:localhost"}, users)
		pending, err := db.PendingNotificationRefreshes(ctx, "!room:test")
		require.NoError(t, err)
		require.ElementsMatch(t, users, pending)
		require.NoError(t, db.CompleteNotificationRefresh(ctx, "!room:test", "@alice:localhost"))
		pending, err = db.PendingNotificationRefreshes(ctx, "!room:test")
		require.NoError(t, err)
		require.Equal(t, []string{"@bob:localhost"}, pending)

		total, highlights, err := db.GetRoomNotificationCounts(ctx, "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.Equal(t, int64(1), total)
		require.Zero(t, highlights)
		counts, err := db.GetRoomThreadNotificationCounts(ctx, "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.Equal(t, map[string]eventutil.ThreadNotificationCounts{
			"main": {UnreadNotificationCount: 1},
		}, counts)

		total, highlights, err = db.GetRoomNotificationCounts(ctx, "bob", "localhost", "!room:test")
		require.NoError(t, err)
		require.Zero(t, total)
		require.Zero(t, highlights)

		// The same event ID in another room is unaffected.
		total, highlights, err = db.GetRoomNotificationCounts(ctx, "alice", "localhost", "!other:test")
		require.NoError(t, err)
		require.Equal(t, int64(1), total)
		require.Equal(t, int64(1), highlights)
	})
}

func TestRoomThreadNotificationCounts(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()
		notify := func(i int, thread string, highlight bool) {
			eventID := fmt.Sprintf("$event%d", i)
			require.NoError(t, db.InsertNotification(ctx, "alice", "localhost", eventID, uint64(i+1), map[string]any{"highlight": highlight}, &api.Notification{RoomID: "!room:test", Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
			if thread != "" {
				_, err := db.StoreNotificationEvent(ctx, "!room:test", eventID, thread, uint64(i+1), int64(i+1), 0)
				require.NoError(t, err)
			}
		}
		notify(0, "main", false)
		notify(1, "$a", true)
		notify(2, "$a", false)
		notify(3, "$b", false)
		// A notification whose thread is not recorded yet belongs to the main timeline.
		notify(4, "", true)
		counts, err := db.GetRoomThreadNotificationCounts(ctx, "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.Equal(t, map[string]eventutil.ThreadNotificationCounts{
			"main": {UnreadNotificationCount: 2, UnreadHighlightCount: 1},
			"$a":   {UnreadNotificationCount: 2, UnreadHighlightCount: 1},
			"$b":   {UnreadNotificationCount: 1},
		}, counts)
		// A threaded receipt only clears its own thread.
		_, err = db.SetNotificationsReadForReceipt(ctx, "alice", "localhost", "!room:test", "$event2", "$a")
		require.NoError(t, err)
		counts, err = db.GetRoomThreadNotificationCounts(ctx, "alice", "localhost", "!room:test")
		require.NoError(t, err)
		require.NotContains(t, counts, "$a")
		require.Equal(t, 2, counts["main"].UnreadNotificationCount)
		counts, err = db.GetRoomThreadNotificationCounts(ctx, "bob", "localhost", "!room:test")
		require.NoError(t, err)
		require.Empty(t, counts)
	})
}

// Redelivering an event must not move the position first recorded for it.
func TestNotificationEventKeepsFirstKnownPosition(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		db, cleanup := mustCreateUserDatabase(t, backend)
		defer cleanup()
		_, err := db.StoreNotificationEvent(t.Context(), "!room:test", "$e", "main", 5, 50, 1)
		require.NoError(t, err)
		_, err = db.StoreNotificationEvent(t.Context(), "!room:test", "$e", "main", 9, 90, 1)
		require.NoError(t, err)
		var depth, pos int64
		require.NoError(t, db.(*shared.Database).DB.QueryRowContext(t.Context(), `SELECT room_depth, stream_pos FROM userapi_notification_events WHERE room_id = '!room:test' AND event_id = '$e'`).Scan(&depth, &pos))
		require.EqualValues(t, 50, depth)
		require.EqualValues(t, 5, pos)
	})
}
