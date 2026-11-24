package storage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage/shared"
)

func TestUnknownDepthReceiptPreservesAndRecoversKnownBoundary(t *testing.T) {
	db, cleanup := mustCreateUserDatabase(t, test.DBTypeSQLite)
	defer cleanup()
	sharedDB, ok := db.(*shared.Database)
	if !ok {
		t.Fatalf("expected *shared.Database, got %T", db)
	}
	roomID := "!room:test"

	_, err := db.StoreNotificationEvent(t.Context(), roomID, "$known", "main", 10, 100, 1)
	require.NoError(t, err)
	require.NoError(t, db.InsertNotification(t.Context(), "alice", "localhost", "$unknown", 1, nil,
		&api.Notification{RoomID: roomID, Event: synctypes.ClientEvent{Content: []byte(`{}`)}}))
	_, err = db.StoreNotificationEvent(t.Context(), roomID, "$unknown", "main", 20, -1, 2)
	require.NoError(t, err)

	_, err = db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", roomID, "$known", "")
	require.NoError(t, err)
	changed, err := db.SetNotificationsReadForReceipt(t.Context(), "alice", "localhost", roomID, "$unknown", "")
	require.NoError(t, err)
	require.True(t, changed, "the exact unknown-depth receipt target must be acknowledged")

	var boundaryEvent string
	var boundaryDepth, boundaryPos int64
	require.NoError(t, sharedDB.DB.QueryRowContext(t.Context(), `SELECT receipt_event_id, room_depth, stream_pos
			FROM userapi_notification_receipt_positions WHERE room_id = $1 AND localpart = 'alice'`, roomID).
		Scan(&boundaryEvent, &boundaryDepth, &boundaryPos))
	require.Equal(t, "$known", boundaryEvent)
	require.EqualValues(t, 100, boundaryDepth)
	require.EqualValues(t, 10, boundaryPos)
	var pending int
	require.NoError(t, sharedDB.DB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM userapi_pending_notification_receipts
			WHERE room_id = $1 AND event_id = '$unknown' AND localpart = 'alice'`, roomID).Scan(&pending))
	require.Equal(t, 1, pending, "the receipt must remain available for boundary recovery")

	_, err = db.StoreNotificationEvent(t.Context(), roomID, "$unknown", "main", 20, 101, 2)
	require.NoError(t, err)
	require.NoError(t, sharedDB.DB.QueryRowContext(t.Context(), `SELECT receipt_event_id, room_depth, stream_pos
			FROM userapi_notification_receipt_positions WHERE room_id = $1 AND localpart = 'alice'`, roomID).
		Scan(&boundaryEvent, &boundaryDepth, &boundaryPos))
	require.Equal(t, "$unknown", boundaryEvent)
	require.EqualValues(t, 101, boundaryDepth)
	require.EqualValues(t, 20, boundaryPos)
	unread, _, err := db.GetRoomNotificationCounts(t.Context(), "alice", "localhost", roomID)
	require.NoError(t, err)
	require.Zero(t, unread)
}
