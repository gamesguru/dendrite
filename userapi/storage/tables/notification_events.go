package tables

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

// CreateNotificationEvents preserves event ordering and thread identity for receipts.
// Existing notifications seed the table when upgrading a database.
func CreateNotificationEvents(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `CREATE TABLE userapi_notification_receipt_locks (room_id TEXT PRIMARY KEY);
	CREATE TABLE userapi_pending_notification_receipts (
	room_id TEXT NOT NULL, event_id TEXT NOT NULL, localpart TEXT NOT NULL, server_name TEXT NOT NULL, thread_id TEXT NOT NULL, created_ts BIGINT NOT NULL,
	PRIMARY KEY (room_id, event_id, localpart, server_name, thread_id));
	CREATE TABLE userapi_notification_events (
	room_id TEXT NOT NULL, event_id TEXT NOT NULL, thread_id TEXT NOT NULL, stream_pos BIGINT NOT NULL, notification_id BIGINT NOT NULL,
	PRIMARY KEY (room_id, event_id));
	CREATE INDEX userapi_notification_events_thread_idx ON userapi_notification_events(room_id, thread_id);`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, room_id, event_id, stream_pos, notification_json FROM userapi_notifications`)
	if err != nil {
		return err
	}
	type entry struct {
		roomID, eventID, parentID, threadID string
		pos                                 uint64
		notificationID                      int64
	}
	entries := make(map[[2]string]entry)
	for rows.Next() {
		var e entry
		var raw []byte
		if err = rows.Scan(&e.notificationID, &e.roomID, &e.eventID, &e.pos, &raw); err != nil {
			break
		}
		var n api.Notification
		if err = json.Unmarshal(raw, &n); err != nil {
			break
		}
		relation := gjson.GetBytes(n.Event.Content, "m\\.relates_to")
		e.parentID = relation.Get("event_id").Str
		if relation.Get("rel_type").Str == "m.thread" {
			e.threadID = e.parentID
		}
		key := [2]string{e.roomID, e.eventID}
		if old, exists := entries[key]; !exists || old.notificationID < e.notificationID {
			entries[key] = e
		}
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		threadID := e.threadID
		parent := e
		for range 2 {
			if threadID != "" {
				break
			}
			parent = entries[[2]string{e.roomID, parent.parentID}]
			threadID = parent.threadID
		}
		if threadID == "" {
			threadID = "main"
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO userapi_notification_events (room_id,event_id,thread_id,stream_pos,notification_id) VALUES ($1,$2,$3,$4,$5)`, e.roomID, e.eventID, threadID, e.pos, e.notificationID); err != nil {
			return err
		}
	}
	return nil
}

// CreateNotificationDepthCheckpoints stores effective depths for recovered
// ancestors so a bounded predecessor walk can reuse work on retry.
func CreateNotificationDepthCheckpoints(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE userapi_notification_depth_checkpoints (
		room_id TEXT NOT NULL, event_id TEXT NOT NULL, room_depth BIGINT NOT NULL, updated_ts BIGINT NOT NULL,
		PRIMARY KEY (room_id, event_id));`)
	return err
}

// CreatePendingNotificationRefreshes creates an outbox for notification count
// updates which must be published after notification rows have been deleted.
func CreatePendingNotificationRefreshes(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE userapi_pending_notification_refreshes (
	room_id TEXT NOT NULL, localpart TEXT NOT NULL, server_name TEXT NOT NULL,
	PRIMARY KEY (room_id, localpart, server_name));`)
	return err
}

// AddNotificationEventOriginTimestamp records the event's own timestamp.
// Notification rows can be inserted later than the events they describe
// (republished events, redelivery, retries), so both their ids and the output
// stream position cannot order a read receipt against them. The event's
// origin_server_ts is stable across reprocessing, so it can.
func AddNotificationEventOriginTimestamp(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `ALTER TABLE userapi_notification_events ADD COLUMN origin_ts BIGINT NOT NULL DEFAULT 0`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT room_id, event_id, notification_json FROM userapi_notifications`)
	if err != nil {
		return err
	}
	type notificationEvent struct {
		roomID, eventID string
		originTS        int64
	}
	var events []notificationEvent
	for rows.Next() {
		var roomID, eventID string
		var raw []byte
		if err = rows.Scan(&roomID, &eventID, &raw); err != nil {
			break
		}
		var notification api.Notification
		if err = json.Unmarshal(raw, &notification); err != nil {
			break
		}
		events = append(events, notificationEvent{roomID: roomID, eventID: eventID, originTS: int64(notification.Event.OriginServerTS)})
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err = tx.ExecContext(ctx, `UPDATE userapi_notification_events SET origin_ts = $1 WHERE room_id = $2 AND event_id = $3 AND origin_ts = 0`, event.originTS, event.roomID, event.eventID); err != nil {
			return err
		}
	}
	return nil
}

// CreateNotificationReceiptPositions stores the latest timeline position read
// by each user in a room/thread. It lets notification events processed late be
// marked read immediately instead of relying on notification insertion order.
func CreateNotificationReceiptPositions(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE userapi_notification_receipt_positions (
	room_id TEXT NOT NULL, localpart TEXT NOT NULL, server_name TEXT NOT NULL, thread_id TEXT NOT NULL,
	origin_ts BIGINT NOT NULL, notification_id BIGINT NOT NULL,
	PRIMARY KEY (room_id, localpart, server_name, thread_id));
	CREATE INDEX userapi_notification_receipt_positions_room_idx
	ON userapi_notification_receipt_positions(room_id, thread_id, origin_ts);`)
	return err
}

// AddNotificationRoomOrder stores the room topology depth used to compare
// receipt boundaries. Stream position breaks ties between events at the same
// depth; origin_server_ts is not an ordering key because it can tie or regress.
func AddNotificationRoomOrder(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `ALTER TABLE userapi_notification_events ADD COLUMN room_depth BIGINT NOT NULL DEFAULT -1;
	ALTER TABLE userapi_notification_receipt_positions ADD COLUMN room_depth BIGINT NOT NULL DEFAULT -1;
	ALTER TABLE userapi_notification_receipt_positions ADD COLUMN stream_pos BIGINT NOT NULL DEFAULT 0;`); err != nil {
		return err
	}
	return nil
}

// AddNotificationRoomOrderRecovery preserves legacy receipt boundaries as
// unknown until their event is reprocessed. Timestamps cannot reconstruct
// room order because they can tie or regress.
func AddNotificationRoomOrderRecovery(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `ALTER TABLE userapi_notification_receipt_positions ADD COLUMN receipt_event_id TEXT NOT NULL DEFAULT '';
	UPDATE userapi_notification_events SET room_depth = -1 WHERE room_depth = 0;
	UPDATE userapi_notification_receipt_positions SET room_depth = -1 WHERE room_depth = 0;`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE userapi_notification_receipt_positions SET receipt_event_id = COALESCE((
		SELECT MIN(event_id) FROM userapi_notification_events e
		WHERE e.room_id = userapi_notification_receipt_positions.room_id
		AND e.origin_ts = userapi_notification_receipt_positions.origin_ts
		AND e.notification_id = userapi_notification_receipt_positions.notification_id
		HAVING COUNT(*) = 1
	), ''), stream_pos = COALESCE((
		SELECT MIN(e.stream_pos) FROM userapi_notification_events e
		WHERE e.room_id = userapi_notification_receipt_positions.room_id
		AND e.origin_ts = userapi_notification_receipt_positions.origin_ts
		AND e.notification_id = userapi_notification_receipt_positions.notification_id
		HAVING COUNT(*) = 1
	), stream_pos) WHERE receipt_event_id = ''`)
	return err
}
