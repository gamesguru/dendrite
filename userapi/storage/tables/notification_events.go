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
