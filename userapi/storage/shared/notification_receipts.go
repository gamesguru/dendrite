package shared

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
)

// StoreNotificationEvent records ordering and thread identity after notification processing.
// Events without notifications are included so receipts on one's own events work too.
// It applies receipts which arrived first and returns users whose counts changed.
func (d *Database) StoreNotificationEvent(ctx context.Context, roomID, eventID, threadID string, pos uint64) (users []string, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := lockNotificationReceipts(ctx, txn, roomID); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_events (room_id, event_id, thread_id, stream_pos, notification_id)
	VALUES ($1, $2, $3, $4, (SELECT COALESCE(MAX(id), 0) FROM userapi_notifications WHERE room_id = $1)) ON CONFLICT (room_id, event_id) DO NOTHING`, roomID, eventID, threadID, pos); err != nil {
			return err
		}
		rows, err := txn.QueryContext(ctx, `SELECT localpart, server_name, thread_id FROM userapi_pending_notification_receipts WHERE room_id = $1 AND event_id = $2`, roomID, eventID)
		if err != nil {
			return err
		}
		type pendingReceipt struct{ localpart, server, thread string }
		var pending []pendingReceipt
		for rows.Next() {
			var receipt pendingReceipt
			if err = rows.Scan(&receipt.localpart, &receipt.server, &receipt.thread); err != nil {
				break
			}
			pending = append(pending, receipt)
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
		for _, receipt := range pending {
			changed, err := readNotificationReceipt(ctx, txn, receipt.localpart, spec.ServerName(receipt.server), roomID, eventID, receipt.thread)
			if err != nil {
				return err
			}
			if changed {
				users = append(users, "@"+receipt.localpart+":"+receipt.server)
			}
		}
		_, err = txn.ExecContext(ctx, `DELETE FROM userapi_pending_notification_receipts WHERE room_id = $1 AND event_id = $2`, roomID, eventID)
		return err
	})
	return
}

// SetNotificationsReadForReceipt marks only notifications covered by the receipt.
// Receipts arriving before their target event are retained for event processing.
func (d *Database) SetNotificationsReadForReceipt(ctx context.Context, localpart string, serverName spec.ServerName, roomID, eventID, threadID string) (affected bool, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := lockNotificationReceipts(ctx, txn, roomID); err != nil {
			return err
		}
		affected, err = readNotificationReceipt(ctx, txn, localpart, serverName, roomID, eventID, threadID)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = txn.ExecContext(ctx, `INSERT INTO userapi_pending_notification_receipts (room_id,event_id,localpart,server_name,thread_id,created_ts)
	VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, roomID, eventID, localpart, serverName, threadID, time.Now().UnixMilli())
		}
		return err
	})
	return
}

func readNotificationReceipt(ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, roomID, eventID, threadID string) (bool, error) {
	var notificationID int64
	if err := txn.QueryRowContext(ctx, `SELECT notification_id FROM userapi_notification_events WHERE room_id = $1 AND event_id = $2`, roomID, eventID).Scan(&notificationID); err != nil {
		return false, err
	}
	result, err := txn.ExecContext(ctx, `UPDATE userapi_notifications SET read = TRUE
	WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND id <= $4 AND NOT read
	AND ($5 = '' OR event_id IN (SELECT event_id FROM userapi_notification_events WHERE room_id = $3 AND thread_id = $5))`, localpart, serverName, roomID, notificationID, threadID)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}

// Serialize the receipt/event handoff in PostgreSQL as well as SQLite. Taking
// this row lock before either lookup prevents a receipt from being queued just
// after its event checked for pending receipts.
func lockNotificationReceipts(ctx context.Context, txn *sql.Tx, roomID string) error {
	_, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_receipt_locks(room_id) VALUES ($1)
	ON CONFLICT (room_id) DO UPDATE SET room_id = excluded.room_id`, roomID)
	return err
}

func cleanNotificationReceipts(ctx context.Context, txn *sql.Tx) error {
	cutoff := time.Now().AddDate(0, -1, 0).UnixMilli()
	if _, err := txn.ExecContext(ctx, `DELETE FROM userapi_pending_notification_receipts WHERE created_ts < $1`, cutoff); err != nil {
		return err
	}
	_, err := txn.ExecContext(ctx, `DELETE FROM userapi_notification_events WHERE stream_pos < $1
	AND NOT EXISTS (SELECT 1 FROM userapi_notifications WHERE userapi_notifications.room_id = userapi_notification_events.room_id AND userapi_notifications.event_id = userapi_notification_events.event_id)`, cutoff)
	return err
}
