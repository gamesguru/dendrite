package shared

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

// StoreNotificationEvent records ordering and thread identity after notification processing.
// Events without notifications are included so receipts on one's own events work too.
// It applies receipts which arrived first and returns users whose counts changed.
// The originTS argument is the event's origin_server_ts, which stays the same if
// the event is processed again, unlike the notification id or the output stream
// position.
func (d *Database) StoreNotificationEvent(ctx context.Context, roomID, eventID, threadID string, pos uint64, originTS int64) (users []string, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := lockNotificationReceipts(ctx, txn, roomID); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_events (room_id, event_id, thread_id, stream_pos, notification_id, origin_ts)
	VALUES ($1, $2, $3, $4, (SELECT COALESCE(MAX(id), 0) FROM userapi_notifications WHERE room_id = $1), $5)
	ON CONFLICT (room_id, event_id) DO UPDATE SET origin_ts = EXCLUDED.origin_ts`, roomID, eventID, threadID, pos, originTS); err != nil {
			return err
		}
		var notificationID int64
		if err := txn.QueryRowContext(ctx, `SELECT notification_id FROM userapi_notification_events WHERE room_id = $1 AND event_id = $2`, roomID, eventID).Scan(&notificationID); err != nil {
			return err
		}
		// A receipt may have been recorded before this event's notification
		// was inserted. Apply every stored room/thread high-water mark now.
		// Events from before the origin timestamp migration fall back to the
		// old notification-id ordering.
		rows, err := txn.QueryContext(ctx, `SELECT localpart, server_name, thread_id, origin_ts, notification_id
		FROM userapi_notification_receipt_positions
		WHERE room_id = $1 AND (thread_id = '' OR thread_id = $2)
		AND (
			(CAST($3 AS BIGINT) > 0 AND origin_ts > 0 AND origin_ts >= CAST($3 AS BIGINT))
			OR ((CAST($3 AS BIGINT) = 0 OR origin_ts = 0) AND notification_id >= $4)
		)`, roomID, threadID, originTS, notificationID)
		if err != nil {
			return err
		}
		type receiptPosition struct {
			localpart, server, thread string
			originTS, notificationID  int64
		}
		var receiptPositions []receiptPosition
		for rows.Next() {
			var receipt receiptPosition
			if err = rows.Scan(&receipt.localpart, &receipt.server, &receipt.thread, &receipt.originTS, &receipt.notificationID); err != nil {
				break
			}
			receiptPositions = append(receiptPositions, receipt)
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
		changedUsers := make(map[string]struct{})
		for _, receipt := range receiptPositions {
			changed, updateErr := markNotificationsReadThrough(ctx, txn, receipt.localpart, spec.ServerName(receipt.server), roomID, receipt.originTS, receipt.notificationID, receipt.thread)
			if updateErr != nil {
				return updateErr
			}
			if changed {
				changedUsers["@"+receipt.localpart+":"+receipt.server] = struct{}{}
			}
		}
		rows, err = txn.QueryContext(ctx, `SELECT localpart, server_name, thread_id FROM userapi_pending_notification_receipts WHERE room_id = $1 AND event_id = $2`, roomID, eventID)
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
				changedUsers["@"+receipt.localpart+":"+receipt.server] = struct{}{}
			}
		}
		for userID := range changedUsers {
			users = append(users, userID)
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
	var originTS, notificationID int64
	if err := txn.QueryRowContext(ctx, `SELECT origin_ts, notification_id FROM userapi_notification_events WHERE room_id = $1 AND event_id = $2`, roomID, eventID).Scan(&originTS, &notificationID); err != nil {
		return false, err
	}
	if _, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_receipt_positions
	(room_id, localpart, server_name, thread_id, origin_ts, notification_id) VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (room_id, localpart, server_name, thread_id) DO UPDATE SET
	origin_ts = CASE WHEN userapi_notification_receipt_positions.origin_ts < excluded.origin_ts THEN excluded.origin_ts ELSE userapi_notification_receipt_positions.origin_ts END,
	notification_id = CASE WHEN userapi_notification_receipt_positions.notification_id < excluded.notification_id THEN excluded.notification_id ELSE userapi_notification_receipt_positions.notification_id END`, roomID, localpart, serverName, threadID, originTS, notificationID); err != nil {
		return false, err
	}
	return markNotificationsReadThrough(ctx, txn, localpart, serverName, roomID, originTS, notificationID, threadID)
}

func markNotificationsReadThrough(ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, roomID string, originTS, notificationID int64, threadID string) (bool, error) {
	// Named placeholders are bound in the order they first appear, so $4 and $5
	// must come before the optional $6. Keep this order.
	query := `UPDATE userapi_notifications SET read = TRUE
	WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND NOT read
	AND event_id IN (
		SELECT event_id FROM userapi_notification_events
		WHERE room_id = $3
		AND (
			(origin_ts > 0 AND origin_ts <= $4)
			OR (($4 = 0 OR origin_ts = 0) AND notification_id <= $5)
		)`
	args := []any{localpart, serverName, roomID, originTS, notificationID}
	if threadID != "" {
		query += ` AND thread_id = $6`
		args = append(args, threadID)
	}
	query += `)`
	result, err := txn.ExecContext(ctx, query, args...)
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
