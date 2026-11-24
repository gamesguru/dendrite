package shared

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

// NotificationEventDepths returns effective room depths already recorded for
// the requested events. Events without a known effective depth are omitted.
func (d *Database) NotificationEventDepths(ctx context.Context, roomID string, eventIDs []string) (depths map[string]int64, err error) {
	depths = make(map[string]int64)
	if len(eventIDs) == 0 {
		return depths, nil
	}
	args := make([]any, 1, len(eventIDs)+1)
	args[0] = roomID
	placeholders := make([]string, len(eventIDs))
	for i, eventID := range eventIDs {
		args = append(args, eventID)
		placeholders[i] = fmt.Sprintf("$%d", len(args))
	}
	rows, err := d.DB.QueryContext(ctx, `SELECT event_id, room_depth FROM userapi_notification_events WHERE room_id = $1 AND event_id IN (`+strings.Join(placeholders, ",")+")", args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var eventID string
		var roomDepth int64
		if err = rows.Scan(&eventID, &roomDepth); err != nil {
			return nil, err
		}
		if roomDepth > 0 {
			depths[eventID] = roomDepth
		}
	}
	return depths, rows.Err()
}

// StoreNotificationEvent records ordering and thread identity after notification processing.
// Events without notifications are included so receipts on one's own events work too.
// It applies receipts which arrived first and returns users whose counts changed.
//
// The event's room depth and output stream position form the persisted
// room-order boundary. Event timestamps are not ordered reliably, and
// notification processing can be late. Historical rows remain unordered until reprocessed with their real room position.
func (d *Database) StoreNotificationEvent(ctx context.Context, roomID, eventID, threadID string, pos uint64, roomDepth, originTS int64) (users []string, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := lockNotificationReceipts(ctx, txn, roomID); err != nil {
			return err
		}
		// A known position recovers a historical row; an unknown one never
		// overwrites it. SQLite binds $N in textual order, so $5 is origin_ts
		// and $6 is room_depth.
		if _, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_events (room_id, event_id, thread_id, stream_pos, notification_id, origin_ts, room_depth)
	VALUES ($1, $2, $3, $4, (SELECT COALESCE(MAX(id), 0) FROM userapi_notifications WHERE room_id = $1), $5, $6)
	ON CONFLICT (room_id, event_id) DO UPDATE SET
		room_depth = CASE WHEN userapi_notification_events.room_depth <= 0 AND EXCLUDED.room_depth > 0 THEN EXCLUDED.room_depth ELSE userapi_notification_events.room_depth END,
		stream_pos = CASE WHEN userapi_notification_events.room_depth <= 0 AND EXCLUDED.room_depth > 0 THEN EXCLUDED.stream_pos ELSE userapi_notification_events.stream_pos END,
		origin_ts = CASE WHEN EXCLUDED.origin_ts > 0 THEN EXCLUDED.origin_ts ELSE userapi_notification_events.origin_ts END`,
			roomID, eventID, threadID, pos, originTS, roomDepth); err != nil {
			return err
		}
		var eventDepth, eventStreamPos int64
		if err := txn.QueryRowContext(ctx, `SELECT room_depth, stream_pos FROM userapi_notification_events WHERE room_id = $1 AND event_id = $2`, roomID, eventID).Scan(&eventDepth, &eventStreamPos); err != nil {
			return err
		}
		if eventDepth > 0 {
			if _, err := txn.ExecContext(ctx, `UPDATE userapi_notification_receipt_positions SET room_depth = $1, stream_pos = $2 WHERE room_id = $3 AND receipt_event_id = $4 AND room_depth <= 0`, eventDepth, eventStreamPos, roomID, eventID); err != nil {
				return err
			}
		}
		// Placeholders must appear in ascending order: SQLite binds them in
		// textual order rather than by number.
		rows, err := txn.QueryContext(ctx, `SELECT rp.localpart, rp.server_name, rp.thread_id, rp.room_depth, rp.stream_pos, rp.receipt_event_id
	FROM userapi_notification_receipt_positions rp
	JOIN userapi_notification_events event ON event.room_id = rp.room_id
	WHERE rp.room_id = $1 AND (rp.thread_id = '' OR rp.thread_id = $2)
	AND event.event_id = $3
	AND rp.room_depth > 0 AND event.room_depth > 0`, roomID, threadID, eventID)
		if err != nil {
			return err
		}
		type receiptPosition struct {
			localpart, server, thread string
			roomDepth, streamPos      int64
			eventID                   string
		}
		var receiptPositions []receiptPosition
		for rows.Next() {
			var receipt receiptPosition
			if err = rows.Scan(&receipt.localpart, &receipt.server, &receipt.thread, &receipt.roomDepth, &receipt.streamPos, &receipt.eventID); err != nil {
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
			if receipt.roomDepth < eventDepth || (receipt.roomDepth == eventDepth && receipt.streamPos < eventStreamPos) {
				continue
			}
			changed, updateErr := markNotificationsReadThrough(ctx, txn, receipt.localpart, spec.ServerName(receipt.server), roomID, receipt.roomDepth, receipt.streamPos, receipt.eventID, receipt.thread)
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
	var roomDepth, streamPos int64
	if err := txn.QueryRowContext(ctx, `SELECT room_depth, stream_pos FROM userapi_notification_events WHERE room_id = $1 AND event_id = $2`, roomID, eventID).Scan(&roomDepth, &streamPos); err != nil {
		return false, err
	}
	if _, err := txn.ExecContext(ctx, `INSERT INTO userapi_notification_receipt_positions
	(room_id, localpart, server_name, thread_id, origin_ts, notification_id, room_depth, stream_pos, receipt_event_id)
	SELECT $1, $2, $3, $4, origin_ts, notification_id, room_depth, stream_pos, $5 FROM userapi_notification_events WHERE room_id = $1 AND event_id = $5
	ON CONFLICT (room_id, localpart, server_name, thread_id) DO UPDATE SET
	room_depth = CASE WHEN excluded.room_depth > 0 AND (userapi_notification_receipt_positions.room_depth <= 0 OR excluded.room_depth > userapi_notification_receipt_positions.room_depth OR (excluded.room_depth = userapi_notification_receipt_positions.room_depth AND excluded.stream_pos > userapi_notification_receipt_positions.stream_pos)) THEN excluded.room_depth ELSE userapi_notification_receipt_positions.room_depth END,
	stream_pos = CASE WHEN excluded.room_depth > 0 AND (userapi_notification_receipt_positions.room_depth <= 0 OR excluded.room_depth > userapi_notification_receipt_positions.room_depth OR (excluded.room_depth = userapi_notification_receipt_positions.room_depth AND excluded.stream_pos > userapi_notification_receipt_positions.stream_pos)) THEN excluded.stream_pos ELSE userapi_notification_receipt_positions.stream_pos END,
	receipt_event_id = CASE WHEN excluded.room_depth > 0 AND (userapi_notification_receipt_positions.room_depth <= 0 OR excluded.room_depth > userapi_notification_receipt_positions.room_depth OR (excluded.room_depth = userapi_notification_receipt_positions.room_depth AND excluded.stream_pos >= userapi_notification_receipt_positions.stream_pos)) THEN excluded.receipt_event_id ELSE userapi_notification_receipt_positions.receipt_event_id END,
	origin_ts = excluded.origin_ts, notification_id = excluded.notification_id`, roomID, localpart, serverName, threadID, eventID); err != nil {
		return false, err
	}
	var savedDepth, savedPos int64
	var savedEvent string
	if err := txn.QueryRowContext(ctx, `SELECT room_depth, stream_pos, receipt_event_id FROM userapi_notification_receipt_positions WHERE room_id = $1 AND localpart = $2 AND server_name = $3 AND thread_id = $4`, roomID, localpart, serverName, threadID).Scan(&savedDepth, &savedPos, &savedEvent); err != nil {
		return false, err
	}
	changed, err := markNotificationsReadThrough(ctx, txn, localpart, serverName, roomID, savedDepth, savedPos, savedEvent, threadID)
	if err != nil {
		return false, err
	}
	if roomDepth <= 0 && savedDepth > 0 {
		// Preserve this receipt for the event's eventual depth recovery while
		// leaving the already-known high-water boundary untouched.
		if _, err = txn.ExecContext(ctx, `INSERT INTO userapi_pending_notification_receipts
			(room_id, event_id, localpart, server_name, thread_id, created_ts)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			roomID, eventID, localpart, serverName, threadID, time.Now().UnixMilli()); err != nil {
			return false, err
		}
	}
	if roomDepth > 0 || savedEvent == eventID {
		return changed, nil
	}
	// The receipt's own event has no known depth, so the saved boundary may
	// not cover it. Acknowledge that exact event without moving the boundary.
	exact, err := markNotificationsReadThrough(ctx, txn, localpart, serverName, roomID, -1, 0, eventID, threadID)
	return changed || exact, err
}

func markNotificationsReadThrough(ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, roomID string, roomDepth, streamPos int64, receiptEventID, threadID string) (bool, error) {
	query := `UPDATE userapi_notifications SET read = TRUE WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND NOT read AND event_id IN (SELECT event_id FROM userapi_notification_events WHERE room_id = $3 AND (($4 > 0 AND ((room_depth > 0 AND (room_depth < $4 OR (room_depth = $4 AND stream_pos <= $5))) OR (room_depth <= 0 AND stream_pos <= $5))) OR ($4 <= 0 AND event_id = $6))`
	args := []any{localpart, serverName, roomID, roomDepth, streamPos, receiptEventID}
	if threadID != "" {
		query += ` AND thread_id = $7`
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
