// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"strings"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func NewSqliteNotificationDataTable(ctx context.Context, db *sql.DB, streamID *StreamIDStatements) (tables.NotificationData, error) {
	_, err := db.Exec(notificationDataSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{Version: "syncapi: notification thread counts", Up: deltas.UpNotificationThreadCounts})
	if err = m.Up(ctx); err != nil {
		return nil, err
	}
	r := &notificationDataStatements{
		streamIDStatements: streamID,
		db:                 db,
	}
	return r, sqlutil.StatementList{
		{&r.upsertRoomUnreadCounts, upsertRoomUnreadNotificationCountsSQL},
		{&r.selectMaxID, selectMaxNotificationIDSQL},
		{&r.purgeNotificationData, purgeNotificationDataSQL},
		// {&r.selectUserUnreadCountsForRooms, selectUserUnreadNotificationsForRooms}, // used at runtime
	}.Prepare(db)
}

type notificationDataStatements struct {
	db                     *sql.DB
	streamIDStatements     *StreamIDStatements
	upsertRoomUnreadCounts *sql.Stmt
	selectMaxID            *sql.Stmt
	purgeNotificationData  *sql.Stmt
	// selectUserUnreadCountsForRooms *sql.Stmt
}

const notificationDataSchema = `
CREATE TABLE IF NOT EXISTS syncapi_notification_data (
	id INTEGER PRIMARY KEY,
	user_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	notification_count BIGINT NOT NULL DEFAULT 0,
	highlight_count BIGINT NOT NULL DEFAULT 0,
	CONSTRAINT syncapi_notifications_unique UNIQUE (user_id, room_id)
);`

const upsertRoomUnreadNotificationCountsSQL = `INSERT INTO syncapi_notification_data
  (user_id, room_id, notification_count, highlight_count, thread_counts)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (user_id, room_id)
  DO UPDATE SET id = $6, notification_count = $3, highlight_count = $4, thread_counts = $5`

const selectUserUnreadNotificationsForRooms = `SELECT room_id, notification_count, highlight_count, thread_counts
	FROM syncapi_notification_data
	WHERE user_id = $1 AND
	      room_id IN ($2)`

const selectMaxNotificationIDSQL = `SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE MAX(id) END FROM syncapi_notification_data`

const purgeNotificationDataSQL = "" +
	"DELETE FROM syncapi_notification_data WHERE room_id = $1"

func (r *notificationDataStatements) UpsertRoomUnreadCounts(ctx context.Context, txn *sql.Tx, userID string, data *eventutil.NotificationData) (pos types.StreamPosition, err error) {
	threads, err := tables.MarshalThreadCounts(data)
	if err != nil {
		return 0, err
	}
	pos, err = r.streamIDStatements.nextNotificationID(ctx, nil)
	if err != nil {
		return
	}
	_, err = r.upsertRoomUnreadCounts.ExecContext(ctx, userID, data.RoomID, data.UnreadNotificationCount, data.UnreadHighlightCount, threads, pos)
	return
}

func (r *notificationDataStatements) SelectUserUnreadCountsForRooms(
	ctx context.Context, txn *sql.Tx, userID string, roomIDs []string,
) (map[string]*eventutil.NotificationData, error) {
	params := make([]any, len(roomIDs)+1)
	params[0] = userID
	for i := range roomIDs {
		params[i+1] = roomIDs[i]
	}
	query := strings.Replace(selectUserUnreadNotificationsForRooms, "($2)", sqlutil.QueryVariadicOffset(len(roomIDs), 1), 1)
	prep, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, prep, "SelectUserUnreadCountsForRooms: prep.close() failed")
	selectStmt := sqlutil.TxStmt(txn, prep)
	rows, err := selectStmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectUserUnreadCountsForRooms: rows.close() failed")

	roomCounts := map[string]*eventutil.NotificationData{}
	var roomID string
	var notificationCount, highlightCount int
	var threads sql.NullString
	for rows.Next() {
		if err = rows.Scan(&roomID, &notificationCount, &highlightCount, &threads); err != nil {
			return nil, err
		}
		roomCounts[roomID], err = tables.UnmarshalNotificationData(roomID, notificationCount, highlightCount, threads)
		if err != nil {
			return nil, err
		}
	}
	return roomCounts, rows.Err()
}

func (r *notificationDataStatements) SelectMaxID(ctx context.Context, txn *sql.Tx) (int64, error) {
	var id int64
	selectStmt := sqlutil.TxStmt(txn, r.selectMaxID)
	err := selectStmt.QueryRowContext(ctx).Scan(&id)
	return id, err
}

func (s *notificationDataStatements) PurgeNotificationData(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	purgeNotificationData := sqlutil.TxStmt(txn, s.purgeNotificationData)
	_, err := purgeNotificationData.ExecContext(ctx, roomID)
	return err
}
