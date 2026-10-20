// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const stickyEventsSchema = `
CREATE SEQUENCE IF NOT EXISTS syncapi_sticky_event_id;

-- Tracks MSC4354 sticky events in their own stream, so that sync can deliver
-- unexpired sticky events that fall outside the timeline. Rows are removed
-- when the event is redacted, and some time after they expire.
CREATE TABLE IF NOT EXISTS syncapi_sticky_events (
	-- The position in the sticky events stream
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_sticky_event_id'),
	-- The room the event is in
	room_id TEXT NOT NULL,
	-- The sticky event
	event_id TEXT NOT NULL,
	-- The user ID of the sender, so that ignored users can be left out
	sender TEXT NOT NULL,
	-- When the event stops being sticky, in milliseconds since the epoch
	expires_at BIGINT NOT NULL,
	CONSTRAINT syncapi_sticky_events_event_id_unique UNIQUE (event_id)
);
CREATE INDEX IF NOT EXISTS syncapi_sticky_events_room_id_idx ON syncapi_sticky_events(room_id, id);
CREATE INDEX IF NOT EXISTS syncapi_sticky_events_expires_at_idx ON syncapi_sticky_events(expires_at);
`

const insertStickyEventSQL = "" +
	"INSERT INTO syncapi_sticky_events (room_id, event_id, sender, expires_at)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT ON CONSTRAINT syncapi_sticky_events_event_id_unique" +
	" DO UPDATE SET event_id = EXCLUDED.event_id" +
	" RETURNING id"

// A NULL limit returns every row.
const selectStickyEventsInRoomsSQL = "" +
	"SELECT id, room_id, event_id, sender, expires_at FROM syncapi_sticky_events" +
	" WHERE id > $1 AND id <= $2 AND expires_at > $3 AND room_id = ANY($4)" +
	" ORDER BY id ASC LIMIT $5"

const selectStickyEventsByIDSQL = "" +
	"SELECT id, room_id, event_id, sender, expires_at FROM syncapi_sticky_events" +
	" WHERE expires_at > $1 AND event_id = ANY($2)"

// The sequence rather than the table, whose rows are deleted, holds the
// latest position.
const selectMaxStickyEventIDSQL = "" +
	"SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM syncapi_sticky_event_id"

const deleteStickyEventSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE event_id = $1"

const deleteExpiredStickyEventsSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE expires_at <= $1"

const purgeStickyEventsSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE room_id = $1"

type stickyEventsStatements struct {
	insertStickyEventStmt         *sql.Stmt
	selectStickyEventsInRoomsStmt *sql.Stmt
	selectStickyEventsByIDStmt    *sql.Stmt
	selectMaxStickyEventIDStmt    *sql.Stmt
	deleteStickyEventStmt         *sql.Stmt
	deleteExpiredStickyEventsStmt *sql.Stmt
	purgeStickyEventsStmt         *sql.Stmt
}

func NewPostgresStickyEventsTable(db *sql.DB) (tables.StickyEvents, error) {
	if _, err := db.Exec(stickyEventsSchema); err != nil {
		return nil, err
	}
	s := &stickyEventsStatements{}
	return s, sqlutil.StatementList{
		{&s.insertStickyEventStmt, insertStickyEventSQL},
		{&s.selectStickyEventsInRoomsStmt, selectStickyEventsInRoomsSQL},
		{&s.selectStickyEventsByIDStmt, selectStickyEventsByIDSQL},
		{&s.selectMaxStickyEventIDStmt, selectMaxStickyEventIDSQL},
		{&s.deleteStickyEventStmt, deleteStickyEventSQL},
		{&s.deleteExpiredStickyEventsStmt, deleteExpiredStickyEventsSQL},
		{&s.purgeStickyEventsStmt, purgeStickyEventsSQL},
	}.Prepare(db)
}

func (s *stickyEventsStatements) InsertStickyEvent(
	ctx context.Context, txn *sql.Tx, roomID, eventID, sender string, expiresAt spec.Timestamp,
) (pos types.StreamPosition, err error) {
	stmt := sqlutil.TxStmt(txn, s.insertStickyEventStmt)
	// An event already in the stream keeps its position.
	err = stmt.QueryRowContext(ctx, roomID, eventID, sender, int64(expiresAt)).Scan(&pos)
	return pos, err
}

func (s *stickyEventsStatements) SelectStickyEventsInRooms(
	ctx context.Context, txn *sql.Tx, roomIDs []string, from, to types.StreamPosition, now spec.Timestamp, limit int,
) ([]types.StickyEvent, error) {
	if len(roomIDs) == 0 || to <= from {
		return nil, nil
	}
	var sqlLimit sql.NullInt64
	if limit > 0 {
		sqlLimit = sql.NullInt64{Int64: int64(limit), Valid: true}
	}
	stmt := sqlutil.TxStmt(txn, s.selectStickyEventsInRoomsStmt)
	rows, err := stmt.QueryContext(ctx, from, to, int64(now), roomIDs, sqlLimit)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectStickyEventsInRooms: rows.close() failed")
	return scanStickyEvents(rows)
}

func (s *stickyEventsStatements) SelectStickyEventsByID(
	ctx context.Context, txn *sql.Tx, eventIDs []string, now spec.Timestamp,
) ([]types.StickyEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	stmt := sqlutil.TxStmt(txn, s.selectStickyEventsByIDStmt)
	rows, err := stmt.QueryContext(ctx, int64(now), eventIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectStickyEventsByID: rows.close() failed")
	return scanStickyEvents(rows)
}

func (s *stickyEventsStatements) SelectMaxStickyEventID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMaxStickyEventIDStmt)
	err = stmt.QueryRowContext(ctx).Scan(&id)
	return
}

func (s *stickyEventsStatements) DeleteStickyEvent(
	ctx context.Context, txn *sql.Tx, eventID string,
) error {
	_, err := sqlutil.TxStmt(txn, s.deleteStickyEventStmt).ExecContext(ctx, eventID)
	return err
}

func (s *stickyEventsStatements) DeleteExpiredStickyEvents(
	ctx context.Context, txn *sql.Tx, now spec.Timestamp,
) error {
	_, err := sqlutil.TxStmt(txn, s.deleteExpiredStickyEventsStmt).ExecContext(ctx, int64(now))
	return err
}

func (s *stickyEventsStatements) PurgeStickyEvents(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	_, err := sqlutil.TxStmt(txn, s.purgeStickyEventsStmt).ExecContext(ctx, roomID)
	return err
}

func scanStickyEvents(rows *sql.Rows) ([]types.StickyEvent, error) {
	var events []types.StickyEvent
	for rows.Next() {
		var ev types.StickyEvent
		var expiresAt int64
		if err := rows.Scan(&ev.Position, &ev.RoomID, &ev.EventID, &ev.Sender, &expiresAt); err != nil {
			return nil, err
		}
		ev.ExpiresAt = spec.Timestamp(expiresAt)
		events = append(events, ev)
	}
	return events, rows.Err()
}
