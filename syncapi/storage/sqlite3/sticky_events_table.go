// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const stickyEventsSchema = `
-- Tracks MSC4354 sticky events in their own stream, so that sync can deliver
-- unexpired sticky events that fall outside the timeline. Rows are removed
-- when the event is redacted, and some time after they expire.
CREATE TABLE IF NOT EXISTS syncapi_sticky_events (
	-- The position in the sticky events stream
	id INTEGER PRIMARY KEY,
	-- The room the event is in
	room_id TEXT NOT NULL,
	-- The sticky event
	event_id TEXT NOT NULL UNIQUE,
	-- The user ID of the sender, so that ignored users can be left out
	sender TEXT NOT NULL,
	-- When the event stops being sticky, in milliseconds since the epoch
	expires_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS syncapi_sticky_events_room_id_idx ON syncapi_sticky_events(room_id, id);
CREATE INDEX IF NOT EXISTS syncapi_sticky_events_expires_at_idx ON syncapi_sticky_events(expires_at);
`

const insertStickyEventSQL = "" +
	"INSERT INTO syncapi_sticky_events (id, room_id, event_id, sender, expires_at)" +
	" VALUES ($1, $2, $3, $4, $5)" +
	" ON CONFLICT (event_id) DO NOTHING"

// The room IDs replace ($4), and the limit, where -1 returns every row,
// follows them, so that placeholders appear in ascending order.
const selectStickyEventsInRoomsSQL = "" +
	"SELECT id, room_id, event_id, sender, expires_at FROM syncapi_sticky_events" +
	" WHERE id > $1 AND id <= $2 AND expires_at > $3 AND room_id IN ($4)" +
	" ORDER BY id ASC LIMIT %s"

// The event IDs replace ($2).
const selectStickyEventsByIDSQL = "" +
	"SELECT id, room_id, event_id, sender, expires_at FROM syncapi_sticky_events" +
	" WHERE expires_at > $1 AND event_id IN ($2)"

// The stream ID counter rather than the table, whose rows are deleted, holds
// the latest position.
const selectMaxStickyEventIDSQL = "" +
	"SELECT stream_id FROM syncapi_stream_id WHERE stream_name = 'sticky'"

const selectStickyEventPositionSQL = "" +
	"SELECT id FROM syncapi_sticky_events WHERE event_id = $1"

const deleteStickyEventSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE event_id = $1"

const deleteExpiredStickyEventsSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE expires_at <= $1"

const purgeStickyEventsSQL = "" +
	"DELETE FROM syncapi_sticky_events WHERE room_id = $1"

type stickyEventsStatements struct {
	db                            *sql.DB
	streamIDStatements            *StreamIDStatements
	insertStickyEventStmt         *sql.Stmt
	selectStickyEventPositionStmt *sql.Stmt
	selectMaxStickyEventIDStmt    *sql.Stmt
	deleteStickyEventStmt         *sql.Stmt
	deleteExpiredStickyEventsStmt *sql.Stmt
	purgeStickyEventsStmt         *sql.Stmt
}

func NewSqliteStickyEventsTable(db *sql.DB, streamID *StreamIDStatements) (tables.StickyEvents, error) {
	if _, err := db.Exec(stickyEventsSchema); err != nil {
		return nil, err
	}
	s := &stickyEventsStatements{
		db:                 db,
		streamIDStatements: streamID,
	}
	return s, sqlutil.StatementList{
		{&s.insertStickyEventStmt, insertStickyEventSQL},
		{&s.selectStickyEventPositionStmt, selectStickyEventPositionSQL},
		{&s.selectMaxStickyEventIDStmt, selectMaxStickyEventIDSQL},
		{&s.deleteStickyEventStmt, deleteStickyEventSQL},
		{&s.deleteExpiredStickyEventsStmt, deleteExpiredStickyEventsSQL},
		{&s.purgeStickyEventsStmt, purgeStickyEventsSQL},
	}.Prepare(db)
}

func (s *stickyEventsStatements) InsertStickyEvent(
	ctx context.Context, txn *sql.Tx, roomID, eventID, sender string, expiresAt spec.Timestamp,
) (types.StreamPosition, error) {
	pos, err := s.streamIDStatements.nextStickyID(ctx, txn)
	if err != nil {
		return 0, err
	}
	res, err := sqlutil.TxStmt(txn, s.insertStickyEventStmt).ExecContext(ctx, pos, roomID, eventID, sender, int64(expiresAt))
	if err != nil {
		return 0, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if inserted == 0 {
		// An event already in the stream keeps its position.
		err = sqlutil.TxStmt(txn, s.selectStickyEventPositionStmt).QueryRowContext(ctx, eventID).Scan(&pos)
	}
	return pos, err
}

func (s *stickyEventsStatements) SelectStickyEventsInRooms(
	ctx context.Context, txn *sql.Tx, roomIDs []string, from, to types.StreamPosition, now spec.Timestamp, limit int,
) ([]types.StickyEvent, error) {
	if len(roomIDs) == 0 || to <= from {
		return nil, nil
	}
	const fixedParams = 3
	query := fmt.Sprintf(selectStickyEventsInRoomsSQL, fmt.Sprintf("$%d", fixedParams+len(roomIDs)+1))
	query = strings.Replace(query, "($4)", sqlutil.QueryVariadicOffset(len(roomIDs), fixedParams), 1)
	params := make([]any, 0, fixedParams+len(roomIDs)+1)
	params = append(params, from, to, int64(now))
	for _, roomID := range roomIDs {
		params = append(params, roomID)
	}
	if limit <= 0 {
		limit = -1
	}
	params = append(params, limit)
	return s.queryStickyEvents(ctx, txn, query, params, "SelectStickyEventsInRooms")
}

func (s *stickyEventsStatements) SelectStickyEventsByID(
	ctx context.Context, txn *sql.Tx, eventIDs []string, now spec.Timestamp,
) ([]types.StickyEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	query := strings.Replace(selectStickyEventsByIDSQL, "($2)", sqlutil.QueryVariadicOffset(len(eventIDs), 1), 1)
	params := make([]any, 0, 1+len(eventIDs))
	params = append(params, int64(now))
	for _, eventID := range eventIDs {
		params = append(params, eventID)
	}
	return s.queryStickyEvents(ctx, txn, query, params, "SelectStickyEventsByID")
}

func (s *stickyEventsStatements) queryStickyEvents(
	ctx context.Context, txn *sql.Tx, query string, params []any, name string,
) ([]types.StickyEvent, error) {
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, query, params...)
	} else {
		rows, err = s.db.QueryContext(ctx, query, params...)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, name+": rows.close() failed")
	var events []types.StickyEvent
	for rows.Next() {
		var ev types.StickyEvent
		var expiresAt int64
		if err = rows.Scan(&ev.Position, &ev.RoomID, &ev.EventID, &ev.Sender, &expiresAt); err != nil {
			return nil, err
		}
		ev.ExpiresAt = spec.Timestamp(expiresAt)
		events = append(events, ev)
	}
	return events, rows.Err()
}

func (s *stickyEventsStatements) SelectMaxStickyEventID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	err = sqlutil.TxStmt(txn, s.selectMaxStickyEventIDStmt).QueryRowContext(ctx).Scan(&id)
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
