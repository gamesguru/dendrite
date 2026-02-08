// Copyright 2018-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"

	"codefloe.com/pat-s/dendrite/internal"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/syncapi/storage/tables"
)

const backwardExtremitiesSchema = `
-- Stores output room events received from the roomserver.
CREATE TABLE IF NOT EXISTS syncapi_backward_extremities (
	-- The 'room_id' key for the event.
	room_id TEXT NOT NULL,
	-- The event ID for the last known event. This is the backwards extremity.
	event_id TEXT NOT NULL,
	-- The prev_events for the last known event. This is used to update extremities.
	prev_event_id TEXT NOT NULL,
	PRIMARY KEY(room_id, event_id, prev_event_id)
);
`

const insertBackwardExtremitySQL = "" +
	"INSERT INTO syncapi_backward_extremities (room_id, event_id, prev_event_id)" +
	" VALUES ($1, $2, $3)" +
	" ON CONFLICT DO NOTHING"

const selectBackwardExtremitiesForRoomSQL = "" +
	"SELECT event_id, prev_event_id FROM syncapi_backward_extremities WHERE room_id = $1"

const deleteBackwardExtremitySQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE room_id = $1 AND prev_event_id = $2"

const purgeBackwardExtremitiesSQL = "" +
	"DELETE FROM syncapi_backward_extremities WHERE room_id = $1"

type backwardExtremitiesStatements struct {
	insertBackwardExtremityStmt          *sql.Stmt
	selectBackwardExtremitiesForRoomStmt *sql.Stmt
	deleteBackwardExtremityStmt          *sql.Stmt
	purgeBackwardExtremitiesStmt         *sql.Stmt
}

func NewPostgresBackwardsExtremitiesTable(db *sql.DB) (tables.BackwardsExtremities, error) {
	s := &backwardExtremitiesStatements{}
	_, err := db.Exec(backwardExtremitiesSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertBackwardExtremityStmt, insertBackwardExtremitySQL},
		{&s.selectBackwardExtremitiesForRoomStmt, selectBackwardExtremitiesForRoomSQL},
		{&s.deleteBackwardExtremityStmt, deleteBackwardExtremitySQL},
		{&s.purgeBackwardExtremitiesStmt, purgeBackwardExtremitiesSQL},
	}.Prepare(db)
}

func (s *backwardExtremitiesStatements) InsertsBackwardExtremity(
	ctx context.Context, txn *sql.Tx, roomID, eventID string, prevEventID string,
) (err error) {
	stmt := sqlutil.TxStmt(txn, s.insertBackwardExtremityStmt)
	_, err = stmt.ExecContext(ctx, roomID, eventID, prevEventID)
	return
}

func (s *backwardExtremitiesStatements) SelectBackwardExtremitiesForRoom(
	ctx context.Context, txn *sql.Tx, roomID string,
) (bwExtrems map[string][]string, err error) {
	selectStmt := sqlutil.TxStmt(txn, s.selectBackwardExtremitiesForRoomStmt)
	rows, err := selectStmt.QueryContext(ctx, roomID)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectBackwardExtremitiesForRoom: rows.close() failed")

	bwExtrems = make(map[string][]string)
	for rows.Next() {
		var eID string
		var prevEventID string
		if err = rows.Scan(&eID, &prevEventID); err != nil {
			return
		}
		bwExtrems[eID] = append(bwExtrems[eID], prevEventID)
	}

	return bwExtrems, rows.Err()
}

func (s *backwardExtremitiesStatements) DeleteBackwardExtremity(
	ctx context.Context, txn *sql.Tx, roomID, knownEventID string,
) (err error) {
	deleteStmt := sqlutil.TxStmt(txn, s.deleteBackwardExtremityStmt)
	_, err = deleteStmt.ExecContext(ctx, roomID, knownEventID)
	return
}

func (s *backwardExtremitiesStatements) PurgeBackwardExtremities(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	purgeStmt := sqlutil.TxStmt(txn, s.purgeBackwardExtremitiesStmt)
	_, err := purgeStmt.ExecContext(ctx, roomID)
	return err
}
