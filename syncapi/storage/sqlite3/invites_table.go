// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const inviteEventsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_invite_events (
	id INTEGER PRIMARY KEY,
	event_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	target_user_id TEXT NOT NULL,
	headered_event_json TEXT NOT NULL,
	deleted BOOL NOT NULL
);

CREATE INDEX IF NOT EXISTS syncapi_invites_target_user_id_idx ON syncapi_invite_events (target_user_id, id);
CREATE INDEX IF NOT EXISTS syncapi_invites_event_id_idx ON syncapi_invite_events (event_id);
`

const insertInviteEventSQL = "" +
	"INSERT INTO syncapi_invite_events" +
	" (id, room_id, event_id, target_user_id, headered_event_json, deleted)" +
	" VALUES ($1, $2, $3, $4, $5, false)" +
	" ON CONFLICT(event_id) DO NOTHING"

const selectInviteEventRetirementSQL = "" +
	"SELECT id, deleted FROM syncapi_invite_events WHERE event_id = $1 ORDER BY id DESC LIMIT 1"

const deleteInviteEventSQL = "" +
	"UPDATE syncapi_invite_events SET deleted=true, id=$1 WHERE event_id = $2 AND deleted=false"

const nextInviteIDSQL = `UPDATE syncapi_stream_id
	SET stream_id = MAX(stream_id, COALESCE((SELECT MAX(id) FROM syncapi_invite_events), 0)) + 1
	WHERE stream_name = 'invite' RETURNING stream_id`

const selectInviteEventsInRangeSQL = "" +
	"SELECT id, room_id, headered_event_json, deleted FROM syncapi_invite_events" +
	" WHERE target_user_id = $1 AND id > $2 AND id <= $3" +
	" ORDER BY id DESC"

const selectMaxInviteIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_invite_events"

const purgeInvitesSQL = "" +
	"DELETE FROM syncapi_invite_events WHERE room_id = $1"

type inviteEventsStatements struct {
	db                              *sql.DB
	insertInviteEventStmt           *sql.Stmt
	selectInviteEventRetirementStmt *sql.Stmt
	selectInviteEventsInRangeStmt   *sql.Stmt
	deleteInviteEventStmt           *sql.Stmt
	nextInviteIDStmt                *sql.Stmt
	selectMaxInviteIDStmt           *sql.Stmt
	purgeInvitesStmt                *sql.Stmt
}

func NewSqliteInvitesTable(ctx context.Context, db *sql.DB, _ *StreamIDStatements) (tables.Invites, error) {
	s := &inviteEventsStatements{
		db: db,
	}
	_, err := db.Exec(inviteEventsSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "syncapi: unique invite event IDs",
		Up:      deltas.UpUniqueInviteEventIDs,
	})
	if err = m.Up(ctx); err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertInviteEventStmt, insertInviteEventSQL},
		{&s.selectInviteEventRetirementStmt, selectInviteEventRetirementSQL},
		{&s.selectInviteEventsInRangeStmt, selectInviteEventsInRangeSQL},
		{&s.deleteInviteEventStmt, deleteInviteEventSQL},
		{&s.nextInviteIDStmt, nextInviteIDSQL},
		{&s.selectMaxInviteIDStmt, selectMaxInviteIDSQL},
		{&s.purgeInvitesStmt, purgeInvitesSQL},
	}.Prepare(db)
}

func (s *inviteEventsStatements) InsertInviteEvent(
	ctx context.Context, txn *sql.Tx, inviteEvent *rstypes.HeaderedEvent,
) (streamPos types.StreamPosition, active bool, err error) {
	// A redelivered invite must not add a second row or reactivate a retired one.
	var deleted bool
	err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).QueryRowContext(ctx, inviteEvent.EventID()).Scan(&streamPos, &deleted)
	if err == nil {
		return streamPos, !deleted, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}

	streamPos, err = nextInviteID(ctx, txn, s.nextInviteIDStmt)
	if err != nil {
		return 0, false, err
	}

	var headeredJSON []byte
	headeredJSON, err = json.Marshal(inviteEvent)
	if err != nil {
		return 0, false, err
	}

	stmt := sqlutil.TxStmt(txn, s.insertInviteEventStmt)
	result, err := stmt.ExecContext(
		ctx,
		streamPos,
		inviteEvent.RoomID().String(),
		inviteEvent.EventID(),
		inviteEvent.UserID.String(),
		headeredJSON,
	)
	if err != nil {
		return 0, false, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if affected == 0 {
		// Another writer inserted the same event first; reuse its row.
		err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).QueryRowContext(ctx, inviteEvent.EventID()).Scan(&streamPos, &deleted)
		return streamPos, !deleted, err
	}
	return streamPos, err == nil, err
}

func (s *inviteEventsStatements) DeleteInviteEvent(
	ctx context.Context, txn *sql.Tx, inviteEventID string,
) (types.StreamPosition, error) {
	var streamPos types.StreamPosition
	var deleted bool
	err := sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).
		QueryRowContext(ctx, inviteEventID).Scan(&streamPos, &deleted)
	if err != nil {
		return 0, err
	}
	if deleted {
		return streamPos, nil
	}

	newPos, err := nextInviteID(ctx, txn, s.nextInviteIDStmt)
	if err != nil {
		return 0, err
	}
	result, err := sqlutil.TxStmt(txn, s.deleteInviteEventStmt).ExecContext(ctx, newPos, inviteEventID)
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		// Another writer retired it first; return the position it persisted.
		err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).
			QueryRowContext(ctx, inviteEventID).Scan(&streamPos, &deleted)
		return streamPos, err
	}
	return newPos, nil
}

func nextInviteID(ctx context.Context, txn *sql.Tx, stmt *sql.Stmt) (types.StreamPosition, error) {
	var streamPos types.StreamPosition
	err := sqlutil.TxStmt(txn, stmt).QueryRowContext(ctx).Scan(&streamPos)
	return streamPos, err
}

// selectInviteEventsInRange returns a map of room ID to invite event for the
// active invites for the target user ID in the supplied range.
func (s *inviteEventsStatements) SelectInviteEventsInRange(
	ctx context.Context, txn *sql.Tx, targetUserID string, r types.Range,
) (map[string]*rstypes.HeaderedEvent, map[string]*rstypes.HeaderedEvent, types.StreamPosition, error) {
	var lastPos types.StreamPosition
	stmt := sqlutil.TxStmt(txn, s.selectInviteEventsInRangeStmt)
	rows, err := stmt.QueryContext(ctx, targetUserID, r.Low(), r.High())
	if err != nil {
		return nil, nil, lastPos, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectInviteEventsInRange: rows.close() failed")
	result := map[string]*rstypes.HeaderedEvent{}
	retired := map[string]*rstypes.HeaderedEvent{}
	for rows.Next() {
		var (
			id        types.StreamPosition
			roomID    string
			eventJSON []byte
			deleted   bool
		)
		if err = rows.Scan(&id, &roomID, &eventJSON, &deleted); err != nil {
			return nil, nil, lastPos, err
		}
		if id > lastPos {
			lastPos = id
		}

		// if we have seen this room before, it has a higher stream position and hence takes priority
		// because the query is ORDER BY id DESC so drop them
		_, isRetired := retired[roomID]
		_, isInvited := result[roomID]
		if isRetired || isInvited {
			continue
		}

		var event *rstypes.HeaderedEvent
		if err := json.Unmarshal(eventJSON, &event); err != nil {
			return nil, nil, lastPos, err
		}

		if deleted {
			retired[roomID] = event
		} else {
			result[roomID] = event
		}
	}
	if lastPos == 0 {
		lastPos = r.To
	}
	return result, retired, lastPos, rows.Err()
}

func (s *inviteEventsStatements) SelectMaxInviteID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	var nullableID sql.NullInt64
	stmt := sqlutil.TxStmt(txn, s.selectMaxInviteIDStmt)
	err = stmt.QueryRowContext(ctx).Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

// SelectRoomsWithInvitesSince returns a list of room IDs that have invite events with stream position > since.
func (s *inviteEventsStatements) SelectRoomsWithInvitesSince(
	ctx context.Context, txn *sql.Tx,
	targetUserID string, roomIDs []string, since types.StreamPosition,
) ([]string, error) {
	// Build a set of candidate room IDs for fast lookup
	candidateRooms := make(map[string]bool, len(roomIDs))
	for _, roomID := range roomIDs {
		candidateRooms[roomID] = true
	}

	// Query for all rooms with invites for this user since the position
	// SQLite doesn't support ANY, so we query all and filter in Go
	query := `SELECT DISTINCT room_id FROM syncapi_invite_events
		WHERE target_user_id = ?  AND id > ?`

	var (
		rows *sql.Rows
		err  error
	)
	if txn == nil {
		rows, err = s.db.QueryContext(ctx, query, targetUserID, since)
	} else {
		rows, err = txn.QueryContext(ctx, query, targetUserID, since)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomsWithInvitesSince: rows.close() failed")

	var result []string
	for rows.Next() {
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			return nil, err
		}
		// Only include if in candidate list
		if candidateRooms[roomID] {
			result = append(result, roomID)
		}
	}
	return result, rows.Err()
}

func (s *inviteEventsStatements) PurgeInvites(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	purgeInvitesStmt := sqlutil.TxStmt(txn, s.purgeInvitesStmt)
	_, err := purgeInvitesStmt.ExecContext(ctx, roomID)
	return err
}
