// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const inviteEventsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_invite_events (
	id BIGINT PRIMARY KEY DEFAULT nextval('syncapi_stream_id'),
	event_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	target_user_id TEXT NOT NULL,
	headered_event_json TEXT NOT NULL,
	deleted BOOL NOT NULL
);

-- For looking up the invites for a given user.
CREATE INDEX IF NOT EXISTS syncapi_invites_target_user_id_idx
	ON syncapi_invite_events (target_user_id, id);

-- For deleting old invites
CREATE INDEX IF NOT EXISTS syncapi_invites_event_id_idx
	ON syncapi_invite_events (event_id);
`

const insertInviteEventSQL = "" +
	"INSERT INTO syncapi_invite_events (" +
	" room_id, event_id, target_user_id, headered_event_json, deleted" +
	") VALUES ($1, $2, $3, $4, FALSE)" +
	" ON CONFLICT (event_id) DO NOTHING RETURNING id"

const selectInviteEventRetirementSQL = "" +
	"SELECT id, deleted FROM syncapi_invite_events WHERE event_id = $1 ORDER BY id DESC LIMIT 1"

const deleteInviteEventSQL = "" +
	"UPDATE syncapi_invite_events SET deleted=TRUE, id=nextval('syncapi_stream_id') WHERE event_id = $1 AND deleted=FALSE RETURNING id"

const insertRetiredInviteEventSQL = "" +
	"INSERT INTO syncapi_invite_events (event_id, room_id, target_user_id, headered_event_json, deleted)" +
	" VALUES ($1, $2, '', '{}', TRUE) ON CONFLICT (event_id) DO NOTHING RETURNING id"

const selectInviteEventsInRangeSQL = "" +
	"SELECT id, room_id, headered_event_json, deleted FROM syncapi_invite_events" +
	" WHERE target_user_id = $1 AND id > $2 AND id <= $3" +
	" ORDER BY id DESC"

const selectMaxInviteIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_invite_events"

const selectRoomsWithInvitesSinceSQL = "" +
	"SELECT DISTINCT room_id FROM syncapi_invite_events" +
	" WHERE target_user_id = $1 AND room_id = ANY($2) AND id > $3"

const purgeInvitesSQL = "" +
	"DELETE FROM syncapi_invite_events WHERE room_id = $1"

type inviteEventsStatements struct {
	insertInviteEventStmt           *sql.Stmt
	insertRetiredInviteEventStmt    *sql.Stmt
	selectInviteEventRetirementStmt *sql.Stmt
	selectInviteEventsInRangeStmt   *sql.Stmt
	deleteInviteEventStmt           *sql.Stmt
	selectMaxInviteIDStmt           *sql.Stmt
	selectRoomsWithInvitesSinceStmt *sql.Stmt
	purgeInvitesStmt                *sql.Stmt
}

// NewPostgresInvitesTable creates the invites table if needed, applies the
// migration that deduplicates event IDs, and prepares its statements. Schema,
// migration, and statement preparation errors are returned.
func NewPostgresInvitesTable(ctx context.Context, db *sql.DB) (tables.Invites, error) {
	s := &inviteEventsStatements{}
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
		{&s.insertRetiredInviteEventStmt, insertRetiredInviteEventSQL},
		{&s.selectInviteEventRetirementStmt, selectInviteEventRetirementSQL},
		{&s.selectInviteEventsInRangeStmt, selectInviteEventsInRangeSQL},
		{&s.deleteInviteEventStmt, deleteInviteEventSQL},
		{&s.selectMaxInviteIDStmt, selectMaxInviteIDSQL},
		{&s.selectRoomsWithInvitesSinceStmt, selectRoomsWithInvitesSinceSQL},
		{&s.purgeInvitesStmt, purgeInvitesSQL},
	}.Prepare(db)
}

// InsertInviteEvent stores an invite for inviteEvent.UserID and returns its
// stream position. An existing event ID reuses its stored position without
// updating the row, including when it is retired. A nil txn uses the database
// directly. Serialization and database errors are returned.
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

	var headeredJSON []byte
	headeredJSON, err = json.Marshal(inviteEvent)
	if err != nil {
		return 0, false, err
	}

	err = sqlutil.TxStmt(txn, s.insertInviteEventStmt).QueryRowContext(
		ctx,
		inviteEvent.RoomID().String(),
		inviteEvent.EventID(),
		inviteEvent.UserID.String(),
		headeredJSON,
	).Scan(&streamPos)
	if errors.Is(err, sql.ErrNoRows) {
		// Another writer inserted the same event first; reuse its row.
		err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).QueryRowContext(ctx, inviteEvent.EventID()).Scan(&streamPos, &deleted)
		return streamPos, !deleted, err
	}
	return streamPos, err == nil, err
}

// DeleteInviteEvent marks an invite as retired and returns its retirement
// stream position. An already retired invite reuses its stored position;
// an unknown event ID creates a tombstone so a later invite delivery cannot
// resurrect it. A nil txn uses the database directly. Other database errors
// are propagated.
func (s *inviteEventsStatements) DeleteInviteEvent(
	ctx context.Context, txn *sql.Tx, inviteEventID, roomID string,
) (sp types.StreamPosition, err error) {
	var deleted bool
	err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).
		QueryRowContext(ctx, inviteEventID).Scan(&sp, &deleted)
	if errors.Is(err, sql.ErrNoRows) {
		// The retirement can arrive before the invite. Persist a deleted row
		// under the same unique event ID so a late invite insert observes it.
		err = sqlutil.TxStmt(txn, s.insertRetiredInviteEventStmt).
			QueryRowContext(ctx, inviteEventID, roomID).Scan(&sp)
		if err == nil {
			return sp, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		// A concurrent invite or retirement won the unique-key race. Read its
		// row and continue below if the invite is still active.
		err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).
			QueryRowContext(ctx, inviteEventID).Scan(&sp, &deleted)
	}
	if err != nil {
		return 0, err
	}
	if deleted {
		return sp, nil
	}

	err = sqlutil.TxStmt(txn, s.deleteInviteEventStmt).QueryRowContext(ctx, inviteEventID).Scan(&sp)
	if errors.Is(err, sql.ErrNoRows) {
		// Another writer retired it first; return the position it persisted.
		err = sqlutil.TxStmt(txn, s.selectInviteEventRetirementStmt).
			QueryRowContext(ctx, inviteEventID).Scan(&sp, &deleted)
	}
	return sp, err
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

// SelectRoomsWithInvitesSince returns a list of room IDs that have invite events with stream position > since
// This is used for incremental sync to filter rooms that haven't had invite changes.
func (s *inviteEventsStatements) SelectRoomsWithInvitesSince(
	ctx context.Context, txn *sql.Tx,
	targetUserID string, roomIDs []string, since types.StreamPosition,
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomsWithInvitesSinceStmt)
	rows, err := stmt.QueryContext(ctx, targetUserID, roomIDs, since)
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
		result = append(result, roomID)
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
