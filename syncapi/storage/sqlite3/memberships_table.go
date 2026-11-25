// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// The memberships table is designed to track the last time that
// the user was a given state. This allows us to find out the
// most recent time that a user was invited to, joined or left
// a room, either by choice or otherwise. This is important for
// building history visibility.

const membershipsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_memberships (
    -- The 'room_id' key for the state event.
    room_id TEXT NOT NULL,
    -- The state event ID
	user_id TEXT NOT NULL,
	-- The status of the membership
	membership TEXT NOT NULL,
	-- The event ID that last changed the membership
	event_id TEXT NOT NULL,
	-- The stream position of the change
	stream_pos BIGINT NOT NULL,
	-- The topological position of the change in the room
	topological_pos BIGINT NOT NULL,
	-- Unique index
	UNIQUE (room_id, user_id, membership)
);

CREATE TABLE IF NOT EXISTS syncapi_membership_history (
    room_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    membership TEXT NOT NULL,
    event_id TEXT NOT NULL,
    stream_pos BIGINT NOT NULL,
    PRIMARY KEY (room_id, user_id, stream_pos)
);
-- The primary key serves per-user "stream_pos DESC LIMIT 1" lookups. The
-- catch-up below needs the global maximum and the rows past it, which only a
-- stream_pos index answers without scanning either table on every startup.
DROP INDEX IF EXISTS syncapi_membership_history_pos_idx;
CREATE INDEX IF NOT EXISTS syncapi_membership_history_stream_pos_idx ON syncapi_membership_history (stream_pos);
CREATE INDEX IF NOT EXISTS syncapi_memberships_stream_pos_idx ON syncapi_memberships (stream_pos);
-- Backfill from the current-state table so lookups before the upgrade don't
-- fall back to the lossy current table. Catch up rows beyond the history floor
-- on later startups as well. That recovers only the latest transition per
-- membership type written after a downgrade to a build that predates
-- membership history, because syncapi_memberships keeps one row per
-- (room, user, membership).
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT room_id, user_id, membership, event_id, stream_pos FROM syncapi_memberships
WHERE stream_pos > COALESCE((SELECT MAX(stream_pos) FROM syncapi_membership_history), 0)
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING;
`

const insertMembershipHistorySQL = `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (room_id, user_id, stream_pos) DO UPDATE
SET membership = excluded.membership, event_id = excluded.event_id
`

const selectMembershipHistoryAtStreamSQL = `
SELECT membership, event_id, stream_pos FROM syncapi_membership_history
WHERE room_id = $1 AND user_id = $2 AND stream_pos <= $3
ORDER BY stream_pos DESC LIMIT 1
`

const purgeMembershipHistorySQL = "DELETE FROM syncapi_membership_history WHERE room_id = $1"

const upsertMembershipSQL = "" +
	"INSERT INTO syncapi_memberships (room_id, user_id, membership, event_id, stream_pos, topological_pos)" +
	" VALUES ($1, $2, $3, $4, $5, $6)" +
	" ON CONFLICT (room_id, user_id, membership)" +
	" DO UPDATE SET event_id = $4, stream_pos = $5, topological_pos = $6" +
	" WHERE syncapi_memberships.event_id <> excluded.event_id" +
	" OR (excluded.topological_pos = 0 AND (syncapi_memberships.stream_pos <> excluded.stream_pos OR syncapi_memberships.topological_pos <> excluded.topological_pos))"

// A user has one row per membership kind, so two rows can share a stream
// position (a state resync stamps every row it writes with the same one). The
// row whose event is the room's current member event then wins the tie, as an
// arbitrary pick could leave a joined user counted and listed as invited.
const isCurrentMemberEventSQL = "EXISTS (SELECT 1 FROM syncapi_current_room_state c" +
	" WHERE c.room_id = m.room_id AND c.type = 'm.room.member' AND c.state_key = m.user_id AND c.event_id = m.event_id)"

const selectMembershipCountSQL = "" +
	"SELECT COUNT(*) FROM (" +
	" SELECT m.membership, ROW_NUMBER() OVER (PARTITION BY m.user_id ORDER BY m.stream_pos DESC, " + isCurrentMemberEventSQL + " DESC) AS rn" +
	" FROM syncapi_memberships m WHERE m.room_id = $1 AND m.stream_pos <= $2" +
	") t WHERE t.rn = 1 AND t.membership = $3"

const selectMembershipBeforeSQL = "" +
	"SELECT membership, topological_pos FROM syncapi_memberships WHERE room_id = $1 and user_id = $2 AND topological_pos <= $3 ORDER BY topological_pos DESC LIMIT 1"

// selectMembershipCurrentSQL answers the same question for the MaxInt64
// "current" sentinel. Ordering by topological position is wrong then: a
// resync stamps the current member event with topological position 0, so a
// stale invite with a real topology would win. Stream order plus the
// current-event tie-break gives the membership the room state actually has.
const selectMembershipCurrentSQL = "" +
	"SELECT m.membership, m.topological_pos FROM syncapi_memberships m" +
	" WHERE m.room_id = $1 AND m.user_id = $2" +
	" ORDER BY m.stream_pos DESC, " + isCurrentMemberEventSQL + " DESC LIMIT 1"

const selectMembershipAtStreamSQL = "" +
	"SELECT m.membership, m.event_id, m.stream_pos, " + isCurrentMemberEventSQL + " FROM syncapi_memberships m" +
	" WHERE m.room_id = $1 and m.user_id = $2 AND m.stream_pos <= $3" +
	" ORDER BY m.stream_pos DESC, " + isCurrentMemberEventSQL + " DESC LIMIT 1"

// Rows written by a partial-state resync carry topological position 0: they
// are the room's current membership stamped with the resync's stream position,
// not events at the room's start. Bound them by that stream position for both
// stream and topology tokens, so a member added after the requested token is
// not reported as having been in the room then. Real memberships still compare
// by topology position.
const selectMembersSQL = `
SELECT event_id FROM
 ( SELECT m.event_id, m.membership,
          ROW_NUMBER() OVER (PARTITION BY m.user_id ORDER BY m.stream_pos DESC, ` + isCurrentMemberEventSQL + ` DESC) AS rn
   FROM syncapi_memberships m WHERE m.room_id = $1
   AND (($2 = 9223372036854775807 AND
            m.stream_pos <= $3)
        OR ($2 <> 9223372036854775807 AND
            ((m.topological_pos = 0 AND m.stream_pos <= $3)
            OR (m.topological_pos > 0 AND
                (m.topological_pos < $2 OR (m.topological_pos = $2 AND m.stream_pos <= $3))))))) t
    WHERE t.rn = 1
    AND ($4 IS NULL OR t.membership = $4)
    AND ($5 IS NULL OR t.membership <> $5)
`

const updateMembershipTopologicalPosSQL = "" +
	"UPDATE syncapi_memberships SET topological_pos = $1 WHERE room_id = $2 AND event_id = $3 AND topological_pos < $1"

const purgeMembershipsSQL = "" +
	"DELETE FROM syncapi_memberships WHERE room_id = $1"

type membershipsStatements struct {
	db                        *sql.DB
	upsertMembershipStmt      *sql.Stmt
	selectMembershipCountStmt *sql.Stmt
	// selectHeroesStmt          *sql.Stmt - prepared at runtime due to variadic
	selectMembershipForUserStmt         *sql.Stmt
	selectMembershipCurrentStmt         *sql.Stmt
	selectMembershipAtStreamStmt        *sql.Stmt
	selectMembersStmt                   *sql.Stmt
	updateMembershipTopologicalPosStmt  *sql.Stmt
	purgeMembershipsStmt                *sql.Stmt
	insertMembershipHistoryStmt         *sql.Stmt
	selectMembershipHistoryAtStreamStmt *sql.Stmt
	purgeMembershipHistoryStmt          *sql.Stmt
}

// NewSqliteMembershipsTable creates the membership table if needed and prepares
// its queries, returning schema or statement preparation errors.
func NewSqliteMembershipsTable(db *sql.DB) (tables.Memberships, error) {
	s := &membershipsStatements{
		db: db,
	}
	_, err := db.Exec(membershipsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.upsertMembershipStmt, upsertMembershipSQL},
		{&s.selectMembershipCountStmt, selectMembershipCountSQL},
		{&s.selectMembershipForUserStmt, selectMembershipBeforeSQL},
		{&s.selectMembershipCurrentStmt, selectMembershipCurrentSQL},
		{&s.selectMembershipAtStreamStmt, selectMembershipAtStreamSQL},
		{&s.selectMembersStmt, selectMembersSQL},
		{&s.updateMembershipTopologicalPosStmt, updateMembershipTopologicalPosSQL},
		{&s.purgeMembershipsStmt, purgeMembershipsSQL},
		{&s.insertMembershipHistoryStmt, insertMembershipHistorySQL},
		{&s.selectMembershipHistoryAtStreamStmt, selectMembershipHistoryAtStreamSQL},
		{&s.purgeMembershipHistoryStmt, purgeMembershipHistorySQL},
	}.Prepare(db)
}

func (s *membershipsStatements) UpsertMembership(
	ctx context.Context, txn *sql.Tx, event *rstypes.HeaderedEvent,
	streamPos, topologicalPos types.StreamPosition,
) error {
	membership, err := event.Membership()
	if err != nil {
		return fmt.Errorf("event.Membership: %w", err)
	}
	upsertMembershipStmt := sqlutil.TxStmt(txn, s.upsertMembershipStmt)
	_, err = upsertMembershipStmt.ExecContext(
		ctx,
		event.RoomID().String(),
		event.StateKeyResolved,
		membership,
		event.EventID(),
		streamPos,
		topologicalPos,
	)
	if err != nil {
		return err
	}
	_, err = sqlutil.TxStmt(txn, s.insertMembershipHistoryStmt).ExecContext(ctx,
		event.RoomID().String(), event.StateKeyResolved, membership, event.EventID(), streamPos)
	return err
}

// UpsertMemberships loops over UpsertMembership: SQLite runs in-process, so a
// statement per row costs no network round trip.
func (s *membershipsStatements) UpsertMemberships(
	ctx context.Context, txn *sql.Tx, events []*rstypes.HeaderedEvent,
	streamPos, topologicalPos types.StreamPosition,
) error {
	for _, event := range events {
		if err := s.UpsertMembership(ctx, txn, event, streamPos, topologicalPos); err != nil {
			return err
		}
	}
	return nil
}

func (s *membershipsStatements) SelectMembershipCount(
	ctx context.Context, txn *sql.Tx, roomID, membership string, pos types.StreamPosition,
) (count int, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipCountStmt)
	err = stmt.QueryRowContext(ctx, roomID, pos, membership).Scan(&count)
	return
}

// SelectMembershipForUser returns the membership of the user before and including the given position. If no membership can be found
// returns "leave", the topological position and no error. If an error occurs, other than sql.ErrNoRows, returns that and an empty
// string as the membership.
func (s *membershipsStatements) SelectMembershipForUser(
	ctx context.Context, txn *sql.Tx, roomID, userID string, pos int64,
) (membership string, topologyPos int64, err error) {
	if pos == math.MaxInt64 {
		stmt := sqlutil.TxStmt(txn, s.selectMembershipCurrentStmt)
		err = stmt.QueryRowContext(ctx, roomID, userID).Scan(&membership, &topologyPos)
	} else {
		stmt := sqlutil.TxStmt(txn, s.selectMembershipForUserStmt)
		err = stmt.QueryRowContext(ctx, roomID, userID, pos).Scan(&membership, &topologyPos)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return "leave", 0, nil
		}
		return "", 0, err
	}
	return membership, topologyPos, nil
}

// SelectMembershipForUserAtStreamPosition returns the latest membership
// at or before streamPos, using sync stream order rather than room topology.
// Membership history retains every transition. No matching
// row returns "leave" without an error; other query or scan errors propagate.
func (s *membershipsStatements) SelectMembershipForUserAtStreamPosition(
	ctx context.Context, txn *sql.Tx, roomID, userID string, streamPos int64,
) (membership, eventID string, err error) {
	var historyMembership, historyEventID string
	var historyPos int64
	historyStmt := sqlutil.TxStmt(txn, s.selectMembershipHistoryAtStreamStmt)
	historyErr := historyStmt.QueryRowContext(ctx, roomID, userID, streamPos).Scan(&historyMembership, &historyEventID, &historyPos)
	if historyErr != nil && historyErr != sql.ErrNoRows {
		return "", "", historyErr
	}
	var currentMembership, currentEventID string
	var currentPos int64
	var currentIsCurrentEvent bool
	currentStmt := sqlutil.TxStmt(txn, s.selectMembershipAtStreamStmt)
	currentErr := currentStmt.QueryRowContext(ctx, roomID, userID, streamPos).Scan(&currentMembership, &currentEventID, &currentPos, &currentIsCurrentEvent)
	if currentErr != nil && currentErr != sql.ErrNoRows {
		return "", "", currentErr
	}
	// The compact row wins when it is newer. On an exact tie it overrides the
	// history only if its event is the room's current member event: history is
	// last-writer-wins per position, so a resync can leave it holding a stale
	// row, but a compact row whose event is not current may merely be the one
	// that a later update of the same membership left behind, while history
	// still holds the transition that happened at that position.
	compactWins := currentErr == nil &&
		(historyErr == sql.ErrNoRows || currentPos > historyPos || (currentPos == historyPos && currentIsCurrentEvent))
	if historyErr == nil && !compactWins {
		return historyMembership, historyEventID, nil
	}
	if currentErr == nil {
		return currentMembership, currentEventID, nil
	}
	return "leave", "", nil
}

func (s *membershipsStatements) UpdateMembershipTopologicalPos(
	ctx context.Context, txn *sql.Tx, roomID, eventID string, topologicalPos types.StreamPosition,
) error {
	_, err := sqlutil.TxStmt(txn, s.updateMembershipTopologicalPosStmt).ExecContext(ctx, topologicalPos, roomID, eventID)
	return err
}

func (s *membershipsStatements) PurgeMemberships(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	if _, err := sqlutil.TxStmt(txn, s.purgeMembershipHistoryStmt).ExecContext(ctx, roomID); err != nil {
		return err
	}
	purgeMembershipsStmt := sqlutil.TxStmt(txn, s.purgeMembershipsStmt)
	_, err := purgeMembershipsStmt.ExecContext(ctx, roomID)
	return err
}

func (s *membershipsStatements) SelectMemberships(
	ctx context.Context, txn *sql.Tx,
	roomID string, pos types.TopologyToken,
	membership, notMembership *string,
) (eventIDs []string, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembersStmt)
	rows, err := stmt.QueryContext(ctx, roomID, pos.Depth, pos.PDUPosition, membership, notMembership)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectMemberships: failed to close rows")
	var eventID string
	for rows.Next() {
		if err = rows.Scan(&eventID); err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs, rows.Err()
}
