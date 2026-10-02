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
	"fmt"
	"slices"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const currentRoomStateSchema = `
-- Stores the current room state for every room.
CREATE TABLE IF NOT EXISTS syncapi_current_room_state (
    -- The 'room_id' key for the state event.
    room_id TEXT NOT NULL,
    -- The state event ID
    event_id TEXT NOT NULL,
    -- The state event type e.g 'm.room.member'
    type TEXT NOT NULL,
    -- The 'sender' property of the event.
    sender TEXT NOT NULL,
	-- true if the event content contains a url key
    contains_url BOOL NOT NULL,
    -- The state_key value for this state event e.g ''
    state_key TEXT NOT NULL,
    -- The JSON for the event. Stored as TEXT because this should be valid UTF-8.
    headered_event_json TEXT NOT NULL,
    -- The 'content.membership' value if this event is an m.room.member event. For other
    -- events, this will be NULL.
    membership TEXT,
    -- The serial ID of the output_room_events table when this event became
    -- part of the current state of the room.
    added_at BIGINT,
    history_visibility SMALLINT NOT NULL DEFAULT 2,
    -- Clobber based on 3-uple of room_id, type and state_key
    CONSTRAINT syncapi_room_state_unique UNIQUE (room_id, type, state_key)
);
-- for event deletion
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_id_idx ON syncapi_current_room_state(event_id, room_id, type, sender, contains_url);
-- for querying membership states of users
CREATE INDEX IF NOT EXISTS syncapi_membership_idx ON syncapi_current_room_state(type, state_key, membership) WHERE membership IS NOT NULL AND membership != 'leave';
-- for querying state by event IDs
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_current_room_state_eventid_idx ON syncapi_current_room_state(event_id);
-- for improving selectRoomIDsWithAnyMembershipSQL
CREATE INDEX IF NOT EXISTS syncapi_current_room_state_type_state_key_idx ON syncapi_current_room_state(type, state_key);
`

const upsertRoomStateSQL = "" +
	"INSERT INTO syncapi_current_room_state (room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, added_at, history_visibility)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)" +
	" ON CONFLICT ON CONSTRAINT syncapi_room_state_unique" +
	" DO UPDATE SET event_id = $2, sender=$4, contains_url=$5, headered_event_json = $7, membership = $8, added_at = $9"

// upsertChunkSize bounds the rows sent in one batched statement. Large enough
// that a 50,000-entry state rewrite costs tens of round trips, small enough
// that one statement stays a few megabytes.
const upsertChunkSize = 1000

// upsertRoomStatesSQL is upsertRoomStateSQL for many rows in one round trip.
// The rows must not share a (room_id, type, state_key), because ON CONFLICT
// cannot update the same row twice within one statement.
const upsertRoomStatesSQL = "" +
	"INSERT INTO syncapi_current_room_state (room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, added_at, history_visibility)" +
	" SELECT room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, $9, history_visibility" +
	" FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bool[], $6::text[], $7::text[], $8::text[], $10::smallint[])" +
	" AS rows(room_id, event_id, type, sender, contains_url, state_key, headered_event_json, membership, history_visibility)" +
	" ON CONFLICT ON CONSTRAINT syncapi_room_state_unique" +
	" DO UPDATE SET event_id = EXCLUDED.event_id, sender = EXCLUDED.sender, contains_url = EXCLUDED.contains_url," +
	" headered_event_json = EXCLUDED.headered_event_json, membership = EXCLUDED.membership, added_at = EXCLUDED.added_at"

const deleteRoomStateByEventIDSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE event_id = $1"

const deleteRoomStateByEventIDsSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE event_id = ANY($1)"

const deleteRoomStateForRoomSQL = "" +
	"DELETE FROM syncapi_current_room_state WHERE room_id = $1"

const selectRoomIDsWithMembershipSQL = "" +
	"SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1 AND membership = $2"

// selectKickedRoomIDsSQL returns rooms where the user was kicked (leave membership where sender != user).
// Per MSC4186/Synapse behavior, kicked rooms should be included in the sliding sync room list.
const selectKickedRoomIDsSQL = "" +
	"SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1 AND membership = 'leave' AND sender != $1"

const selectRoomIDsWithAnyMembershipSQL = "" +
	"SELECT room_id, membership FROM syncapi_current_room_state WHERE type = 'm.room.member' AND state_key = $1"

const selectCurrentStateSQL = "" +
	"SELECT event_id, headered_event_json FROM syncapi_current_room_state WHERE room_id = $1" +
	" AND ( $2::text[] IS NULL OR     sender  = ANY($2)  )" +
	" AND ( $3::text[] IS NULL OR NOT(sender  = ANY($3)) )" +
	" AND ( $4::text[] IS NULL OR     type LIKE ANY($4)  )" +
	" AND ( $5::text[] IS NULL OR NOT(type LIKE ANY($5)) )" +
	" AND ( $6::bool IS NULL   OR     contains_url = $6  )" +
	" AND (event_id = ANY($7)) IS NOT TRUE"

const selectJoinedUsersSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join'"

const selectJoinedUsersInRoomSQL = "" +
	"SELECT room_id, state_key FROM syncapi_current_room_state WHERE type = 'm.room.member' AND membership = 'join' AND room_id = ANY($1)"

const selectStateEventSQL = "" +
	"SELECT headered_event_json FROM syncapi_current_room_state WHERE room_id = $1 AND type = $2 AND state_key = $3"

const selectStateEventsSQL = "" +
	"SELECT type, state_key, headered_event_json FROM syncapi_current_room_state WHERE room_id = $1" +
	" AND (type, state_key) IN (SELECT * FROM unnest($2::text[], $3::text[]))"

const selectEventsWithEventIDsSQL = "" +
	"SELECT event_id, added_at, headered_event_json, history_visibility FROM syncapi_current_room_state WHERE event_id = ANY($1)"

const selectSharedUsersSQL = "" +
	"SELECT state_key FROM syncapi_current_room_state WHERE room_id = ANY(" +
	"	SELECT DISTINCT room_id FROM syncapi_current_room_state WHERE state_key = $1 AND membership='join'" +
	") AND type = 'm.room.member' AND state_key = ANY($2) AND membership IN ('join', 'invite');"

const selectMembershipCount = `SELECT count(*) FROM syncapi_current_room_state WHERE type = 'm.room.member' AND room_id = $1 AND membership = $2`

const selectRoomHeroes = `
SELECT state_key FROM syncapi_current_room_state
WHERE type = 'm.room.member' AND room_id = $1 AND membership = ANY($2) AND state_key != $3
ORDER BY added_at, state_key
LIMIT 5
`

type currentRoomStateStatements struct {
	upsertRoomStateStmt                *sql.Stmt
	upsertRoomStatesStmt               *sql.Stmt
	deleteRoomStateByEventIDStmt       *sql.Stmt
	deleteRoomStateByEventIDsStmt      *sql.Stmt
	deleteRoomStateForRoomStmt         *sql.Stmt
	selectRoomIDsWithMembershipStmt    *sql.Stmt
	selectKickedRoomIDsStmt            *sql.Stmt
	selectRoomIDsWithAnyMembershipStmt *sql.Stmt
	selectCurrentStateStmt             *sql.Stmt
	selectJoinedUsersStmt              *sql.Stmt
	selectJoinedUsersInRoomStmt        *sql.Stmt
	selectEventsWithEventIDsStmt       *sql.Stmt
	selectStateEventStmt               *sql.Stmt
	selectStateEventsStmt              *sql.Stmt
	selectSharedUsersStmt              *sql.Stmt
	selectMembershipCountStmt          *sql.Stmt
	selectRoomHeroesStmt               *sql.Stmt
}

func NewPostgresCurrentRoomStateTable(db *sql.DB) (tables.CurrentRoomState, error) {
	s := &currentRoomStateStatements{}
	_, err := db.Exec(currentRoomStateSchema)
	if err != nil {
		return nil, err
	}

	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "syncapi: add history visibility column (current_room_state)",
		Up:      deltas.UpAddHistoryVisibilityColumnCurrentRoomState,
	})
	err = m.Up(context.Background())
	if err != nil {
		return nil, err
	}

	return s, sqlutil.StatementList{
		{&s.upsertRoomStateStmt, upsertRoomStateSQL},
		{&s.upsertRoomStatesStmt, upsertRoomStatesSQL},
		{&s.deleteRoomStateByEventIDStmt, deleteRoomStateByEventIDSQL},
		{&s.deleteRoomStateByEventIDsStmt, deleteRoomStateByEventIDsSQL},
		{&s.deleteRoomStateForRoomStmt, deleteRoomStateForRoomSQL},
		{&s.selectRoomIDsWithMembershipStmt, selectRoomIDsWithMembershipSQL},
		{&s.selectKickedRoomIDsStmt, selectKickedRoomIDsSQL},
		{&s.selectRoomIDsWithAnyMembershipStmt, selectRoomIDsWithAnyMembershipSQL},
		{&s.selectCurrentStateStmt, selectCurrentStateSQL},
		{&s.selectJoinedUsersStmt, selectJoinedUsersSQL},
		{&s.selectJoinedUsersInRoomStmt, selectJoinedUsersInRoomSQL},
		{&s.selectEventsWithEventIDsStmt, selectEventsWithEventIDsSQL},
		{&s.selectStateEventStmt, selectStateEventSQL},
		{&s.selectStateEventsStmt, selectStateEventsSQL},
		{&s.selectSharedUsersStmt, selectSharedUsersSQL},
		{&s.selectMembershipCountStmt, selectMembershipCount},
		{&s.selectRoomHeroesStmt, selectRoomHeroes},
	}.Prepare(db)
}

// SelectJoinedUsers returns a map of room ID to a list of joined user IDs.
func (s *currentRoomStateStatements) SelectJoinedUsers(
	ctx context.Context, txn *sql.Tx,
) (map[string][]string, error) {
	selectStmt := sqlutil.TxStmt(txn, s.selectJoinedUsersStmt)
	rows, err := selectStmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsers: rows.close() failed")

	result := make(map[string][]string)
	var roomID string
	var userID string
	for rows.Next() {
		if err := rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		users := result[roomID]
		users = append(users, userID)
		result[roomID] = users
	}
	return result, rows.Err()
}

// SelectJoinedUsersInRoom returns a map of room ID to a list of joined user IDs for a given room.
func (s *currentRoomStateStatements) SelectJoinedUsersInRoom(
	ctx context.Context, txn *sql.Tx, roomIDs []string,
) (map[string][]string, error) {
	selectStmt := sqlutil.TxStmt(txn, s.selectJoinedUsersInRoomStmt)
	rows, err := selectStmt.QueryContext(ctx, roomIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectJoinedUsers: rows.close() failed")

	result := make(map[string][]string)
	var userID, roomID string
	for rows.Next() {
		if err := rows.Scan(&roomID, &userID); err != nil {
			return nil, err
		}
		users := result[roomID]
		users = append(users, userID)
		result[roomID] = users
	}
	return result, rows.Err()
}

// SelectRoomIDsWithMembership returns the list of room IDs which have the given user in the given membership state.
func (s *currentRoomStateStatements) SelectRoomIDsWithMembership(
	ctx context.Context,
	txn *sql.Tx,
	userID string,
	membership string,
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomIDsWithMembershipStmt)
	rows, err := stmt.QueryContext(ctx, userID, membership)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomIDsWithMembership: rows.close() failed")

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

// SelectKickedRoomIDs returns rooms where the user was kicked (leave membership where sender != user).
// Per MSC4186/Synapse behavior, kicked rooms should be included in the sliding sync room list.
func (s *currentRoomStateStatements) SelectKickedRoomIDs(
	ctx context.Context,
	txn *sql.Tx,
	userID string,
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectKickedRoomIDsStmt)
	rows, err := stmt.QueryContext(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectKickedRoomIDs: rows.close() failed")

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

// SelectRoomIDsWithAnyMembership returns a map of all memberships for the given user.
func (s *currentRoomStateStatements) SelectRoomIDsWithAnyMembership(
	ctx context.Context,
	txn *sql.Tx,
	userID string,
) (map[string]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomIDsWithAnyMembershipStmt)
	rows, err := stmt.QueryContext(ctx, userID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomIDsWithAnyMembership: rows.close() failed")

	result := map[string]string{}
	for rows.Next() {
		var roomID string
		var membership string
		if err := rows.Scan(&roomID, &membership); err != nil {
			return nil, err
		}
		result[roomID] = membership
	}
	return result, rows.Err()
}

// SelectCurrentState returns all the current state events for the given room.
func (s *currentRoomStateStatements) SelectCurrentState(
	ctx context.Context, txn *sql.Tx, roomID string,
	stateFilter *synctypes.StateFilter,
	excludeEventIDs []string,
) ([]*rstypes.HeaderedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectCurrentStateStmt)
	senders, notSenders := getSendersStateFilterFilter(stateFilter)
	// We're going to query members later, so remove them from this request
	if stateFilter.LazyLoadMembers && !stateFilter.IncludeRedundantMembers {
		notTypes := &[]string{spec.MRoomMember}
		if stateFilter.NotTypes != nil {
			*stateFilter.NotTypes = append(*stateFilter.NotTypes, spec.MRoomMember)
		} else {
			stateFilter.NotTypes = notTypes
		}
	}
	rows, err := stmt.QueryContext(ctx, roomID,
		senders,
		notSenders,
		filterConvertTypeWildcardToSQL(stateFilter.Types),
		filterConvertTypeWildcardToSQL(stateFilter.NotTypes),
		stateFilter.ContainsURL,
		excludeEventIDs,
	)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectCurrentState: rows.close() failed")

	return rowsToEvents(rows)
}

func (s *currentRoomStateStatements) DeleteRoomStateByEventID(
	ctx context.Context, txn *sql.Tx, eventID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteRoomStateByEventIDStmt)
	_, err := stmt.ExecContext(ctx, eventID)
	return err
}

func (s *currentRoomStateStatements) DeleteRoomStateByEventIDs(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) error {
	if len(eventIDs) == 0 {
		return nil
	}
	stmt := sqlutil.TxStmt(txn, s.deleteRoomStateByEventIDsStmt)
	_, err := stmt.ExecContext(ctx, eventIDs)
	return err
}

func (s *currentRoomStateStatements) DeleteRoomStateForRoom(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteRoomStateForRoomStmt)
	_, err := stmt.ExecContext(ctx, roomID)
	return err
}

func (s *currentRoomStateStatements) UpsertRoomState(
	ctx context.Context, txn *sql.Tx,
	event *rstypes.HeaderedEvent, membership *string, addedAt types.StreamPosition,
) error {
	containsURL, headeredJSON, err := stateRowFields(event)
	if err != nil {
		return err
	}

	// upsert state event
	stmt := sqlutil.TxStmt(txn, s.upsertRoomStateStmt)
	_, err = stmt.ExecContext(
		ctx,
		event.RoomID().String(),
		event.EventID(),
		event.Type(),
		event.UserID.String(),
		containsURL,
		*event.StateKeyResolved,
		headeredJSON,
		membership,
		addedAt,
		event.Visibility,
	)
	return err
}

// stateRowFields derives the columns of a current state row that are not
// copied directly from the event.
func stateRowFields(event *rstypes.HeaderedEvent) (containsURL bool, headeredJSON []byte, err error) {
	// Parse content as JSON and search for an "url" key
	var content map[string]any
	if json.Unmarshal(event.Content(), &content) != nil {
		// Set containsURL to true if url is present
		_, containsURL = content["url"]
	}
	headeredJSON, err = json.Marshal(event)
	return containsURL, headeredJSON, err
}

// UpsertRoomStates writes the upserts in chunks of upsertChunkSize rows, so a
// room with tens of thousands of state events costs tens of round trips
// rather than tens of thousands.
func (s *currentRoomStateStatements) UpsertRoomStates(
	ctx context.Context, txn *sql.Tx, upserts []tables.StateUpsert, addedAt types.StreamPosition,
) error {
	upserts = sqlutil.LastByKey(upserts, func(u tables.StateUpsert) [3]string {
		return [3]string{u.Event.RoomID().String(), u.Event.Type(), *u.Event.StateKeyResolved}
	})
	stmt := sqlutil.TxStmt(txn, s.upsertRoomStatesStmt)
	for chunk := range slices.Chunk(upserts, upsertChunkSize) {
		roomIDs := make([]string, len(chunk))
		eventIDs := make([]string, len(chunk))
		evTypes := make([]string, len(chunk))
		senders := make([]string, len(chunk))
		containsURLs := make([]bool, len(chunk))
		stateKeys := make([]string, len(chunk))
		headeredJSONs := make([]string, len(chunk))
		memberships := make([]*string, len(chunk))
		visibilities := make([]int64, len(chunk))
		for i, u := range chunk {
			containsURL, headeredJSON, err := stateRowFields(u.Event)
			if err != nil {
				return err
			}
			visibility, err := u.Event.Visibility.Value()
			if err != nil {
				return err
			}
			roomIDs[i] = u.Event.RoomID().String()
			eventIDs[i] = u.Event.EventID()
			evTypes[i] = u.Event.Type()
			senders[i] = u.Event.UserID.String()
			containsURLs[i] = containsURL
			stateKeys[i] = *u.Event.StateKeyResolved
			headeredJSONs[i] = string(headeredJSON)
			memberships[i] = u.Membership
			v, ok := visibility.(int64)
			if !ok {
				return fmt.Errorf("history visibility %q encodes as %T, want int64", u.Event.Visibility, visibility)
			}
			visibilities[i] = v
		}
		if _, err := stmt.ExecContext(
			ctx, roomIDs, eventIDs, evTypes, senders, containsURLs, stateKeys,
			headeredJSONs, memberships, addedAt, visibilities,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *currentRoomStateStatements) SelectStateEvents(
	ctx context.Context, txn *sql.Tx, roomID string, keys []gomatrixserverlib.StateKeyTuple,
) (map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent, error) {
	result := make(map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent, len(keys))
	stmt := sqlutil.TxStmt(txn, s.selectStateEventsStmt)
	for chunk := range slices.Chunk(keys, upsertChunkSize) {
		evTypes := make([]string, len(chunk))
		stateKeys := make([]string, len(chunk))
		for i, key := range chunk {
			evTypes[i] = key.EventType
			stateKeys[i] = key.StateKey
		}
		if err := func() error {
			rows, err := stmt.QueryContext(ctx, roomID, evTypes, stateKeys)
			if err != nil {
				return err
			}
			defer internal.CloseAndLogIfError(ctx, rows, "selectStateEvents: rows.close() failed")
			for rows.Next() {
				var key gomatrixserverlib.StateKeyTuple
				var eventBytes []byte
				if err = rows.Scan(&key.EventType, &key.StateKey, &eventBytes); err != nil {
					return err
				}
				var ev rstypes.HeaderedEvent
				if err = json.Unmarshal(eventBytes, &ev); err != nil {
					return err
				}
				result[key] = &ev
			}
			return rows.Err()
		}(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *currentRoomStateStatements) SelectEventsWithEventIDs(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) ([]types.StreamEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectEventsWithEventIDsStmt)
	rows, err := stmt.QueryContext(ctx, eventIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventsWithEventIDs: rows.close() failed")
	return currentRoomStateRowsToStreamEvents(rows)
}

func currentRoomStateRowsToStreamEvents(rows *sql.Rows) ([]types.StreamEvent, error) {
	var events []types.StreamEvent
	for rows.Next() {
		var (
			eventID           string
			streamPos         types.StreamPosition
			eventBytes        []byte
			historyVisibility gomatrixserverlib.HistoryVisibility
		)
		if err := rows.Scan(&eventID, &streamPos, &eventBytes, &historyVisibility); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}

		ev.Visibility = historyVisibility

		events = append(events, types.StreamEvent{
			HeaderedEvent:  &ev,
			StreamPosition: streamPos,
		})
	}

	return events, rows.Err()
}

func rowsToEvents(rows *sql.Rows) ([]*rstypes.HeaderedEvent, error) {
	result := []*rstypes.HeaderedEvent{}
	for rows.Next() {
		var eventID string
		var eventBytes []byte
		if err := rows.Scan(&eventID, &eventBytes); err != nil {
			return nil, err
		}
		// TODO: Handle redacted events
		var ev rstypes.HeaderedEvent
		if err := json.Unmarshal(eventBytes, &ev); err != nil {
			return nil, err
		}
		result = append(result, &ev)
	}
	return result, rows.Err()
}

func (s *currentRoomStateStatements) SelectStateEvent(
	ctx context.Context, txn *sql.Tx, roomID, evType, stateKey string,
) (*rstypes.HeaderedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectStateEventStmt)
	var res []byte
	err := stmt.QueryRowContext(ctx, roomID, evType, stateKey).Scan(&res)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ev rstypes.HeaderedEvent
	if err = json.Unmarshal(res, &ev); err != nil {
		return nil, err
	}
	return &ev, err
}

func (s *currentRoomStateStatements) SelectSharedUsers(
	ctx context.Context, txn *sql.Tx, userID string, otherUserIDs []string,
) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectSharedUsersStmt)
	rows, err := stmt.QueryContext(ctx, userID, otherUserIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectSharedUsersStmt: rows.close() failed")

	var stateKey string
	result := make([]string, 0, len(otherUserIDs))
	for rows.Next() {
		if err := rows.Scan(&stateKey); err != nil {
			return nil, err
		}
		result = append(result, stateKey)
	}
	return result, rows.Err()
}

func (s *currentRoomStateStatements) SelectRoomHeroes(ctx context.Context, txn *sql.Tx, roomID, excludeUserID string, memberships []string) ([]string, error) {
	stmt := sqlutil.TxStmt(txn, s.selectRoomHeroesStmt)
	rows, err := stmt.QueryContext(ctx, roomID, memberships, excludeUserID)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomHeroesStmt: rows.close() failed")

	var stateKey string
	result := make([]string, 0, 5)
	for rows.Next() {
		if err = rows.Scan(&stateKey); err != nil {
			return nil, err
		}
		result = append(result, stateKey)
	}
	return result, rows.Err()
}

func (s *currentRoomStateStatements) SelectMembershipCount(ctx context.Context, txn *sql.Tx, roomID, membership string) (count int, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMembershipCountStmt)
	err = stmt.QueryRowContext(ctx, roomID, membership).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}
