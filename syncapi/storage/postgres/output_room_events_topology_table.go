// Copyright 2018-2024 New Vector Ltd.
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
	"codefloe.com/pat-s/zendrite/internal/depth"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const outputRoomEventsTopologySchema = `
-- Stores output room events received from the roomserver.
CREATE TABLE IF NOT EXISTS syncapi_output_room_events_topology (
	-- The event ID for the event.
    event_id TEXT PRIMARY KEY,
	-- The place of the event in the room's topology. This can usually be determined
	-- from the event's depth.
	topological_position BIGINT NOT NULL,
	stream_position BIGINT NOT NULL,
    -- The 'room_id' key for the event.
    room_id TEXT NOT NULL
);
-- The topological order will be used in events selection and ordering
CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_topological_position_idx ON syncapi_output_room_events_topology(topological_position, stream_position, room_id);
-- Repair scans from a topological position within a room
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_topology_room_topo_idx ON syncapi_output_room_events_topology(room_id, topological_position);
-- Lookups of an event by its stream position within a room
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_topology_room_stream_idx ON syncapi_output_room_events_topology(room_id, stream_position);
`

const insertEventInTopologySQL = "" +
	"INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, room_id, stream_position)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (topological_position, stream_position, room_id) DO UPDATE SET event_id = $1" +
	" RETURNING topological_position"

const selectEventIDsInRangeASCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position > $5) OR" +
	"(topological_position = $3 AND stream_position >= 0 AND $3 > $2)" +
	") ORDER BY topological_position ASC, stream_position ASC LIMIT $6"

const selectEventIDsInRangeDESCSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND (" +
	"(topological_position > $2 AND topological_position < $3) OR" +
	"(topological_position = $4 AND stream_position <= $5)" +
	") ORDER BY topological_position DESC, stream_position DESC LIMIT $6"

// A stream token bounds pagination by stream order alone; the events before it
// are still returned in topological order.
const selectEventIDsBeforeStreamPositionSQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND topological_position > $2" +
	" AND (stream_position <= $3 OR ($4 AND topological_position < $5 AND event_id IN" +
	"  (SELECT event_id FROM syncapi_output_room_events WHERE room_id = $1 AND exclude_from_sync = TRUE)))" +
	" AND ($4 OR event_id NOT IN" +
	"  (SELECT event_id FROM syncapi_output_room_events WHERE room_id = $1 AND exclude_from_sync = TRUE))" +
	" ORDER BY topological_position DESC, stream_position DESC LIMIT $6"

const selectPositionInTopologySQL = "" +
	"SELECT topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE event_id = $1"

const selectPositionByStreamPositionSQL = "" +
	"SELECT topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND stream_position = $2"

const selectPositionsInTopologySQL = "" +
	"SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology" +
	" WHERE event_id = ANY($1)"

const selectStreamToTopologicalPositionAscSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position >= $2 ORDER BY topological_position ASC LIMIT 1;"

const selectStreamToTopologicalPositionDescSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position <= $2 ORDER BY topological_position DESC LIMIT 1;"

const selectFirstSyncedDepthAfterStreamPositionSQL = "" +
	"SELECT t.topological_position FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND t.stream_position > $2 AND e.exclude_from_sync = FALSE" +
	" ORDER BY t.stream_position ASC LIMIT 1;"

const purgeEventsTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE room_id = $1"

const lockRoomTopologySQL = "" +
	"SELECT pg_advisory_xact_lock(hashtext($1))"

// The highest topological position among the given events that are already in
// the room's topology, or -1 if none is.
const selectMaxTopologyPositionSQL = "" +
	"SELECT COALESCE(MAX(topological_position), -1) FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND event_id = ANY($2)"

// The first delivered, non-create state event establishes when the room's
// state timeline became live. Backfilled messages must not drag the boundary
// into history. PostgreSQL rejects \u0000 and lone-surrogate escapes that Go
// accepts, so replace them before parsing the JSON; only the presence of
// state_key matters here, not the decoded value.
const selectLiveStartPositionSQL = "" +
	"SELECT t.topological_position FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND e.exclude_from_sync = FALSE" +
	" AND e.type <> 'm.room.create'" +
	" AND CASE WHEN e.headered_event_json LIKE '%\\\\u%'" +
	" THEN regexp_replace(e.headered_event_json, '\\\\u(0000|[dD][89a-fA-F][0-9a-fA-F]{2})', '\\\\uFFFD', 'g')" +
	" ELSE e.headered_event_json END::json -> 'state_key' IS NOT NULL" +
	" ORDER BY t.topological_position ASC LIMIT 1"

// Whether a backfilled event has a greater topological position in the room.
const selectExcludedEventsAfterTopologyPositionSQL = "" +
	"SELECT EXISTS (SELECT 1 FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND e.exclude_from_sync = TRUE" +
	" AND t.topological_position > $2)"

// The last backfilled event after a topological position, for anchoring
// backward pagination when no delivered event follows it.
const selectLastExcludedEventAfterTopologyPositionSQL = "" +
	"SELECT t.topological_position, t.stream_position" +
	" FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND e.exclude_from_sync = TRUE" +
	" AND (t.topological_position > $2 OR (t.topological_position = $2 AND t.stream_position > $3))" +
	" AND (t.topological_position < $4 OR (t.topological_position = $4 AND t.stream_position <= $5))" +
	" ORDER BY t.topological_position DESC, t.stream_position DESC LIMIT 1"

const deleteEventFromTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE event_id = $1"

const updateEventPositionSQL = "" +
	"UPDATE syncapi_output_room_events_topology SET topological_position = $1 WHERE event_id = $2 AND topological_position < $1"

const selectRoomEventTopologySQL = "" +
	"SELECT e.headered_event_json, t.topological_position FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND t.topological_position >= $2 ORDER BY t.stream_position, t.event_id"

type outputRoomEventsTopologyStatements struct {
	lockRoomTopologyStmt                             *sql.Stmt
	insertEventInTopologyStmt                        *sql.Stmt
	selectEventIDsInRangeASCStmt                     *sql.Stmt
	selectEventIDsInRangeDESCStmt                    *sql.Stmt
	selectEventIDsBeforeStreamPositionStmt           *sql.Stmt
	selectPositionInTopologyStmt                     *sql.Stmt
	selectLiveStartPositionStmt                      *sql.Stmt
	selectExcludedEventsAfterTopologyPositionStmt    *sql.Stmt
	selectLastExcludedEventAfterTopologyPositionStmt *sql.Stmt
	selectPositionByStreamPositionStmt               *sql.Stmt
	selectPositionsInTopologyStmt                    *sql.Stmt
	selectStreamToTopologicalPositionAscStmt         *sql.Stmt
	selectStreamToTopologicalPositionDescStmt        *sql.Stmt
	selectFirstSyncedDepthAfterStreamPositionStmt    *sql.Stmt
	purgeEventsTopologyStmt                          *sql.Stmt
	deleteEventFromTopologyStmt                      *sql.Stmt
	selectMaxTopologyPositionStmt                    *sql.Stmt
	updateEventPositionStmt                          *sql.Stmt
	selectRoomEventTopologyStmt                      *sql.Stmt
}

func NewPostgresTopologyTable(db *sql.DB) (tables.Topology, error) {
	s := &outputRoomEventsTopologyStatements{}
	_, err := db.Exec(outputRoomEventsTopologySchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.lockRoomTopologyStmt, lockRoomTopologySQL},
		{&s.insertEventInTopologyStmt, insertEventInTopologySQL},
		{&s.selectEventIDsInRangeASCStmt, selectEventIDsInRangeASCSQL},
		{&s.selectEventIDsInRangeDESCStmt, selectEventIDsInRangeDESCSQL},
		{&s.selectEventIDsBeforeStreamPositionStmt, selectEventIDsBeforeStreamPositionSQL},
		{&s.selectPositionInTopologyStmt, selectPositionInTopologySQL},
		{&s.selectLiveStartPositionStmt, selectLiveStartPositionSQL},
		{&s.selectExcludedEventsAfterTopologyPositionStmt, selectExcludedEventsAfterTopologyPositionSQL},
		{&s.selectLastExcludedEventAfterTopologyPositionStmt, selectLastExcludedEventAfterTopologyPositionSQL},
		{&s.selectPositionByStreamPositionStmt, selectPositionByStreamPositionSQL},
		{&s.selectPositionsInTopologyStmt, selectPositionsInTopologySQL},
		{&s.selectStreamToTopologicalPositionAscStmt, selectStreamToTopologicalPositionAscSQL},
		{&s.selectStreamToTopologicalPositionDescStmt, selectStreamToTopologicalPositionDescSQL},
		{&s.selectFirstSyncedDepthAfterStreamPositionStmt, selectFirstSyncedDepthAfterStreamPositionSQL},
		{&s.purgeEventsTopologyStmt, purgeEventsTopologySQL},
		{&s.deleteEventFromTopologyStmt, deleteEventFromTopologySQL},
		{&s.selectMaxTopologyPositionStmt, selectMaxTopologyPositionSQL},
		{&s.updateEventPositionStmt, updateEventPositionSQL},
		{&s.selectRoomEventTopologyStmt, selectRoomEventTopologySQL},
	}.Prepare(db)
}

func (s *outputRoomEventsTopologyStatements) LockRoomTopology(ctx context.Context, txn *sql.Tx, roomID string) error {
	if txn == nil {
		return errors.New("LockRoomTopology called without a transaction")
	}
	_, err := sqlutil.TxStmt(txn, s.lockRoomTopologyStmt).ExecContext(ctx, roomID)
	return err
}

func (s *outputRoomEventsTopologyStatements) UpdateEventPosition(
	ctx context.Context, txn *sql.Tx, eventID string, topoPos types.StreamPosition,
) error {
	_, err := sqlutil.TxStmt(txn, s.updateEventPositionStmt).ExecContext(ctx, topoPos, eventID)
	return err
}

func (s *outputRoomEventsTopologyStatements) SelectRoomEventTopology(
	ctx context.Context, txn *sql.Tx, roomID string, minTopologicalPos types.StreamPosition,
) ([]tables.StoredEventTopology, error) {
	rows, err := sqlutil.TxStmt(txn, s.selectRoomEventTopologyStmt).QueryContext(ctx, roomID, minTopologicalPos)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRoomEventTopology: rows.close() failed")

	var events []tables.StoredEventTopology
	for rows.Next() {
		var eventJSON string
		var position types.StreamPosition
		if err = rows.Scan(&eventJSON, &position); err != nil {
			return nil, err
		}
		var event rstypes.HeaderedEvent
		if err = json.Unmarshal([]byte(eventJSON), &event); err != nil {
			return nil, err
		}
		events = append(events, tables.StoredEventTopology{Event: &event, Position: position})
	}
	return events, rows.Err()
}

// InsertEventInTopology inserts the given event in the room's topology, based
// on the event's depth.
func (s *outputRoomEventsTopologyStatements) InsertEventInTopology(
	ctx context.Context, txn *sql.Tx, event *rstypes.HeaderedEvent, pos types.StreamPosition,
) (topoPos types.StreamPosition, err error) {
	depth, err := s.ProspectiveEventPosition(ctx, txn, event)
	if err != nil {
		return 0, err
	}
	err = sqlutil.TxStmt(txn, s.insertEventInTopologyStmt).QueryRowContext(
		ctx, event.EventID(), depth, event.RoomID().String(), pos,
	).Scan(&topoPos)
	return
}

// SelectEventIDsInRange selects the IDs of events which positions are within a
// given range in a given room's topological order. Returns the start/end topological tokens for
// the returned eventIDs.
// Returns an empty slice if no events match the given range.
func (s *outputRoomEventsTopologyStatements) SelectEventIDsInRange(
	ctx context.Context, txn *sql.Tx, roomID string, minDepth, maxDepth, maxStreamPos types.StreamPosition,
	limit int, chronologicalOrder bool,
) (eventIDs []string, start, end types.TopologyToken, err error) {
	// Decide on the selection's order according to whether chronological order
	// is requested or not.
	var stmt *sql.Stmt
	if chronologicalOrder {
		stmt = sqlutil.TxStmt(txn, s.selectEventIDsInRangeASCStmt)
	} else {
		stmt = sqlutil.TxStmt(txn, s.selectEventIDsInRangeDESCStmt)
	}

	// Query the event IDs.
	boundaryDepth := maxDepth
	if chronologicalOrder {
		// Forward cursors sit at the lower bound; backward cursors at the upper.
		boundaryDepth = minDepth
	}
	rows, err := stmt.QueryContext(ctx, roomID, minDepth, maxDepth, boundaryDepth, maxStreamPos, limit)
	if errors.Is(err, sql.ErrNoRows) {
		// If no event matched the request, return an empty slice.
		return []string{}, start, end, nil
	} else if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventIDsInRange: rows.close() failed")

	// Return the IDs.
	var eventID string
	var token types.TopologyToken
	var tokens []types.TopologyToken
	for rows.Next() {
		if err = rows.Scan(&eventID, &token.Depth, &token.PDUPosition); err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
		tokens = append(tokens, token)
	}

	// The values are already ordered by SQL, so we can use them as is.
	if len(tokens) > 0 {
		start = tokens[0]
		end = tokens[len(tokens)-1]
	}

	return eventIDs, start, end, rows.Err()
}

// SelectEventIDsBeforeStreamPosition selects, newest first in topological order,
// the events of a room at or before a stream position and deeper than minDepth.
// When includeBackfilled is set, events excluded from sync (backfilled history)
// are also returned when they were persisted after the position, but only below
// maxBackfillDepth. When it is not set, excluded events are dropped even if
// they were persisted before the position.
func (s *outputRoomEventsTopologyStatements) SelectEventIDsBeforeStreamPosition(
	ctx context.Context, txn *sql.Tx, roomID string, minDepth, maxStreamPos, maxBackfillDepth types.StreamPosition,
	limit int, includeBackfilled bool,
) (eventIDs []string, start, end types.TopologyToken, err error) {
	rows, err := sqlutil.TxStmt(txn, s.selectEventIDsBeforeStreamPositionStmt).QueryContext(
		ctx, roomID, minDepth, maxStreamPos, includeBackfilled, maxBackfillDepth, limit,
	)
	if err != nil {
		return
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectEventIDsBeforeStreamPosition: rows.close() failed")

	var eventID string
	var token types.TopologyToken
	var tokens []types.TopologyToken
	for rows.Next() {
		if err = rows.Scan(&eventID, &token.Depth, &token.PDUPosition); err != nil {
			return
		}
		eventIDs = append(eventIDs, eventID)
		tokens = append(tokens, token)
	}
	if len(tokens) > 0 {
		start = tokens[0]
		end = tokens[len(tokens)-1]
	}
	return eventIDs, start, end, rows.Err()
}

// SelectPositionInTopology returns the position of a given event in the
// topology of the room it belongs to.
func (s *outputRoomEventsTopologyStatements) SelectPositionInTopology(
	ctx context.Context, txn *sql.Tx, eventID string,
) (pos, spos types.StreamPosition, err error) {
	err = sqlutil.TxStmt(txn, s.selectPositionInTopologyStmt).QueryRowContext(ctx, eventID).Scan(&pos, &spos)
	return
}

// SelectLiveStartPosition returns the first delivered non-create state event's
// topological position, and false when the room has no such event.
func (s *outputRoomEventsTopologyStatements) SelectLiveStartPosition(
	ctx context.Context, txn *sql.Tx, roomID string,
) (types.StreamPosition, bool, error) {
	var pos sql.NullInt64
	err := sqlutil.TxStmt(txn, s.selectLiveStartPositionStmt).QueryRowContext(ctx, roomID).Scan(&pos)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil || !pos.Valid {
		return 0, false, err
	}
	return types.StreamPosition(pos.Int64), true, nil
}

// HasExcludedEventsAfterTopologyPosition reports whether an event excluded
// from sync has a greater topological position in the room.
func (s *outputRoomEventsTopologyStatements) HasExcludedEventsAfterTopologyPosition(
	ctx context.Context, txn *sql.Tx, roomID string, position types.StreamPosition,
) (bool, error) {
	var exists bool
	err := sqlutil.TxStmt(txn, s.selectExcludedEventsAfterTopologyPositionStmt).QueryRowContext(
		ctx, roomID, position,
	).Scan(&exists)
	return exists, err
}

// LastExcludedEventAfterTopologyPosition returns the newest event excluded
// from sync after position and at or before through in the room's topology.
func (s *outputRoomEventsTopologyStatements) LastExcludedEventAfterTopologyPosition(
	ctx context.Context, txn *sql.Tx, roomID string, position, through types.TopologyToken,
) (types.TopologyToken, bool, error) {
	var token types.TopologyToken
	err := sqlutil.TxStmt(txn, s.selectLastExcludedEventAfterTopologyPositionStmt).QueryRowContext(
		ctx, roomID, position.Depth, position.PDUPosition, through.Depth, through.PDUPosition,
	).Scan(&token.Depth, &token.PDUPosition)
	if errors.Is(err, sql.ErrNoRows) {
		return types.TopologyToken{}, false, nil
	}
	return token, err == nil, err
}

// ProspectiveEventPosition returns the topological position that
// InsertEventInTopology would assign based on currently stored prev events.
func (s *outputRoomEventsTopologyStatements) ProspectiveEventPosition(
	ctx context.Context, txn *sql.Tx, event *rstypes.HeaderedEvent,
) (types.StreamPosition, error) {
	maxPrev := int64(-1)
	if prevs := event.PrevEventIDs(); len(prevs) > 0 {
		if err := sqlutil.TxStmt(txn, s.selectMaxTopologyPositionStmt).QueryRowContext(
			ctx, event.RoomID().String(), sqlutil.StringArray(prevs),
		).Scan(&maxPrev); err != nil {
			return 0, err
		}
	}
	pos := depth.LiftAbovePrevEvents(event.Depth(), maxPrev)
	return types.StreamPosition(pos), nil
}

func (s *outputRoomEventsTopologyStatements) SelectPositionByStreamPosition(
	ctx context.Context, txn *sql.Tx, roomID string, streamPos types.StreamPosition,
) (depth, spos types.StreamPosition, err error) {
	err = sqlutil.TxStmt(txn, s.selectPositionByStreamPositionStmt).QueryRowContext(ctx, roomID, streamPos).Scan(&depth, &spos)
	return
}

func (s *outputRoomEventsTopologyStatements) SelectPositionsInTopology(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) (map[string]types.TopologyToken, error) {
	positions := make(map[string]types.TopologyToken, len(eventIDs))
	if len(eventIDs) == 0 {
		return positions, nil
	}
	rows, err := sqlutil.TxStmt(txn, s.selectPositionsInTopologyStmt).QueryContext(ctx, eventIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectPositionsInTopology: rows.close() failed")
	for rows.Next() {
		var eventID string
		var position types.TopologyToken
		if err = rows.Scan(&eventID, &position.Depth, &position.PDUPosition); err != nil {
			return nil, err
		}
		positions[eventID] = position
	}
	return positions, rows.Err()
}

// SelectStreamToTopologicalPosition returns the closest position of a given event
// in the topology of the room it belongs to from the given stream position.
func (s *outputRoomEventsTopologyStatements) SelectStreamToTopologicalPosition(
	ctx context.Context, txn *sql.Tx, roomID string, streamPos types.StreamPosition, backwardOrdering bool,
) (topoPos types.StreamPosition, err error) {
	if backwardOrdering {
		err = sqlutil.TxStmt(txn, s.selectStreamToTopologicalPositionDescStmt).QueryRowContext(ctx, roomID, streamPos).Scan(&topoPos)
	} else {
		err = sqlutil.TxStmt(txn, s.selectStreamToTopologicalPositionAscStmt).QueryRowContext(ctx, roomID, streamPos).Scan(&topoPos)
	}
	return
}

// SelectFirstSyncedDepthAfterStreamPosition returns the depth of the first
// event, in stream order, that was persisted after the given stream position and
// was not backfilled. It is the event a prev_batch token issued before any
// stored event refers to.
func (s *outputRoomEventsTopologyStatements) SelectFirstSyncedDepthAfterStreamPosition(
	ctx context.Context, txn *sql.Tx, roomID string, streamPos types.StreamPosition,
) (topoPos types.StreamPosition, err error) {
	err = sqlutil.TxStmt(txn, s.selectFirstSyncedDepthAfterStreamPositionStmt).QueryRowContext(ctx, roomID, streamPos).Scan(&topoPos)
	return
}

func (s *outputRoomEventsTopologyStatements) PurgeEventsTopology(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	purgeEventsTopologyStmt := sqlutil.TxStmt(txn, s.purgeEventsTopologyStmt)
	_, err := purgeEventsTopologyStmt.ExecContext(ctx, roomID)
	return err
}

func (s *outputRoomEventsTopologyStatements) DeleteEventFromTopology(
	ctx context.Context, txn *sql.Tx, eventID string,
) error {
	deleteEventFromTopologyStmt := sqlutil.TxStmt(txn, s.deleteEventFromTopologyStmt)
	_, err := deleteEventFromTopologyStmt.ExecContext(ctx, eventID)
	return err
}
