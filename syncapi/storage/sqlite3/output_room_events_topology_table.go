// Copyright 2018-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

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
  event_id TEXT PRIMARY KEY,
  topological_position BIGINT NOT NULL,
  stream_position BIGINT NOT NULL,
  room_id TEXT NOT NULL,

	UNIQUE(topological_position, room_id, stream_position)
);
-- Whole-room reads (such as repairing the topology after a late parent) filter
-- on room_id alone, which the unique index above cannot serve.
CREATE INDEX IF NOT EXISTS syncapi_output_room_events_topology_room_stream_idx ON syncapi_output_room_events_topology(room_id, stream_position);
-- The topological order will be used in events selection and ordering
-- CREATE UNIQUE INDEX IF NOT EXISTS syncapi_event_topological_position_idx ON syncapi_output_room_events_topology(topological_position, stream_position, room_id);
`

const insertEventInTopologySQL = "" +
	"INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, room_id, stream_position)" +
	" VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT DO NOTHING"

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
	" AND (stream_position <= $3 OR ($4 AND topological_position <= $5 AND event_id IN" +
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

const selectStreamToTopologicalPositionAscSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position >= $2 ORDER BY topological_position ASC LIMIT 1;"

const selectStreamToTopologicalPositionDescSQL = "" +
	"SELECT topological_position FROM syncapi_output_room_events_topology WHERE room_id = $1 AND stream_position <= $2 ORDER BY topological_position DESC LIMIT 1;"

const selectFirstSyncedDepthAfterStreamPositionSQL = "" +
	"SELECT t.topological_position FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 AND t.stream_position > $2 AND e.exclude_from_sync = 0" +
	" ORDER BY t.stream_position ASC LIMIT 1;"

const purgeEventsTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE room_id = $1"

// The highest topological position among the given events that are already in
// the room's topology, or -1 if none is.
const selectMaxTopologyPositionSQL = "" +
	"SELECT COALESCE(MAX(topological_position), -1) FROM syncapi_output_room_events_topology" +
	" WHERE room_id = $1 AND event_id IN ($2)"

const deleteEventFromTopologySQL = "" +
	"DELETE FROM syncapi_output_room_events_topology WHERE event_id = $1"

const updateEventPositionSQL = "" +
	"UPDATE syncapi_output_room_events_topology SET topological_position = $1 WHERE event_id = $2 AND topological_position < $1"

const selectRoomEventTopologySQL = "" +
	"SELECT e.headered_event_json, t.topological_position FROM syncapi_output_room_events_topology t" +
	" JOIN syncapi_output_room_events e ON e.event_id = t.event_id" +
	" WHERE t.room_id = $1 ORDER BY t.stream_position, t.event_id"

type outputRoomEventsTopologyStatements struct {
	db                                            *sql.DB
	insertEventInTopologyStmt                     *sql.Stmt
	selectEventIDsInRangeASCStmt                  *sql.Stmt
	selectEventIDsInRangeDESCStmt                 *sql.Stmt
	selectEventIDsBeforeStreamPositionStmt        *sql.Stmt
	selectPositionInTopologyStmt                  *sql.Stmt
	selectPositionByStreamPositionStmt            *sql.Stmt
	selectStreamToTopologicalPositionAscStmt      *sql.Stmt
	selectStreamToTopologicalPositionDescStmt     *sql.Stmt
	selectFirstSyncedDepthAfterStreamPositionStmt *sql.Stmt
	updateEventPositionStmt                       *sql.Stmt
	selectRoomEventTopologyStmt                   *sql.Stmt
	purgeEventsTopologyStmt                       *sql.Stmt
	deleteEventFromTopologyStmt                   *sql.Stmt
}

func NewSqliteTopologyTable(db *sql.DB) (tables.Topology, error) {
	s := &outputRoomEventsTopologyStatements{
		db: db,
	}
	_, err := db.Exec(outputRoomEventsTopologySchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertEventInTopologyStmt, insertEventInTopologySQL},
		{&s.selectEventIDsInRangeASCStmt, selectEventIDsInRangeASCSQL},
		{&s.selectEventIDsInRangeDESCStmt, selectEventIDsInRangeDESCSQL},
		{&s.selectEventIDsBeforeStreamPositionStmt, selectEventIDsBeforeStreamPositionSQL},
		{&s.selectPositionInTopologyStmt, selectPositionInTopologySQL},
		{&s.selectPositionByStreamPositionStmt, selectPositionByStreamPositionSQL},
		{&s.selectStreamToTopologicalPositionAscStmt, selectStreamToTopologicalPositionAscSQL},
		{&s.selectStreamToTopologicalPositionDescStmt, selectStreamToTopologicalPositionDescSQL},
		{&s.selectFirstSyncedDepthAfterStreamPositionStmt, selectFirstSyncedDepthAfterStreamPositionSQL},
		{&s.updateEventPositionStmt, updateEventPositionSQL},
		{&s.selectRoomEventTopologyStmt, selectRoomEventTopologySQL},
		{&s.purgeEventsTopologyStmt, purgeEventsTopologySQL},
		{&s.deleteEventFromTopologyStmt, deleteEventFromTopologySQL},
	}.Prepare(db)
}

func (s *outputRoomEventsTopologyStatements) UpdateEventPosition(
	ctx context.Context, txn *sql.Tx, eventID string, topoPos types.StreamPosition,
) error {
	_, err := sqlutil.TxStmt(txn, s.updateEventPositionStmt).ExecContext(ctx, topoPos, eventID)
	return err
}

func (s *outputRoomEventsTopologyStatements) SelectRoomEventTopology(
	ctx context.Context, txn *sql.Tx, roomID string,
) ([]tables.StoredEventTopology, error) {
	rows, err := sqlutil.TxStmt(txn, s.selectRoomEventTopologyStmt).QueryContext(ctx, roomID)
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

// insertEventInTopology inserts the given event in the room's topology, based
// on the event's depth.
func (s *outputRoomEventsTopologyStatements) InsertEventInTopology(
	ctx context.Context, txn *sql.Tx, event *rstypes.HeaderedEvent, pos types.StreamPosition,
) (types.StreamPosition, error) {
	// Clamp the depth to prevent issues with events that have depth values
	// exceeding the canonical JSON integer limit (e.g., from corrupt federation data).
	depth := depth.Clamp(event.Depth())
	// An event must sort after its parents. A server that restarted its depth
	// counter claims a depth below them, which would split the room into two
	// depth ranges, so lift the position above the stored prev events.
	if prevs := event.PrevEventIDs(); len(prevs) > 0 {
		args := make([]any, 0, len(prevs)+1)
		args = append(args, event.RoomID().String())
		for _, prev := range prevs {
			args = append(args, prev)
		}
		query := strings.Replace(selectMaxTopologyPositionSQL, "($2)", sqlutil.QueryVariadicOffset(len(prevs), 1), 1)
		var maxPrev int64
		row := s.db.QueryRowContext
		if txn != nil {
			row = txn.QueryRowContext
		}
		if err := row(ctx, query, args...).Scan(&maxPrev); err != nil {
			return 0, err
		}
		if lifted := maxPrev + 1; maxPrev >= 0 && lifted > depth {
			depth = lifted
		}
	}
	insertEventInTopologyStmt := sqlutil.TxStmt(txn, s.insertEventInTopologyStmt)
	_, err := insertEventInTopologyStmt.ExecContext(
		ctx, event.EventID(), depth, event.RoomID().String(), pos,
	)
	return types.StreamPosition(depth), err
}

// SelectEventIDsInRange selects the IDs of events which positions are within a
// given range in a given room's topological order. Returns the start/end topological tokens for
// the returned eventIDs.
// Returns an empty slice if no events match the given range.
func (s *outputRoomEventsTopologyStatements) SelectEventIDsInRange(
	ctx context.Context, txn *sql.Tx, roomID string,
	minDepth, maxDepth, maxStreamPos types.StreamPosition,
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
	defer internal.CloseAndLogIfError(ctx, rows, "SelectEventIDsInRange: failed to close rows")

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
	err = rows.Err()
	return
}

// SelectEventIDsBeforeStreamPosition selects, newest first in topological order,
// the events of a room at or before a stream position and deeper than minDepth.
// When includeBackfilled is set, events excluded from sync (backfilled history)
// are also returned when they were persisted after the position, but only up to
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
	defer internal.CloseAndLogIfError(ctx, rows, "SelectEventIDsBeforeStreamPosition: failed to close rows")

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

// selectPositionInTopology returns the position of a given event in the
// topology of the room it belongs to.
func (s *outputRoomEventsTopologyStatements) SelectPositionInTopology(
	ctx context.Context, txn *sql.Tx, eventID string,
) (pos types.StreamPosition, spos types.StreamPosition, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectPositionInTopologyStmt)
	err = stmt.QueryRowContext(ctx, eventID).Scan(&pos, &spos)
	return
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
	args := make([]any, len(eventIDs))
	for i, eventID := range eventIDs {
		args[i] = eventID
	}
	query := "SELECT event_id, topological_position, stream_position FROM syncapi_output_room_events_topology WHERE event_id IN " + sqlutil.QueryVariadic(len(args))
	var rows *sql.Rows
	var err error
	if txn != nil {
		rows, err = txn.QueryContext(ctx, query, args...)
	} else {
		rows, err = s.db.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectPositionsInTopology: failed to close rows")
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
