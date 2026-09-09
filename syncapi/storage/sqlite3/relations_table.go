// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const relationsSchema = `
CREATE TABLE IF NOT EXISTS syncapi_relations (
	id BIGINT PRIMARY KEY,
	room_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	child_event_id TEXT NOT NULL,
	child_event_type TEXT NOT NULL,
	rel_type TEXT NOT NULL,
	UNIQUE (room_id, event_id, child_event_id, rel_type)
);
`

const insertRelationSQL = "" +
	"INSERT INTO syncapi_relations (" +
	"  id, room_id, event_id, child_event_id, child_event_type, rel_type" +
	") VALUES ($1, $2, $3, $4, $5, $6) " +
	" ON CONFLICT DO NOTHING"

const deleteRelationSQL = "" +
	"DELETE FROM syncapi_relations WHERE room_id = $1 AND child_event_id = $2"

const selectRelationsInRangeAscSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE room_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id > $5 AND id <= $6" +
	" ORDER BY id ASC LIMIT $7"

const selectRelationsInRangeDescSQL = "" +
	"SELECT id, child_event_id, rel_type FROM syncapi_relations" +
	" WHERE room_id = $1 AND event_id = $2" +
	" AND ( $3 = '' OR rel_type = $3 )" +
	" AND ( $4 = '' OR child_event_type = $4 )" +
	" AND id >= $5 AND id < $6" +
	" ORDER BY id DESC LIMIT $7"

const selectMaxRelationIDSQL = "" +
	"SELECT COALESCE(MAX(id), 0) FROM syncapi_relations"

// Page through individual thread replies in topology order. The relation watermark
// freezes the input across requests, including replies backfilled at older depths.
const selectThreadRelationsSQL = `
SELECT r.event_id, r.child_event_id, t.topological_position, t.stream_position
FROM syncapi_relations r
JOIN syncapi_output_room_events_topology t ON t.event_id = r.child_event_id AND t.room_id = r.room_id
WHERE r.room_id = $1 AND r.rel_type = 'm.thread' AND r.id <= $2
AND (t.topological_position < $3 OR (t.topological_position = $3 AND t.stream_position < $4))
ORDER BY t.topological_position DESC, t.stream_position DESC LIMIT $5
`

type relationsStatements struct {
	selectThreadRelationsStmt      *sql.Stmt
	streamIDStatements             *StreamIDStatements
	insertRelationStmt             *sql.Stmt
	selectRelationsInRangeAscStmt  *sql.Stmt
	selectRelationsInRangeDescStmt *sql.Stmt
	deleteRelationStmt             *sql.Stmt
	selectMaxRelationIDStmt        *sql.Stmt
}

func NewSqliteRelationsTable(db *sql.DB, streamID *StreamIDStatements) (tables.Relations, error) {
	s := &relationsStatements{
		streamIDStatements: streamID,
	}
	_, err := db.Exec(relationsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertRelationStmt, insertRelationSQL},
		{&s.selectThreadRelationsStmt, selectThreadRelationsSQL},
		{&s.selectRelationsInRangeAscStmt, selectRelationsInRangeAscSQL},
		{&s.selectRelationsInRangeDescStmt, selectRelationsInRangeDescSQL},
		{&s.deleteRelationStmt, deleteRelationSQL},
		{&s.selectMaxRelationIDStmt, selectMaxRelationIDSQL},
	}.Prepare(db)
}

func (s *relationsStatements) InsertRelation(
	ctx context.Context, txn *sql.Tx, roomID, eventID, childEventID, childEventType, relType string,
) (err error) {
	var streamPos types.StreamPosition
	if streamPos, err = s.streamIDStatements.nextRelationID(ctx, txn); err != nil {
		return
	}
	insertRelationStmt := sqlutil.TxStmt(txn, s.insertRelationStmt)
	_, err = insertRelationStmt.ExecContext(
		ctx, streamPos, roomID, eventID, childEventID, childEventType, relType,
	)
	return
}

func (s *relationsStatements) DeleteRelation(
	ctx context.Context, txn *sql.Tx, roomID, childEventID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteRelationStmt)
	_, err := stmt.ExecContext(
		ctx, roomID, childEventID,
	)
	return err
}

// SelectRelationsInRange returns a map rel_type -> []child_event_id.
func (s *relationsStatements) SelectRelationsInRange(
	ctx context.Context, txn *sql.Tx, roomID, eventID, relType, eventType string,
	r types.Range, limit int,
) (map[string][]types.RelationEntry, types.StreamPosition, error) {
	var lastPos types.StreamPosition
	var stmt *sql.Stmt
	if r.Backwards {
		stmt = sqlutil.TxStmt(txn, s.selectRelationsInRangeDescStmt)
	} else {
		stmt = sqlutil.TxStmt(txn, s.selectRelationsInRangeAscStmt)
	}
	rows, err := stmt.QueryContext(ctx, roomID, eventID, relType, eventType, r.Low(), r.High(), limit)
	if err != nil {
		return nil, lastPos, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectRelationsInRange: rows.close() failed")
	result := map[string][]types.RelationEntry{}
	var (
		id           types.StreamPosition
		childEventID string
		relationType string
	)
	for rows.Next() {
		if err = rows.Scan(&id, &childEventID, &relationType); err != nil {
			return nil, lastPos, err
		}
		if id > lastPos {
			lastPos = id
		}
		result[relationType] = append(result[relationType], types.RelationEntry{
			Position: id,
			EventID:  childEventID,
		})
	}
	if lastPos == 0 {
		lastPos = r.To
	}
	return result, lastPos, rows.Err()
}

func (s *relationsStatements) SelectMaxRelationID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectMaxRelationIDStmt)
	err = stmt.QueryRowContext(ctx).Scan(&id)
	return
}

// SelectThreadRelations returns replies in descending topology order, before an exclusive cursor.
func (s *relationsStatements) SelectThreadRelations(ctx context.Context, txn *sql.Tx, roomID string, watermark types.StreamPosition, before types.TopologyToken, limit int) ([]types.ThreadRelation, error) {
	rows, err := sqlutil.TxStmt(txn, s.selectThreadRelationsStmt).QueryContext(ctx, roomID, watermark, before.Depth, before.PDUPosition, limit)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectThreadRelations: rows.close() failed")
	var result []types.ThreadRelation
	for rows.Next() {
		var entry types.ThreadRelation
		if err = rows.Scan(&entry.RootEventID, &entry.EventID, &entry.Position.Depth, &entry.Position.PDUPosition); err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}
