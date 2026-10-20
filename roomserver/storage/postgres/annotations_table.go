// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"errors"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres/deltas"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

const annotationsSchema = `
-- Indexes the m.annotation relations of accepted, unredacted events, so that
-- a user can be stopped from sending the same annotation twice (MSC2677).
CREATE TABLE IF NOT EXISTS roomserver_annotations (
    annotation_event_id TEXT NOT NULL PRIMARY KEY,
    room_nid BIGINT NOT NULL,
    target_event_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    -- Not indexed: a key may be longer than an index entry can hold.
    annotation_key TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS roomserver_annotations_target_idx ON roomserver_annotations (room_nid, target_event_id, sender_id);
`

// An annotation whose redaction has already been validated, for example
// because the redaction arrived first, is not recorded.
const insertAnnotationSQL = "" +
	"INSERT INTO roomserver_annotations (annotation_event_id, room_nid, target_event_id, sender_id, event_type, annotation_key)" +
	" SELECT $1::TEXT, $2::BIGINT, $3::TEXT, $4::TEXT, $5::TEXT, $6::TEXT" +
	" WHERE NOT EXISTS (SELECT 1 FROM roomserver_redactions WHERE redacts_event_id = $1 AND validated = TRUE)" +
	" ON CONFLICT DO NOTHING"

const selectAnnotationExistsSQL = "" +
	"SELECT 1 FROM roomserver_annotations" +
	" WHERE room_nid = $1 AND target_event_id = $2 AND sender_id = $3 AND event_type = $4 AND annotation_key = $5" +
	" AND annotation_event_id <> $6" +
	// Builds before the index existed validate redactions without deleting
	// rows, so a rolled-back deployment may leave rows of redacted events.
	" AND NOT EXISTS (SELECT 1 FROM roomserver_redactions" +
	" WHERE redacts_event_id = annotation_event_id AND validated = TRUE)" +
	" LIMIT 1"

const deleteAnnotationSQL = "" +
	"DELETE FROM roomserver_annotations WHERE annotation_event_id = $1"

type annotationsStatements struct {
	insertAnnotationStmt       *sql.Stmt
	selectAnnotationExistsStmt *sql.Stmt
	deleteAnnotationStmt       *sql.Stmt
}

// CreateAnnotationsTable creates the table and fills it from the stored
// events, so it needs the events, event JSON and event types tables.
func CreateAnnotationsTable(db *sql.DB) error {
	if _, err := db.Exec(annotationsSchema); err != nil {
		return err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "roomserver: backfill annotations",
		Up:      deltas.UpBackfillAnnotations,
	})
	return m.Up(context.Background())
}

func PrepareAnnotationsTable(db *sql.DB) (tables.Annotations, error) {
	s := &annotationsStatements{}
	return s, sqlutil.StatementList{
		{&s.insertAnnotationStmt, insertAnnotationSQL},
		{&s.selectAnnotationExistsStmt, selectAnnotationExistsSQL},
		{&s.deleteAnnotationStmt, deleteAnnotationSQL},
	}.Prepare(db)
}

func (s *annotationsStatements) InsertAnnotation(
	ctx context.Context, txn *sql.Tx, roomNID types.RoomNID, annotationEventID, targetEventID, senderID, eventType, key string,
) error {
	_, err := sqlutil.TxStmt(txn, s.insertAnnotationStmt).ExecContext(
		ctx, annotationEventID, roomNID, targetEventID, senderID, eventType, key,
	)
	return err
}

func (s *annotationsStatements) SelectAnnotationExists(
	ctx context.Context, txn *sql.Tx, roomNID types.RoomNID, targetEventID, senderID, eventType, key, excludeEventID string,
) (bool, error) {
	var one int
	err := sqlutil.TxStmt(txn, s.selectAnnotationExistsStmt).QueryRowContext(
		ctx, roomNID, targetEventID, senderID, eventType, key, excludeEventID,
	).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *annotationsStatements) DeleteAnnotation(ctx context.Context, txn *sql.Tx, annotationEventID string) error {
	_, err := sqlutil.TxStmt(txn, s.deleteAnnotationStmt).ExecContext(ctx, annotationEventID)
	return err
}
