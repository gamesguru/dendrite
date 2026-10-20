// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal"
)

// annotationBackfillBatchSize is how many reactions are read at a time.
const annotationBackfillBatchSize = 1000

type backfillAnnotation struct {
	eventNID int64
	eventID  string
	roomNID  int64
	json     []byte
}

// UpBackfillAnnotations fills roomserver_annotations with the m.reaction
// events stored before it existed, so that reactions sent before the upgrade
// are also not sent twice. Other event types may carry annotations as well,
// but parsing every stored event would make the upgrade slow on large
// servers. The JSON is parsed here rather than with jsonb, which rejects
// some strings that event JSON may contain. Redacted reactions have had
// their content removed, so they are left out. Annotations containing a NUL
// character are left out on both databases, as they are when sent.
func UpBackfillAnnotations(ctx context.Context, tx *sql.Tx) error {
	var reactionTypeNID int64
	err := tx.QueryRowContext(ctx, "SELECT event_type_nid FROM roomserver_event_types WHERE event_type = 'm.reaction'").Scan(&reactionTypeNID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to select the m.reaction event type: %w", err)
	}
	var after int64
	for {
		batch, err := selectBackfillAnnotations(ctx, tx, reactionTypeNID, after)
		if err != nil {
			return err
		}
		for _, ev := range batch {
			relation := gjson.GetBytes(ev.json, `content.m\.relates_to`)
			targetID, key := relation.Get("event_id"), relation.Get("key")
			if relation.Get("rel_type").Str != "m.annotation" || targetID.Type != gjson.String || key.Type != gjson.String {
				continue
			}
			// PostgreSQL cannot store a NUL character in text.
			if strings.ContainsRune(targetID.Str, 0) || strings.ContainsRune(key.Str, 0) {
				continue
			}
			if _, err = tx.ExecContext(ctx, ""+
				"INSERT INTO roomserver_annotations (annotation_event_id, room_nid, target_event_id, sender_id, event_type, annotation_key)"+
				" VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
				ev.eventID, ev.roomNID, targetID.Str, gjson.GetBytes(ev.json, "sender").Str, "m.reaction", key.Str,
			); err != nil {
				return fmt.Errorf("failed to insert annotation %s: %w", ev.eventID, err)
			}
		}
		if len(batch) < annotationBackfillBatchSize {
			return nil
		}
		after = batch[len(batch)-1].eventNID
	}
}

// selectBackfillAnnotations reads the next batch of accepted reactions. The
// rows are read in full before inserting, as the connection cannot run
// another statement while a result set is open.
func selectBackfillAnnotations(ctx context.Context, tx *sql.Tx, reactionTypeNID, after int64) ([]backfillAnnotation, error) {
	rows, err := tx.QueryContext(ctx, ""+
		"SELECT e.event_nid, e.event_id, e.room_nid, j.event_json FROM roomserver_events e"+
		" JOIN roomserver_event_json j ON j.event_nid = e.event_nid"+
		" WHERE e.event_type_nid = $1 AND e.is_rejected = $2 AND e.event_nid > $3"+
		" ORDER BY e.event_nid LIMIT $4",
		reactionTypeNID, false, after, annotationBackfillBatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to select reactions: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "failed to close rows")
	batch := make([]backfillAnnotation, 0, annotationBackfillBatchSize)
	for rows.Next() {
		var ev backfillAnnotation
		if err = rows.Scan(&ev.eventNID, &ev.eventID, &ev.roomNID, &ev.json); err != nil {
			return nil, fmt.Errorf("failed to scan reaction: %w", err)
		}
		batch = append(batch, ev)
	}
	return batch, rows.Err()
}
