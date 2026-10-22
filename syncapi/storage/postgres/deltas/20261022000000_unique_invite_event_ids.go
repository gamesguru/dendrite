// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
)

// UpUniqueInviteEventIDs gives every invite event at most one row. A previous
// version could insert the same invite more than once when it was redelivered,
// and retiring those duplicates then collided on the primary key. Keep the
// newest row for each event ID, then enforce it in the database so concurrent
// consumers cannot race to insert duplicates.
func UpUniqueInviteEventIDs(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM syncapi_invite_events a
		USING syncapi_invite_events b
		WHERE a.event_id = b.event_id AND a.id < b.id`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS syncapi_invites_event_id_unique_idx
		ON syncapi_invite_events (event_id)`)
	return err
}
