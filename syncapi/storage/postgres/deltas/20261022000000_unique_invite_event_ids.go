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
// and retiring those duplicates then collided on the primary key. Prefer a
// retired row over an active duplicate, then keep the newest row of the same
// status. Enforce uniqueness so concurrent consumers cannot add duplicates.
func UpUniqueInviteEventIDs(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM syncapi_invite_events AS old
		USING syncapi_invite_events AS preferred
		WHERE old.event_id = preferred.event_id
		  AND ((preferred.deleted AND NOT old.deleted)
		       OR (preferred.deleted = old.deleted AND preferred.id > old.id))`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS syncapi_invites_event_id_unique_idx
		ON syncapi_invite_events (event_id)`); err != nil {
		return err
	}
	// The unique index serves every lookup by event ID.
	_, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS syncapi_invites_event_id_idx`)
	return err
}
