// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
)

// UpBackfillMembershipHistory rebuilds position-bounded membership history
// from retained output events, including transitions overwritten in the
// compact membership table before history was introduced.
func UpBackfillMembershipHistory(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT room_id,
       json_extract(headered_event_json, '$.state_key'),
       json_extract(headered_event_json, '$.content.membership'),
       event_id,
       id
FROM syncapi_output_room_events
WHERE type = 'm.room.member'
  AND json_extract(headered_event_json, '$.state_key') IS NOT NULL
  AND json_extract(headered_event_json, '$.content.membership') IS NOT NULL
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING
`)
	return err
}
