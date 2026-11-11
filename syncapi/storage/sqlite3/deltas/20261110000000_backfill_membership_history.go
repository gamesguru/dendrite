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
// compact membership table before history was introduced. Only events the
// live path records (those in their own add_state_ids) are backfilled; old
// backfilled events never reach the history table. Pseudo user IDs (MSC4014)
// are resolved through syncapi_current_room_state, whose state_key is already
// resolved by the live path; a key with no current-state row keeps its raw
// value as a fallback.
func UpBackfillMembershipHistory(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT o.room_id,
       CASE
         WHEN json_extract(o.headered_event_json, '$.state_key') GLOB '@*:*'
           THEN json_extract(o.headered_event_json, '$.state_key')
         ELSE COALESCE(
           (SELECT cs.state_key
            FROM syncapi_current_room_state cs
            WHERE cs.room_id = o.room_id
              AND cs.type = 'm.room.member'
              AND json_extract(cs.headered_event_json, '$.state_key') =
                  json_extract(o.headered_event_json, '$.state_key')
            LIMIT 1),
           json_extract(o.headered_event_json, '$.state_key'))
       END,
       json_extract(o.headered_event_json, '$.content.membership'),
       o.event_id,
       o.id
FROM syncapi_output_room_events o
WHERE o.type = 'm.room.member'
  AND EXISTS (SELECT 1 FROM json_each(o.add_state_ids) WHERE value = o.event_id)
  AND json_extract(o.headered_event_json, '$.state_key') IS NOT NULL
  AND json_extract(o.headered_event_json, '$.content.membership') IS NOT NULL
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING
`)
	return err
}
