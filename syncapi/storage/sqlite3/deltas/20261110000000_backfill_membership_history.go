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
// compact membership table before history was introduced. Like the live
// writer, it records every membership event that an output event adds to room
// state (its add_state_ids) at that output event's position, which includes
// older events restored by a later one. Old backfilled events add nothing and
// never reach the history table. Pseudo user IDs (MSC4014)
// are resolved through syncapi_current_room_state, whose state_key is already
// resolved by the live path; a key with no current-state row keeps its raw
// value as a fallback.
func UpBackfillMembershipHistory(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT o.room_id,
       CASE
         WHEN json_extract(m.headered_event_json, '$.state_key') GLOB '@*:*'
           THEN json_extract(m.headered_event_json, '$.state_key')
         ELSE COALESCE(
           (SELECT cs.state_key
            FROM syncapi_current_room_state cs
            WHERE cs.room_id = o.room_id
              AND cs.type = 'm.room.member'
              AND json_extract(cs.headered_event_json, '$.state_key') =
                  json_extract(m.headered_event_json, '$.state_key')
            LIMIT 1),
           json_extract(m.headered_event_json, '$.state_key'))
       END,
       json_extract(m.headered_event_json, '$.content.membership'),
       m.event_id,
       o.id
FROM syncapi_output_room_events o
-- InsertEvent stores an empty list as '', which json_each rejects.
JOIN json_each(CASE WHEN json_valid(o.add_state_ids) THEN o.add_state_ids ELSE '[]' END) a
JOIN syncapi_output_room_events m ON m.event_id = a.value AND m.type = 'm.room.member'
WHERE json_extract(m.headered_event_json, '$.state_key') IS NOT NULL
  AND json_extract(m.headered_event_json, '$.content.membership') IS NOT NULL
-- The live writer keeps the last added event per user at a position.
ORDER BY o.id, a.key DESC
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING
`)
	return err
}
