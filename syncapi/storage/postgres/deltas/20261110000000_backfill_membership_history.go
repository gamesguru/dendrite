// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

const backfillBatchSize = 1000

// UpBackfillMembershipHistory rebuilds position-bounded membership history
// from retained output events, including transitions overwritten in the
// compact membership table before history was introduced.
//
// Only events that the live path records are backfilled: those listed in their
// own add_state_ids. Old (backfilled) events carry empty lists and never
// reach the history table. The event JSON is parsed here rather than cast to
// jsonb in SQL, because jsonb rejects \u0000 and lone-surrogate escapes that
// stored event content may contain, and one such event must not block startup.
//
// Rows are keyed by the user ID the live path stores. Pseudo user IDs
// (MSC4014) are resolved through syncapi_current_room_state, whose state_key
// is already resolved by the live path; a key with no current-state row keeps
// its raw value as a fallback.
func UpBackfillMembershipHistory(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
SELECT id, room_id, event_id, headered_event_json
FROM syncapi_output_room_events
WHERE type = 'm.room.member' AND event_id = ANY(add_state_ids)
ORDER BY id
`)
	if err != nil {
		return err
	}
	var pos []int64
	var rooms, users, states, events []string
	for rows.Next() {
		var p int64
		var roomID, eventID, raw string
		if err = rows.Scan(&p, &roomID, &eventID, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		var ev struct {
			StateKey *string `json:"state_key"`
			Content  struct {
				Membership *string `json:"membership"`
			} `json:"content"`
		}
		if json.Unmarshal([]byte(raw), &ev) != nil || ev.StateKey == nil || ev.Content.Membership == nil {
			continue
		}
		pos = append(pos, p)
		rooms = append(rooms, roomID)
		users = append(users, *ev.StateKey)
		states = append(states, *ev.Content.Membership)
		events = append(events, eventID)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if err = resolvePseudoStateKeys(ctx, tx, rooms, users); err != nil {
		return err
	}
	for start := 0; start < len(pos); start += backfillBatchSize {
		end := min(start+backfillBatchSize, len(pos))
		if _, err = tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[])
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING
`, rooms[start:end], users[start:end], states[start:end], events[start:end], pos[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// This helper replaces pseudo-ID state keys with the user IDs the live path
// stores. It uses syncapi_current_room_state, which keeps the resolved state
// key, together with the event JSON that keeps the raw one, so no roomserver
// call is needed from a schema delta. A key without a current-state row is
// left as it is.
func resolvePseudoStateKeys(ctx context.Context, tx *sql.Tx, rooms, users []string) error {
	pseudoRooms := make(map[string]struct{})
	for i, user := range users {
		if !isUserID(user) {
			pseudoRooms[rooms[i]] = struct{}{}
		}
	}
	for roomID := range pseudoRooms {
		rows, err := tx.QueryContext(ctx, `
SELECT state_key, headered_event_json
FROM syncapi_current_room_state
WHERE room_id = $1 AND type = 'm.room.member'
`, roomID)
		if err != nil {
			return fmt.Errorf("resolve pseudo state keys for room %s: %w", roomID, err)
		}
		resolved := make(map[string]string)
		for rows.Next() {
			var resolvedKey, raw string
			if err = rows.Scan(&resolvedKey, &raw); err != nil {
				_ = rows.Close()
				return err
			}
			var ev struct {
				StateKey *string `json:"state_key"`
			}
			if json.Unmarshal([]byte(raw), &ev) == nil && ev.StateKey != nil {
				resolved[*ev.StateKey] = resolvedKey
			}
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err = rows.Close(); err != nil {
			return err
		}
		for i := range users {
			if rooms[i] != roomID || isUserID(users[i]) {
				continue
			}
			if userID, ok := resolved[users[i]]; ok {
				users[i] = userID
			}
		}
	}
	return nil
}

// isUserID reports whether stateKey can be used as a user ID without the
// roomserver's pseudo-ID mapping.
func isUserID(stateKey string) bool {
	if !strings.HasPrefix(stateKey, "@") || !strings.Contains(stateKey, ":") {
		return false
	}
	_, err := spec.NewUserID(stateKey, true)
	return err == nil
}
