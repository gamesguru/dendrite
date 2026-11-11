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
	lastID := int64(-1)
	for {
		// Keyset pages keep memory bounded: the rows of a page are read and
		// closed before the next statement runs on the transaction.
		rows, err := tx.QueryContext(ctx, `
SELECT id, room_id, event_id, headered_event_json
FROM syncapi_output_room_events
WHERE type = 'm.room.member' AND event_id = ANY(add_state_ids) AND id > $1
ORDER BY id
LIMIT $2
`, lastID, backfillBatchSize)
		if err != nil {
			return err
		}
		var pos []int64
		var rooms, users, states, events []string
		scanned := 0
		for rows.Next() {
			var p int64
			var roomID, eventID, raw string
			if err = rows.Scan(&p, &roomID, &eventID, &raw); err != nil {
				_ = rows.Close()
				return err
			}
			scanned++
			lastID = p
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
		if scanned == 0 {
			return nil
		}
		// Keep the pseudo-ID mapping cache scoped to this batch so memory stays
		// bounded as the migration advances through rooms.
		resolvers := make(map[string]map[string]string)
		if err = resolvePseudoStateKeys(ctx, tx, resolvers, rooms, users); err != nil {
			return err
		}
		if len(pos) == 0 {
			continue
		}
		if _, err = tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[])
ON CONFLICT (room_id, user_id, stream_pos) DO NOTHING
`, rooms, users, states, events, pos); err != nil {
			return err
		}
	}
}

// This helper replaces pseudo-ID state keys with the user IDs the live path
// stores. It uses syncapi_current_room_state, which keeps the resolved state
// key, together with the event JSON that keeps the raw one, so no roomserver
// call is needed from a schema delta. A key without a current-state row is
// left as it is. Each room's mapping is loaded once, on first use, into
// resolvers.
func resolvePseudoStateKeys(ctx context.Context, tx *sql.Tx, resolvers map[string]map[string]string, rooms, users []string) error {
	for i, user := range users {
		if isUserID(user) {
			continue
		}
		resolved, ok := resolvers[rooms[i]]
		if !ok {
			var err error
			if resolved, err = loadPseudoStateKeys(ctx, tx, rooms[i]); err != nil {
				return err
			}
			resolvers[rooms[i]] = resolved
		}
		if userID, found := resolved[user]; found {
			users[i] = userID
		}
	}
	return nil
}

// loadPseudoStateKeys maps each raw member state key in a room to the resolved
// state key of its current-state row.
func loadPseudoStateKeys(ctx context.Context, tx *sql.Tx, roomID string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT state_key, headered_event_json
FROM syncapi_current_room_state
WHERE room_id = $1 AND type = 'm.room.member'
`, roomID)
	if err != nil {
		return nil, fmt.Errorf("resolve pseudo state keys for room %s: %w", roomID, err)
	}
	resolved := make(map[string]string)
	for rows.Next() {
		var resolvedKey, raw string
		if err = rows.Scan(&resolvedKey, &raw); err != nil {
			_ = rows.Close()
			return nil, err
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
		return nil, err
	}
	return resolved, rows.Close()
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
