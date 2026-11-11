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

	"codefloe.com/pat-s/zendrite/internal/sqlutil"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

const backfillBatchSize = 1000

// UpBackfillMembershipHistory rebuilds position-bounded membership history
// from retained output events, including transitions overwritten in the
// compact membership table before history was introduced.
//
// Like the live writer, it records every membership event that an output event
// adds to room state (its add_state_ids) at that output event's position,
// which includes older events restored by a later one. Old (backfilled)
// events carry empty lists and never reach the history table. The event JSON is parsed here rather than cast to
// jsonb in SQL, because jsonb rejects \u0000 and lone-surrogate escapes that
// stored event content may contain, and one such event must not block startup.
//
// Rows are keyed by the user ID the live path stores. Pseudo user IDs
// (MSC4014) are resolved through syncapi_current_room_state, whose state_key
// is already resolved by the live path; a key with no current-state row keeps
// its raw value as a fallback.
func UpBackfillMembershipHistory(ctx context.Context, tx *sql.Tx) error {
	// Resolved pseudo-ID maps are loaded once per room and kept across batches.
	resolvers := make(map[string]map[string]string)
	lastID := int64(-1)
	for {
		// Keyset pages keep memory bounded: the rows of a page are read and
		// closed before the next statement runs on the transaction.
		rows, err := tx.QueryContext(ctx, `
SELECT id, room_id, add_state_ids
FROM syncapi_output_room_events
WHERE cardinality(add_state_ids) > 0 AND id > $1
ORDER BY id
LIMIT $2
`, lastID, backfillBatchSize)
		if err != nil {
			return err
		}
		type added struct {
			pos    int64
			roomID string
			ids    []string
		}
		var page []added
		var wanted []string
		for rows.Next() {
			var a added
			var ids sqlutil.StringArray
			if err = rows.Scan(&a.pos, &a.roomID, &ids); err != nil {
				_ = rows.Close()
				return err
			}
			a.ids = ids
			lastID = a.pos
			page = append(page, a)
			wanted = append(wanted, ids...)
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}

		// The added state events are stored as output events of their own,
		// possibly at an earlier position than the event that added them back.
		bodies, err := loadMemberBodies(ctx, tx, wanted)
		if err != nil {
			return err
		}
		var pos []int64
		var rooms, users, states, events []string
		for _, a := range page {
			// The live writer keeps the last added event per user at a position.
			latest := make(map[string]string)
			var order []string
			for _, id := range a.ids {
				m, ok := bodies[id]
				if !ok {
					continue
				}
				if _, seen := latest[m.user]; !seen {
					order = append(order, m.user)
				}
				latest[m.user] = id
			}
			for _, user := range order {
				id := latest[user]
				pos = append(pos, a.pos)
				rooms = append(rooms, a.roomID)
				users = append(users, user)
				states = append(states, bodies[id].membership)
				events = append(events, id)
			}
		}
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

type memberBody struct{ user, membership string }

// loadMemberBodies returns the state key and membership of each retained
// membership event among eventIDs. The JSON is parsed here rather than cast to
// jsonb in SQL, because jsonb rejects \u0000 and lone-surrogate escapes that
// stored event content may contain, and one such event must not block startup.
func loadMemberBodies(ctx context.Context, tx *sql.Tx, eventIDs []string) (map[string]memberBody, error) {
	out := make(map[string]memberBody, len(eventIDs))
	rows, err := tx.QueryContext(ctx, `
SELECT event_id, headered_event_json
FROM syncapi_output_room_events
WHERE type = 'm.room.member' AND event_id = ANY($1)
`, sqlutil.StringArray(eventIDs))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var eventID, raw string
		if err = rows.Scan(&eventID, &raw); err != nil {
			_ = rows.Close()
			return nil, err
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
		out[eventID] = memberBody{*ev.StateKey, *ev.Content.Membership}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return out, rows.Close()
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
