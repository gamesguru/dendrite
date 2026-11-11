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
	lastID := int64(-1)
	lastOrdinal := int64(0)
	for {
		// Page expanded state additions rather than output events. One rewrite
		// can add an arbitrarily large room state, so limiting output events
		// would not bound memory.
		rows, err := tx.QueryContext(ctx, `
			SELECT output_event.id, added.ordinal, output_event.room_id,
			       member_event.event_id, member_event.headered_event_json
			FROM syncapi_output_room_events AS output_event
			CROSS JOIN LATERAL unnest(output_event.add_state_ids)
			    WITH ORDINALITY AS added(state_event_id, ordinal)
			JOIN syncapi_output_room_events AS member_event
			  ON member_event.event_id = added.state_event_id
			 AND member_event.type = 'm.room.member'
			WHERE (output_event.id, added.ordinal) > ($1, $2)
			ORDER BY output_event.id, added.ordinal
			LIMIT $3
		`, lastID, lastOrdinal, backfillBatchSize)
		if err != nil {
			return err
		}
		var positions []int64
		var rooms, users, memberships, eventIDs []string
		scanned := 0
		for rows.Next() {
			var position, ordinal int64
			var roomID, eventID, raw string
			if err = rows.Scan(&position, &ordinal, &roomID, &eventID, &raw); err != nil {
				_ = rows.Close()
				return err
			}
			scanned++
			lastID, lastOrdinal = position, ordinal
			var event struct {
				StateKey *string `json:"state_key"`
				Content  struct {
					Membership *string `json:"membership"`
				} `json:"content"`
			}
			if json.Unmarshal([]byte(raw), &event) != nil || event.StateKey == nil || event.Content.Membership == nil {
				continue
			}
			positions = append(positions, position)
			rooms = append(rooms, roomID)
			users = append(users, *event.StateKey)
			memberships = append(memberships, *event.Content.Membership)
			eventIDs = append(eventIDs, eventID)
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
		// Entries for one output event can span pages. Updating conflicts in
		// ordinal order keeps the same last-added event as the live writer.
		// The resolver cache lives for one page only, so memory stays bounded
		// by the rooms that have pseudo keys on this page.
		if err = resolvePseudoStateKeys(ctx, tx, make(map[string]map[string]string), rooms, users); err != nil {
			return err
		}
		if len(positions) == 0 {
			continue
		}
		type historyKey struct {
			roomID, userID string
			position       int64
		}
		lastForKey := make(map[historyKey]int, len(positions))
		keys := make([]historyKey, len(positions))
		for i := range positions {
			keys[i] = historyKey{rooms[i], users[i], positions[i]}
			lastForKey[keys[i]] = i
		}
		uniquePositions := make([]int64, 0, len(lastForKey))
		uniqueRooms := make([]string, 0, len(lastForKey))
		uniqueUsers := make([]string, 0, len(lastForKey))
		uniqueMemberships := make([]string, 0, len(lastForKey))
		uniqueEventIDs := make([]string, 0, len(lastForKey))
		for i := range positions {
			if lastForKey[keys[i]] != i {
				continue
			}
			uniquePositions = append(uniquePositions, positions[i])
			uniqueRooms = append(uniqueRooms, rooms[i])
			uniqueUsers = append(uniqueUsers, users[i])
			uniqueMemberships = append(uniqueMemberships, memberships[i])
			uniqueEventIDs = append(uniqueEventIDs, eventIDs[i])
		}
		if _, err = tx.ExecContext(ctx, `
INSERT INTO syncapi_membership_history(room_id, user_id, membership, event_id, stream_pos)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[])
			ON CONFLICT (room_id, user_id, stream_pos) DO UPDATE
			SET membership = EXCLUDED.membership, event_id = EXCLUDED.event_id
		`, uniqueRooms, uniqueUsers, uniqueMemberships, uniqueEventIDs, uniquePositions); err != nil {
			return err
		}
	}
}

// This helper replaces pseudo-ID state keys with the user IDs the live path
// stores. It uses syncapi_current_room_state, which keeps the resolved state
// key, together with the event JSON that keeps the raw one, so no roomserver
// call is needed from a schema delta. A key without a current-state row is
// left as it is. Each room's mapping is loaded once, on first use, into
// resolvers, which the caller scopes to a single page.
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
