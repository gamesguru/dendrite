// Copyright 2024 New Vector Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package shared

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// BackfillStateHAMTs builds persisted HAMTs for snapshots which predate
// snapshot-time HAMT writes. A non-positive limit selects a conservative batch
// size so callers can repeat this operation until it returns zero.
func (d *Database) BackfillStateHAMTs(ctx context.Context, limit int) (int, error) {
	if !statehamt.Enabled {
		return 0, statehamt.ErrDisabled
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := d.DB.QueryContext(ctx, `
		SELECT s.state_snapshot_nid, s.room_nid
		FROM roomserver_state_snapshots AS s
		LEFT JOIN roomserver_state_hamt_snapshots AS h
		  ON h.state_snapshot_nid = s.state_snapshot_nid
		WHERE h.state_snapshot_nid IS NULL
		ORDER BY s.state_snapshot_nid
		LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("select snapshots without state HAMTs: %w", err)
	}
	closeRows := func(cause error) error {
		if closeErr := rows.Close(); closeErr != nil {
			return fmt.Errorf("%w (also failed to close snapshot rows: %v)", cause, closeErr)
		}
		return cause
	}
	type snapshot struct {
		nid     types.StateSnapshotNID
		roomNID types.RoomNID
	}
	var snapshots []snapshot
	for rows.Next() {
		var item snapshot
		if err = rows.Scan(&item.nid, &item.roomNID); err != nil {
			return 0, closeRows(fmt.Errorf("scan snapshot without state HAMT: %w", err))
		}
		snapshots = append(snapshots, item)
	}
	if err = rows.Err(); err != nil {
		return 0, closeRows(fmt.Errorf("iterate snapshots without state HAMTs: %w", err))
	}
	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("close snapshots without state HAMTs: %w", err)
	}

	completed := 0
	for _, item := range snapshots {
		built := false
		err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
			// Another backfill process or a live writer may have filled this
			// snapshot after the batch query.
			existing, err := d.StateHAMTTable.SelectStateHAMT(ctx, txn, item.nid)
			if err != nil {
				return fmt.Errorf("check state HAMT for snapshot %d: %w", item.nid, err)
			}
			if existing != nil {
				return nil
			}
			blockLists, err := d.StateSnapshotTable.BulkSelectStateBlockNIDs(ctx, txn, []types.StateSnapshotNID{item.nid})
			if err != nil {
				return fmt.Errorf("load state blocks for snapshot %d: %w", item.nid, err)
			}
			if len(blockLists) != 1 || blockLists[0].StateSnapshotNID != item.nid {
				return fmt.Errorf("state blocks missing for snapshot %d", item.nid)
			}
			if err := d.persistStateHAMT(ctx, txn, item.roomNID, item.nid, blockLists[0].StateBlockNIDs); err != nil {
				return fmt.Errorf("persist HAMT for snapshot %d: %w", item.nid, err)
			}
			built = true
			return nil
		})
		if err != nil {
			return completed, err
		}
		if built {
			completed++
		}
	}
	return completed, nil
}

// StateHAMTCoverage reports how many state snapshots exist and how many of
// them have a persisted HAMT. The migration to HAMT-backed snapshots is
// complete when the two counts are equal: only then can reads stop falling
// back to state blocks.
func (d *Database) StateHAMTCoverage(ctx context.Context) (total, withHAMT int64, err error) {
	err = d.DB.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(h.state_snapshot_nid)
		FROM roomserver_state_snapshots AS s
		LEFT JOIN roomserver_state_hamt_snapshots AS h
		  ON h.state_snapshot_nid = s.state_snapshot_nid`).Scan(&total, &withHAMT)
	if err != nil {
		return 0, 0, fmt.Errorf("count state HAMT coverage: %w", err)
	}
	return total, withHAMT, nil
}

// purgeStateHAMTs removes a room's roots and, when the Rezzy reader is
// available, sweeps node records not reachable from any remaining root. Node
// records are content-addressed and shared, so the sweep must trace all roots.
func (d *Database) purgeStateHAMTs(ctx context.Context, txn *sql.Tx, roomNID types.RoomNID) error {
	if err := d.StateHAMTTable.DeleteStateHAMTRoom(ctx, txn, roomNID); err != nil {
		return fmt.Errorf("delete state HAMT roots for room %d: %w", roomNID, err)
	}
	if !statehamt.Enabled {
		// Root rows are gone, so the room is purged correctly. A non-Rezzy
		// binary cannot safely trace shared node references; leave node cleanup
		// for a Rezzy-enabled run rather than risk deleting live data.
		return nil
	}

	roots, err := d.StateHAMTTable.SelectStateHAMTRootRefs(ctx, txn)
	if err != nil {
		return fmt.Errorf("select remaining state HAMT roots: %w", err)
	}
	live := make(map[string]struct{})
	for _, root := range roots {
		nodes := make([]statehamt.Node, 0, 16)
		seen := make(map[string]struct{})
		complete := false
		for round := 0; round < 64; round++ {
			needed, err := statehamt.Needed(root.RoomID, root.RootHash, nodes)
			if err != nil {
				return fmt.Errorf("trace state HAMT rooted at %x for room %s: %w", root.RootHash, root.RoomID, err)
			}
			if len(needed) == 0 {
				complete = true
				break
			}
			fetched, err := d.StateHAMTTable.SelectStateHAMTNodes(ctx, txn, needed)
			if err != nil {
				return fmt.Errorf("load state HAMT nodes during purge: %w", err)
			}
			if len(fetched) == 0 {
				return fmt.Errorf("state HAMT for room %s has missing nodes during purge", root.RoomID)
			}
			for _, node := range fetched {
				key := string(node.Hash)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				nodes = append(nodes, statehamt.Node{Hash: node.Hash, Bytes: node.Bytes})
			}
		}
		if !complete {
			return fmt.Errorf("state HAMT for room %s exceeded the traversal limit during purge", root.RoomID)
		}
		for _, node := range nodes {
			live[string(node.Hash)] = struct{}{}
		}
	}

	allHashes, err := d.StateHAMTTable.SelectStateHAMTNodeHashes(ctx, txn)
	if err != nil {
		return fmt.Errorf("select state HAMT node hashes during purge: %w", err)
	}
	var garbage [][]byte
	for _, hash := range allHashes {
		if _, ok := live[string(hash)]; !ok {
			garbage = append(garbage, hash)
		}
	}
	if err := d.StateHAMTTable.DeleteStateHAMTNodes(ctx, txn, garbage); err != nil {
		return fmt.Errorf("delete unreachable state HAMT nodes: %w", err)
	}
	return nil
}

// persistStateHAMT builds the HAMT for a just-created snapshot and stores its
// nodes and root record. It is a no-op when the build does not link rezzy.
func (d *Database) persistStateHAMT(
	ctx context.Context,
	txn *sql.Tx,
	roomNID types.RoomNID,
	snapshotNID types.StateSnapshotNID,
	stateBlockNIDs []types.StateBlockNID,
) error {
	if !statehamt.Enabled {
		return nil
	}
	blocks, err := d.StateBlockTable.BulkSelectStateBlockEntries(ctx, txn, stateBlockNIDs)
	if err != nil {
		return fmt.Errorf("BulkSelectStateBlockEntries: %w", err)
	}
	var eventNIDs []types.EventNID
	for _, block := range blocks {
		eventNIDs = append(eventNIDs, block...)
	}
	entriesByNID := make(map[types.EventNID]types.StateEntry, len(eventNIDs))
	if len(eventNIDs) > 0 {
		entries, err := d.EventsTable.BulkSelectStateEventByNID(ctx, txn, eventNIDs, nil)
		if err != nil {
			return fmt.Errorf("BulkSelectStateEventByNID: %w", err)
		}
		for _, entry := range entries {
			entriesByNID[entry.EventNID] = entry
		}
	}
	// Later blocks override earlier ones for the same state key, matching
	// LoadStateAtSnapshot's stable sort followed by last-wins deduplication.
	ordered := make(map[types.StateKeyTuple]types.StateEntry, len(eventNIDs))
	for _, block := range blocks {
		for _, eventNID := range block {
			entry, ok := entriesByNID[eventNID]
			if !ok {
				return fmt.Errorf("state block references unknown event NID %d", eventNID)
			}
			ordered[entry.StateKeyTuple] = entry
		}
	}
	stateEntries := make([]types.StateEntry, 0, len(ordered))
	for _, entry := range ordered {
		stateEntries = append(stateEntries, entry)
	}
	hamtEntries, err := d.hamtEntriesFromStateEntries(ctx, txn, stateEntries)
	if err != nil {
		return err
	}
	roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{roomNID})
	if err != nil {
		return fmt.Errorf("BulkSelectRoomIDs: %w", err)
	}
	if len(roomIDs) == 0 {
		return fmt.Errorf("room NID %d has no room ID", roomNID)
	}
	persisted, err := statehamt.Build(roomIDs[0], hamtEntries)
	if err != nil {
		return fmt.Errorf("statehamt.Build: %w", err)
	}
	nodes := make([]tables.StateHAMTNode, len(persisted.Nodes))
	for i, node := range persisted.Nodes {
		nodes[i] = tables.StateHAMTNode{Hash: node.Hash, Bytes: node.Bytes}
	}
	if err := d.StateHAMTTable.InsertStateHAMTNodes(ctx, txn, nodes); err != nil {
		return fmt.Errorf("InsertStateHAMTNodes: %w", err)
	}
	if err := d.StateHAMTTable.InsertStateHAMT(
		ctx, txn, snapshotNID, roomNID,
		persisted.RootHash, persisted.StateGroupID, persisted.RootRecord, persisted.EntryCount,
	); err != nil {
		return fmt.Errorf("InsertStateHAMT: %w", err)
	}
	return nil
}

func (d *Database) hamtEntriesFromStateEntries(
	ctx context.Context, txn *sql.Tx, entries []types.StateEntry,
) ([]statehamt.Entry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	eventNIDList := make([]types.EventNID, 0, len(entries))
	typeNIDList := make([]types.EventTypeNID, 0, len(entries))
	stateKeyNIDList := make([]types.EventStateKeyNID, 0, len(entries))
	seenEvent := make(map[types.EventNID]struct{}, len(entries))
	seenType := make(map[types.EventTypeNID]struct{}, len(entries))
	seenStateKey := make(map[types.EventStateKeyNID]struct{}, len(entries))
	for _, entry := range entries {
		if _, ok := seenEvent[entry.EventNID]; !ok {
			seenEvent[entry.EventNID] = struct{}{}
			eventNIDList = append(eventNIDList, entry.EventNID)
		}
		if _, ok := seenType[entry.EventTypeNID]; !ok {
			seenType[entry.EventTypeNID] = struct{}{}
			typeNIDList = append(typeNIDList, entry.EventTypeNID)
		}
		if _, ok := seenStateKey[entry.EventStateKeyNID]; !ok {
			seenStateKey[entry.EventStateKeyNID] = struct{}{}
			stateKeyNIDList = append(stateKeyNIDList, entry.EventStateKeyNID)
		}
	}
	eventIDs, err := d.EventsTable.BulkSelectEventID(ctx, txn, eventNIDList)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventID: %w", err)
	}
	eventTypes, err := d.EventTypesTable.BulkSelectEventType(ctx, txn, typeNIDList)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventType: %w", err)
	}
	stateKeys, err := d.EventStateKeysTable.BulkSelectEventStateKey(ctx, txn, stateKeyNIDList)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventStateKey: %w", err)
	}
	result := make([]statehamt.Entry, 0, len(entries))
	for _, entry := range entries {
		eventID, ok := eventIDs[entry.EventNID]
		if !ok {
			return nil, fmt.Errorf("event NID %d has no event ID", entry.EventNID)
		}
		eventType, ok := eventTypes[entry.EventTypeNID]
		if !ok {
			return nil, fmt.Errorf("event type NID %d has no string", entry.EventTypeNID)
		}
		stateKey, ok := stateKeys[entry.EventStateKeyNID]
		if !ok {
			return nil, fmt.Errorf("state key NID %d has no string", entry.EventStateKeyNID)
		}
		result = append(result, statehamt.Entry{Type: eventType, StateKey: stateKey, EventID: eventID})
	}
	return result, nil
}

func (d *Database) stateEntriesFromHAMT(
	ctx context.Context, txn *sql.Tx, entries []statehamt.Entry,
) ([]types.StateEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	eventIDs := make([]string, 0, len(entries))
	eventTypes := make([]string, 0, len(entries))
	stateKeys := make([]string, 0, len(entries))
	seenEvent := make(map[string]struct{}, len(entries))
	seenType := make(map[string]struct{}, len(entries))
	seenStateKey := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if _, ok := seenEvent[entry.EventID]; !ok {
			seenEvent[entry.EventID] = struct{}{}
			eventIDs = append(eventIDs, entry.EventID)
		}
		if _, ok := seenType[entry.Type]; !ok {
			seenType[entry.Type] = struct{}{}
			eventTypes = append(eventTypes, entry.Type)
		}
		if _, ok := seenStateKey[entry.StateKey]; !ok {
			seenStateKey[entry.StateKey] = struct{}{}
			stateKeys = append(stateKeys, entry.StateKey)
		}
	}
	eventNIDs, err := d.EventsTable.BulkSelectEventNID(ctx, txn, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventNID: %w", err)
	}
	typeNIDs, err := d.EventTypesTable.BulkSelectEventTypeNID(ctx, txn, eventTypes)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventTypeNID: %w", err)
	}
	stateKeyNIDs, err := d.EventStateKeysTable.BulkSelectEventStateKeyNID(ctx, txn, stateKeys)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectEventStateKeyNID: %w", err)
	}
	result := make([]types.StateEntry, 0, len(entries))
	for _, entry := range entries {
		event, ok := eventNIDs[entry.EventID]
		if !ok {
			return nil, fmt.Errorf("event ID %q has no NID", entry.EventID)
		}
		typeNID, ok := typeNIDs[entry.Type]
		if !ok {
			return nil, fmt.Errorf("event type %q has no NID", entry.Type)
		}
		stateKeyNID, ok := stateKeyNIDs[entry.StateKey]
		if !ok {
			return nil, fmt.Errorf("state key %q has no NID", entry.StateKey)
		}
		result = append(result, types.StateEntry{
			StateKeyTuple: types.StateKeyTuple{
				EventTypeNID:     typeNID,
				EventStateKeyNID: stateKeyNID,
			},
			EventNID: event.EventNID,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].EventTypeNID != result[j].EventTypeNID {
			return result[i].EventTypeNID < result[j].EventTypeNID
		}
		return result[i].EventStateKeyNID < result[j].EventStateKeyNID
	})
	return result, nil
}

// StateHAMTEntries returns the full state at a snapshot from its persisted
// HAMT. The boolean is false when the snapshot predates HAMT persistence, in
// which case the caller keeps using state blocks.
func (d *Database) StateHAMTEntries(
	ctx context.Context, snapshotNID types.StateSnapshotNID,
) ([]types.StateEntry, bool, error) {
	return d.stateHAMTEntries(ctx, nil, snapshotNID)
}

func (d *Database) stateHAMTEntries(
	ctx context.Context, txn *sql.Tx, snapshotNID types.StateSnapshotNID,
) ([]types.StateEntry, bool, error) {
	root, err := d.StateHAMTTable.SelectStateHAMT(ctx, txn, snapshotNID)
	if err != nil {
		return nil, false, fmt.Errorf("SelectStateHAMT: %w", err)
	}
	if root == nil {
		return nil, false, nil
	}
	roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{root.RoomNID})
	if err != nil {
		return nil, false, fmt.Errorf("BulkSelectRoomIDs: %w", err)
	}
	if len(roomIDs) == 0 {
		return nil, false, fmt.Errorf("room NID %d has no room ID", root.RoomNID)
	}
	roomID := roomIDs[0]
	nodes, err := d.loadStateHAMTNodes(ctx, txn, roomID, root, snapshotNID)
	if err != nil {
		return nil, false, err
	}
	entries, err := statehamt.Entries(roomID, root.RootHash, nodes)
	if err != nil {
		return nil, false, fmt.Errorf("statehamt.Entries: %w", err)
	}
	resolved, err := d.stateEntriesFromHAMT(ctx, txn, entries)
	if err != nil {
		return nil, false, err
	}
	return resolved, true, nil
}

func (d *Database) loadStateHAMTNodes(
	ctx context.Context, txn *sql.Tx, roomID string, root *tables.StateHAMTRoot,
	snapshotNID types.StateSnapshotNID,
) ([]statehamt.Node, error) {
	nodes := make([]statehamt.Node, 0, 8)
	seen := make(map[string]struct{}, 8)
	for round := 0; round < 16; round++ {
		needed, err := statehamt.Needed(roomID, root.RootHash, nodes)
		if err != nil {
			return nil, fmt.Errorf("statehamt.Needed: %w", err)
		}
		if len(needed) == 0 {
			return nodes, nil
		}
		fetched, err := d.StateHAMTTable.SelectStateHAMTNodes(ctx, txn, needed)
		if err != nil {
			return nil, fmt.Errorf("SelectStateHAMTNodes: %w", err)
		}
		if len(fetched) == 0 {
			return nil, fmt.Errorf("state HAMT for snapshot %d is missing nodes", snapshotNID)
		}
		for _, node := range fetched {
			key := string(node.Hash)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			nodes = append(nodes, statehamt.Node{Hash: node.Hash, Bytes: node.Bytes})
		}
	}
	return nil, fmt.Errorf("state HAMT for snapshot %d is missing nodes after 16 rounds", snapshotNID)
}

// StateHAMTEntriesForTuples returns the entries matching tuples at a snapshot.
// The boolean is false when the snapshot predates HAMT persistence.
func (d *Database) StateHAMTEntriesForTuples(
	ctx context.Context, snapshotNID types.StateSnapshotNID, tuples []types.StateKeyTuple,
) ([]types.StateEntry, bool, error) {
	return d.stateHAMTEntriesForTuples(ctx, nil, snapshotNID, tuples)
}

func (d *Database) stateHAMTEntriesForTuples(
	ctx context.Context, txn *sql.Tx, snapshotNID types.StateSnapshotNID, tuples []types.StateKeyTuple,
) ([]types.StateEntry, bool, error) {
	root, err := d.StateHAMTTable.SelectStateHAMT(ctx, txn, snapshotNID)
	if err != nil {
		return nil, false, fmt.Errorf("SelectStateHAMT: %w", err)
	}
	if root == nil {
		return nil, false, nil
	}
	roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{root.RoomNID})
	if err != nil {
		return nil, false, fmt.Errorf("BulkSelectRoomIDs: %w", err)
	}
	if len(roomIDs) == 0 {
		return nil, false, fmt.Errorf("room NID %d has no room ID", root.RoomNID)
	}
	roomID := roomIDs[0]
	typeNIDs := make([]types.EventTypeNID, 0, len(tuples))
	stateKeyNIDs := make([]types.EventStateKeyNID, 0, len(tuples))
	for _, tuple := range tuples {
		typeNIDs = append(typeNIDs, tuple.EventTypeNID)
		stateKeyNIDs = append(stateKeyNIDs, tuple.EventStateKeyNID)
	}
	typesByNID, err := d.EventTypesTable.BulkSelectEventType(ctx, txn, typeNIDs)
	if err != nil {
		return nil, false, fmt.Errorf("BulkSelectEventType: %w", err)
	}
	stateKeysByNID, err := d.EventStateKeysTable.BulkSelectEventStateKey(ctx, txn, stateKeyNIDs)
	if err != nil {
		return nil, false, fmt.Errorf("BulkSelectEventStateKey: %w", err)
	}
	keys := make([]statehamt.Key, 0, len(tuples))
	for _, tuple := range tuples {
		eventType, ok := typesByNID[tuple.EventTypeNID]
		if !ok {
			continue
		}
		stateKey, ok := stateKeysByNID[tuple.EventStateKeyNID]
		if !ok {
			continue
		}
		keys = append(keys, statehamt.Key{Type: eventType, StateKey: stateKey})
	}
	if len(keys) == 0 {
		return nil, true, nil
	}
	nodes, err := d.loadStateHAMTNodes(ctx, txn, roomID, root, snapshotNID)
	if err != nil {
		return nil, false, err
	}
	values, found, err := statehamt.Lookup(roomID, root.RootHash, nodes, keys)
	if err != nil {
		return nil, false, fmt.Errorf("statehamt.Lookup: %w", err)
	}
	entries := make([]statehamt.Entry, 0, len(keys))
	for i, key := range keys {
		if found[i] {
			entries = append(entries, statehamt.Entry{Type: key.Type, StateKey: key.StateKey, EventID: values[i]})
		}
	}
	resolved, err := d.stateEntriesFromHAMT(ctx, txn, entries)
	if err != nil {
		return nil, false, err
	}
	return resolved, true, nil
}
