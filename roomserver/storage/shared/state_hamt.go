// Copyright 2024 New Vector Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package shared

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// BackfillStateHAMTs builds persisted HAMTs for snapshots which predate
// snapshot-time HAMT writes. A non-positive limit selects a conservative batch
// size so callers can repeat this operation until it returns zero.
func (d *Database) BackfillStateHAMTs(ctx context.Context, limit int) (int, error) {
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
			return fmt.Errorf("%w (also failed to close snapshot rows: %w)", cause, closeErr)
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
	var failures []error
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
			// One unbuildable snapshot must not block the rest of the batch.
			logrus.WithError(err).WithField("state_snapshot_nid", item.nid).
				Warn("failed to backfill state HAMT; continuing")
			failures = append(failures, err)
			continue
		}
		if built {
			completed++
		}
	}
	return completed, errors.Join(failures...)
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

// StateSnapshotNIDs returns up to limit snapshot NIDs greater than after, in
// ascending order, so maintenance commands can page through every snapshot.
func (d *Database) StateSnapshotNIDs(
	ctx context.Context, after types.StateSnapshotNID, limit int,
) ([]types.StateSnapshotNID, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := d.DB.QueryContext(ctx, `
		SELECT state_snapshot_nid
		FROM roomserver_state_snapshots
		WHERE state_snapshot_nid > $1
		ORDER BY state_snapshot_nid
		LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("select state snapshot IDs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var nids []types.StateSnapshotNID
	for rows.Next() {
		var nid types.StateSnapshotNID
		if err = rows.Scan(&nid); err != nil {
			return nil, fmt.Errorf("scan state snapshot ID: %w", err)
		}
		nids = append(nids, nid)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate state snapshot IDs: %w", err)
	}
	return nids, nil
}

// VerifyStateHAMT checks that a snapshot's persisted HAMT is internally
// consistent and, when the snapshot still has legacy state blocks, that both
// representations hold the same state. It returns types.ErrNoStateHAMT when
// the snapshot has no HAMT yet.
func (d *Database) VerifyStateHAMT(
	ctx context.Context, snapshotNID types.StateSnapshotNID,
) error {
	return d.withStateHAMTReadLock(ctx, nil, func(txn *sql.Tx) error {
		return d.verifyStateHAMT(ctx, txn, snapshotNID)
	})
}

func (d *Database) verifyStateHAMT(
	ctx context.Context, txn *sql.Tx, snapshotNID types.StateSnapshotNID,
) error {
	root, err := d.StateHAMTTable.SelectStateHAMT(ctx, txn, snapshotNID)
	if err != nil {
		return fmt.Errorf("load state HAMT for snapshot %d: %w", snapshotNID, err)
	}
	if root == nil {
		return types.ErrNoStateHAMT
	}
	roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{root.RoomNID})
	if err != nil {
		return fmt.Errorf("load room for snapshot %d: %w", snapshotNID, err)
	}
	if len(roomIDs) == 0 {
		return fmt.Errorf("snapshot %d belongs to unknown room NID %d", snapshotNID, root.RoomNID)
	}
	roomID := roomIDs[0]
	nodes, err := d.loadStateHAMTNodes(ctx, txn, roomID, root, snapshotNID)
	if err != nil {
		return err
	}
	// The root record and the node set must hash back to the stored identity,
	// which catches corruption that a missing-node check alone would miss.
	verified, err := statehamt.Verify(roomID, root.RootRecord, nodes)
	if err != nil {
		return fmt.Errorf("state HAMT for snapshot %d does not verify: %w", snapshotNID, err)
	}
	if !bytes.Equal(verified.RootHash, root.RootHash) {
		return fmt.Errorf("state HAMT for snapshot %d has root hash %x, recorded %x", snapshotNID, verified.RootHash, root.RootHash)
	}
	if !bytes.Equal(verified.StateGroupID, root.StateGroupID) {
		return fmt.Errorf("state HAMT for snapshot %d has state group %x, recorded %x", snapshotNID, verified.StateGroupID, root.StateGroupID)
	}
	if verified.EntryCount != root.EntryCount {
		return fmt.Errorf("state HAMT for snapshot %d has %d entries, recorded %d", snapshotNID, verified.EntryCount, root.EntryCount)
	}

	blockLists, err := d.stateBlockNIDs(ctx, txn, []types.StateSnapshotNID{snapshotNID})
	if err != nil {
		return fmt.Errorf("load state blocks for snapshot %d: %w", snapshotNID, err)
	}
	if len(blockLists) != 1 || blockLists[0].StateSnapshotNID != snapshotNID {
		return fmt.Errorf("state blocks missing for snapshot %d", snapshotNID)
	}
	if len(blockLists[0].StateBlockNIDs) == 0 {
		// A snapshot written by a HAMT-only build has no legacy
		// representation to compare against; the identity check above is the
		// verification.
		return nil
	}
	hamtEntries, ok, err := d.stateHAMTEntriesLocked(ctx, txn, snapshotNID)
	if err != nil {
		return fmt.Errorf("load state HAMT entries for snapshot %d: %w", snapshotNID, err)
	}
	if !ok {
		return types.ErrNoStateHAMT
	}
	// Merge the blocks with last-wins semantics, as LoadStateAtSnapshot does,
	// so superseded entries for the same state key do not count as a mismatch.
	blockEntries, err := d.stateEntriesFromBlocks(ctx, txn, blockLists[0].StateBlockNIDs)
	if err != nil {
		return fmt.Errorf("load state entries for snapshot %d: %w", snapshotNID, err)
	}
	return diffStateEntries(snapshotNID, hamtEntries, blockEntries)
}

// diffStateEntries reports how two representations of the same snapshot's
// state differ. The HAMT entries and the block entries are both sorted sets of
// (event type, state key, event) tuples, so an equal set means an equal state.
func diffStateEntries(
	snapshotNID types.StateSnapshotNID, hamt, blocks []types.StateEntry,
) error {
	sorted := func(entries []types.StateEntry) []types.StateEntry {
		out := append([]types.StateEntry(nil), entries...)
		sort.Slice(out, func(i, j int) bool {
			if out[i].EventTypeNID != out[j].EventTypeNID {
				return out[i].EventTypeNID < out[j].EventTypeNID
			}
			if out[i].EventStateKeyNID != out[j].EventStateKeyNID {
				return out[i].EventStateKeyNID < out[j].EventStateKeyNID
			}
			return out[i].EventNID < out[j].EventNID
		})
		return out
	}
	h, b := sorted(hamt), sorted(blocks)
	if len(h) == len(b) {
		equal := true
		for i := range h {
			if h[i] != b[i] {
				equal = false
				break
			}
		}
		if equal {
			return nil
		}
	}
	missing, extra := stateEntrySetDifference(b, h), stateEntrySetDifference(h, b)
	const sample = 3
	return fmt.Errorf(
		"state HAMT for snapshot %d disagrees with its state blocks: %d entries in HAMT, %d in blocks, %d only in blocks%s, %d only in HAMT%s",
		snapshotNID, len(h), len(b), len(missing), sampleEntries(missing, sample),
		len(extra), sampleEntries(extra, sample),
	)
}

func stateEntrySetDifference(a, b []types.StateEntry) []types.StateEntry {
	in := make(map[types.StateEntry]struct{}, len(b))
	for _, entry := range b {
		in[entry] = struct{}{}
	}
	var out []types.StateEntry
	for _, entry := range a {
		if _, ok := in[entry]; !ok {
			out = append(out, entry)
		}
	}
	return out
}

func sampleEntries(entries []types.StateEntry, limit int) string {
	if len(entries) == 0 {
		return ""
	}
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return fmt.Sprintf(" (first: %v)", entries)
}

// purgeStateHAMTs removes a room's roots. Node records are content-addressed
// and shared; reclaiming unreachable nodes is deferred to maintenance so a
// foreground room purge does not trace every retained snapshot under an
// exclusive node lock.
func (d *Database) purgeStateHAMTs(ctx context.Context, txn *sql.Tx, roomNID types.RoomNID) error {
	if err := d.StateHAMTTable.LockStateHAMTNodes(ctx, txn, true); err != nil {
		return fmt.Errorf("lock state HAMT nodes for purge: %w", err)
	}
	if err := d.StateHAMTTable.DeleteStateHAMTRoom(ctx, txn, roomNID); err != nil {
		return fmt.Errorf("delete state HAMT roots for room %d: %w", roomNID, err)
	}
	return nil
}

// CollectStateHAMTGarbage traces every retained root and deletes unreachable
// content-addressed nodes. This is intentionally an explicit maintenance
// operation: its exclusive lock is held for the full history scan.
func (d *Database) CollectStateHAMTGarbage(ctx context.Context) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		if err := d.StateHAMTTable.LockStateHAMTNodes(ctx, txn, true); err != nil {
			return fmt.Errorf("lock state HAMT nodes for collection: %w", err)
		}
		return d.collectStateHAMTGarbageLocked(ctx, txn)
	})
}

func (d *Database) collectStateHAMTGarbageLocked(ctx context.Context, txn *sql.Tx) error {
	roots, err := d.StateHAMTTable.SelectStateHAMTRootRefs(ctx, txn)
	if err != nil {
		return fmt.Errorf("select remaining state HAMT roots: %w", err)
	}
	live := make(map[string]struct{})
	traced := make(map[string]struct{})
	for _, root := range roots {
		nodes := make([]statehamt.Node, 0, 16)
		complete := false
		for round := 0; round < 64; round++ {
			needed, err := statehamt.Needed(root.RoomID, root.RootHash, nodes)
			if err != nil {
				return fmt.Errorf("trace state HAMT rooted at %x for room %s: %w", root.RootHash, root.RoomID, err)
			}
			untraced := needed[:0]
			for _, hash := range needed {
				key := root.RoomID + "\x00" + string(hash)
				if _, ok := traced[key]; !ok {
					untraced = append(untraced, hash)
				}
			}
			needed = untraced
			if len(needed) == 0 {
				complete = true
				break
			}
			fetched, err := d.StateHAMTTable.SelectStateHAMTNodes(ctx, txn, needed)
			if err != nil {
				return fmt.Errorf("load state HAMT nodes during collection: %w", err)
			}
			if len(fetched) == 0 {
				return fmt.Errorf("state HAMT for room %s has missing nodes during collection", root.RoomID)
			}
			for _, node := range fetched {
				key := root.RoomID + "\x00" + string(node.Hash)
				if _, ok := traced[key]; ok {
					continue
				}
				traced[key] = struct{}{}
				live[string(node.Hash)] = struct{}{}
				nodes = append(nodes, statehamt.Node{Hash: node.Hash, Bytes: node.Bytes})
			}
		}
		if !complete {
			return fmt.Errorf("state HAMT for room %s exceeded the traversal limit during collection", root.RoomID)
		}
	}

	allHashes, err := d.StateHAMTTable.SelectStateHAMTNodeHashes(ctx, txn)
	if err != nil {
		return fmt.Errorf("select state HAMT node hashes during collection: %w", err)
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

// persistStateHAMT builds the HAMT for a snapshot that predates HAMT storage
// from the entries of its state blocks and stores it.
func (d *Database) persistStateHAMT(
	ctx context.Context,
	txn *sql.Tx,
	roomNID types.RoomNID,
	snapshotNID types.StateSnapshotNID,
	stateBlockNIDs []types.StateBlockNID,
) error {
	stateEntries, err := d.stateEntriesFromBlocks(ctx, txn, stateBlockNIDs)
	if err != nil {
		return err
	}
	persisted, err := d.buildStateHAMT(ctx, txn, roomNID, stateEntries)
	if err != nil {
		return err
	}
	return d.insertStateHAMT(ctx, txn, roomNID, snapshotNID, persisted)
}

// stateEntriesFromBlocks materializes the full state a snapshot's state blocks
// describe.
func (d *Database) stateEntriesFromBlocks(
	ctx context.Context, txn *sql.Tx, stateBlockNIDs []types.StateBlockNID,
) ([]types.StateEntry, error) {
	blocks, err := d.StateBlockTable.BulkSelectStateBlockEntries(ctx, txn, stateBlockNIDs)
	if err != nil {
		return nil, fmt.Errorf("BulkSelectStateBlockEntries: %w", err)
	}
	var eventNIDs []types.EventNID
	for _, block := range blocks {
		eventNIDs = append(eventNIDs, block...)
	}
	entriesByNID := make(map[types.EventNID]types.StateEntry, len(eventNIDs))
	if len(eventNIDs) > 0 {
		entries, err := d.EventsTable.BulkSelectStateEventByNID(ctx, txn, eventNIDs, nil)
		if err != nil {
			return nil, fmt.Errorf("BulkSelectStateEventByNID: %w", err)
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
				return nil, fmt.Errorf("state block references unknown event NID %d", eventNID)
			}
			ordered[entry.StateKeyTuple] = entry
		}
	}
	stateEntries := make([]types.StateEntry, 0, len(ordered))
	for _, entry := range ordered {
		stateEntries = append(stateEntries, entry)
	}
	return stateEntries, nil
}

// buildStateHAMT persists nothing; it computes the HAMT records for a full
// state so the caller can key the snapshot by the resulting root.
func (d *Database) buildStateHAMT(
	ctx context.Context, txn *sql.Tx, roomNID types.RoomNID, state []types.StateEntry,
) (*statehamt.Persisted, error) {
	hamtEntries, err := d.hamtEntriesFromStateEntries(ctx, txn, dedupeStateEntries(state))
	if err != nil {
		return nil, err
	}
	roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{roomNID})
	if err != nil {
		return nil, fmt.Errorf("BulkSelectRoomIDs: %w", err)
	}
	if len(roomIDs) == 0 {
		return nil, fmt.Errorf("room NID %d has no room ID", roomNID)
	}
	persisted, err := statehamt.Build(roomIDs[0], hamtEntries)
	if err != nil {
		return nil, fmt.Errorf("statehamt.Build: %w", err)
	}
	return persisted, nil
}

// addStateDelta stores the snapshot produced by replacing the entries for
// changed's state keys in the parent snapshot. It touches O(len(changed) *
// log n) HAMT nodes instead of rebuilding the whole tree, and returns
// types.ErrNoStateHAMT when the parent has no persisted HAMT so the caller can
// fall back to storing the full state.
func (d *Database) addStateDelta(
	ctx context.Context, txn *sql.Tx, roomNID types.RoomNID,
	parentNID types.StateSnapshotNID, changed []types.StateEntry,
) (stateNID types.StateSnapshotNID, err error) {
	err = d.Writer.Do(d.DB, txn, func(txn *sql.Tx) error {
		// Keep purge's exclusive mark-and-sweep out for the entire read,
		// mutation, and root publication. Locking only while inserting nodes
		// would still let a purge delete a parent node between these steps.
		if err := d.StateHAMTTable.LockStateHAMTNodes(ctx, txn, false); err != nil {
			return fmt.Errorf("lock state HAMT nodes for delta: %w", err)
		}
		parent, err := d.StateHAMTTable.SelectStateHAMT(ctx, txn, parentNID)
		if err != nil {
			return fmt.Errorf("SelectStateHAMT: %w", err)
		}
		if parent == nil {
			return types.ErrNoStateHAMT
		}
		if parent.RoomNID != roomNID {
			return fmt.Errorf("parent snapshot %d belongs to room %d, not %d", parentNID, parent.RoomNID, roomNID)
		}
		roomIDs, err := d.RoomsTable.BulkSelectRoomIDs(ctx, txn, []types.RoomNID{roomNID})
		if err != nil {
			return fmt.Errorf("BulkSelectRoomIDs: %w", err)
		}
		if len(roomIDs) == 0 {
			return fmt.Errorf("room NID %d has no room ID", roomNID)
		}
		entries, err := d.hamtEntriesFromStateEntries(ctx, txn, dedupeStateEntries(changed))
		if err != nil {
			return err
		}
		mutations := make([]statehamt.Mutation, len(entries))
		for i := range entries {
			mutations[i] = statehamt.Mutation{
				Type: entries[i].Type, StateKey: entries[i].StateKey, EventID: &entries[i].EventID,
			}
		}
		fetch := func(hashes [][]byte) ([]statehamt.Node, error) {
			rows, err := d.StateHAMTTable.SelectStateHAMTNodes(ctx, txn, hashes)
			if err != nil {
				return nil, fmt.Errorf("SelectStateHAMTNodes: %w", err)
			}
			nodes := make([]statehamt.Node, len(rows))
			for i, row := range rows {
				nodes[i] = statehamt.Node{Hash: row.Hash, Bytes: row.Bytes}
			}
			return nodes, nil
		}
		persisted, err := statehamt.Apply(roomIDs[0], parent.RootRecord, parent.EntryCount, fetch, mutations)
		if err != nil {
			var missing *statehamt.MissingNodesError
			if errors.As(err, &missing) {
				logrus.WithFields(logrus.Fields{
					"room_nid":     roomNID,
					"snapshot_nid": parentNID,
				}).WithError(err).Warn("State HAMT parent is missing nodes; inspect with backfill-state-hamt -verify")
			}
			return fmt.Errorf("statehamt.Apply: %w", err)
		}
		stateNID, err = d.StateSnapshotTable.InsertState(ctx, txn, roomNID, persisted.RootHash, nil)
		if err != nil {
			return fmt.Errorf("d.StateSnapshotTable.InsertState: %w", err)
		}
		return d.insertStateHAMTLocked(ctx, txn, roomNID, stateNID, persisted)
	})
	if err != nil {
		return 0, err
	}
	return stateNID, nil
}

// dedupeStateEntries keeps one entry per (event type, state key) tuple, the
// last one winning, which is the order the legacy state-block merge used.
func dedupeStateEntries(entries []types.StateEntry) []types.StateEntry {
	if len(entries) <= 1 {
		return entries
	}
	positions := make(map[types.StateKeyTuple]int, len(entries))
	result := make([]types.StateEntry, 0, len(entries))
	for _, entry := range entries {
		if i, ok := positions[entry.StateKeyTuple]; ok {
			result[i] = entry
			continue
		}
		positions[entry.StateKeyTuple] = len(result)
		result = append(result, entry)
	}
	return result
}

// insertStateHAMT stores the nodes and root record of a built HAMT against a
// snapshot.
func (d *Database) insertStateHAMT(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, snapshotNID types.StateSnapshotNID, persisted *statehamt.Persisted,
) error {
	if err := d.StateHAMTTable.LockStateHAMTNodes(ctx, txn, false); err != nil {
		return fmt.Errorf("lock state HAMT nodes: %w", err)
	}
	return d.insertStateHAMTLocked(ctx, txn, roomNID, snapshotNID, persisted)
}

// insertStateHAMTLocked writes a HAMT while the caller holds the shared
// state-HAMT node lock for the enclosing transaction.
func (d *Database) insertStateHAMTLocked(
	ctx context.Context, txn *sql.Tx,
	roomNID types.RoomNID, snapshotNID types.StateSnapshotNID, persisted *statehamt.Persisted,
) error {
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
	var entries []types.StateEntry
	var ok bool
	err := d.withStateHAMTReadLock(ctx, txn, func(readTxn *sql.Tx) error {
		var err error
		entries, ok, err = d.stateHAMTEntriesLocked(ctx, readTxn, snapshotNID)
		return err
	})
	return entries, ok, err
}

func (d *Database) stateHAMTEntriesLocked(
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
	return d.loadStateHAMTNodesFrom(ctx, txn, roomID, root, snapshotNID, nil)
}

// loadStateHAMTNodesFrom completes a node set in batches, starting from nodes
// the caller already holds so they are not read again.
func (d *Database) loadStateHAMTNodesFrom(
	ctx context.Context, txn *sql.Tx, roomID string, root *tables.StateHAMTRoot,
	snapshotNID types.StateSnapshotNID, have []statehamt.Node,
) ([]statehamt.Node, error) {
	nodes := make([]statehamt.Node, 0, len(have)+8)
	seen := make(map[string]struct{}, len(have)+8)
	for _, node := range have {
		seen[string(node.Hash)] = struct{}{}
		nodes = append(nodes, node)
	}
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
	var entries []types.StateEntry
	var ok bool
	err := d.withStateHAMTReadLock(ctx, txn, func(readTxn *sql.Tx) error {
		var err error
		entries, ok, err = d.stateHAMTEntriesForTuplesLocked(ctx, readTxn, snapshotNID, tuples)
		return err
	})
	return entries, ok, err
}

func (d *Database) stateHAMTEntriesForTuplesLocked(
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
	values, found, err := d.lookupStateHAMTKeys(ctx, txn, roomID, root, snapshotNID, keys)
	if err != nil {
		return nil, false, err
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

// lookupStateHAMTKeys loads only the nodes visited by the requested keys.
// Lookup reports the next missing node on those paths; fetching that node and
// retrying discovers the next level without loading unrelated subtrees.
//
// Each call reveals at most one missing node, so a lookup over many keys would
// need one query per node on their combined paths. After a few rounds this
// falls back to the batched whole-tree load, which is fewer round trips once
// the key set is large.
func (d *Database) lookupStateHAMTKeys(
	ctx context.Context, txn *sql.Tx, roomID string, root *tables.StateHAMTRoot,
	snapshotNID types.StateSnapshotNID, keys []statehamt.Key,
) ([]string, []bool, error) {
	const keyedRounds = 8
	nodes := make([]statehamt.Node, 0, keyedRounds)
	seen := make(map[string]struct{}, keyedRounds)
	for round := 0; round < keyedRounds; round++ {
		values, found, err := statehamt.Lookup(roomID, root.RootHash, nodes, keys)
		if err == nil {
			return values, found, nil
		}
		var missing *statehamt.MissingNodesError
		if !errors.As(err, &missing) {
			return nil, nil, fmt.Errorf("statehamt.Lookup: %w", err)
		}
		fetched, fetchErr := d.StateHAMTTable.SelectStateHAMTNodes(ctx, txn, missing.Hashes)
		if fetchErr != nil {
			return nil, nil, fmt.Errorf("SelectStateHAMTNodes: %w", fetchErr)
		}
		added := 0
		for _, node := range fetched {
			key := string(node.Hash)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			nodes = append(nodes, statehamt.Node{Hash: node.Hash, Bytes: node.Bytes})
			added++
		}
		if added == 0 {
			return nil, nil, fmt.Errorf("state HAMT for snapshot %d is missing lookup nodes: %w", snapshotNID, err)
		}
	}
	// Too many paths for per-node discovery; load the tree in batches instead,
	// reusing the nodes already fetched.
	nodes, err := d.loadStateHAMTNodesFrom(ctx, txn, roomID, root, snapshotNID, nodes)
	if err != nil {
		return nil, nil, err
	}
	values, found, err := statehamt.Lookup(roomID, root.RootHash, nodes, keys)
	if err != nil {
		return nil, nil, fmt.Errorf("statehamt.Lookup: %w", err)
	}
	return values, found, nil
}

// withStateHAMTReadLock keeps the purge sweep from deleting nodes between a
// root lookup and the corresponding node reads. The advisory lock is
// transaction-scoped, so a standalone read opens an owned transaction when the
// backend has real node locks. SQLite has none, so it reads without one;
// callers with an existing transaction keep using it.
func (d *Database) withStateHAMTReadLock(
	ctx context.Context, txn *sql.Tx, read func(*sql.Tx) error,
) error {
	if txn == nil {
		if !d.StateHAMTTable.HasNodeLocks() {
			// SQLite: the lock is a no-op, and an owned read transaction would
			// only hold a database lock against the single writer.
			return read(nil)
		}
		return sqlutil.WithTransaction(d.DB, func(txn *sql.Tx) error {
			return d.withStateHAMTReadLock(ctx, txn, read)
		})
	}
	if err := d.StateHAMTTable.LockStateHAMTNodes(ctx, txn, false); err != nil {
		return fmt.Errorf("lock state HAMT nodes for read: %w", err)
	}
	return read(txn)
}
