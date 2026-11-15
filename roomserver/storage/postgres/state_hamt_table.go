// Copyright 2024 New Vector Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

const stateHAMTSchema = `
  CREATE TABLE IF NOT EXISTS roomserver_state_hamt_snapshots (
    state_snapshot_nid BIGINT PRIMARY KEY,
    room_nid BIGINT NOT NULL,
    root_hash BYTEA NOT NULL,
    state_group_id BYTEA NOT NULL,
    root_record BYTEA NOT NULL,
    entry_count BIGINT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS roomserver_state_hamt_snapshots_room_idx
    ON roomserver_state_hamt_snapshots (room_nid);
  CREATE TABLE IF NOT EXISTS roomserver_state_hamt_nodes (
    node_hash BYTEA PRIMARY KEY,
    node_bytes BYTEA NOT NULL
  );
`

const insertStateHAMTSQL = `
  INSERT INTO roomserver_state_hamt_snapshots
    (state_snapshot_nid, room_nid, root_hash, state_group_id, root_record, entry_count)
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (state_snapshot_nid) DO NOTHING
`

const selectStateHAMTSQL = `
  SELECT room_nid, root_hash, state_group_id, root_record, entry_count
  FROM roomserver_state_hamt_snapshots WHERE state_snapshot_nid = $1
`

const selectStateHAMTNodesSQL = `
  SELECT node_hash, node_bytes FROM roomserver_state_hamt_nodes WHERE node_hash IN ($1)
`

// stateHAMTLookupBatch bounds the hashes sent in one IN query.
const stateHAMTLookupBatch = 500

const (
	deleteStateHAMTRoomSQL     = `DELETE FROM roomserver_state_hamt_snapshots WHERE room_nid = $1`
	selectStateHAMTRootRefsSQL = `
  SELECT DISTINCT r.room_id, h.root_hash
  FROM roomserver_state_hamt_snapshots AS h
  JOIN roomserver_rooms AS r ON r.room_nid = h.room_nid
`
)

const (
	selectStateHAMTNodeHashesSQL = `SELECT node_hash FROM roomserver_state_hamt_nodes`
	deleteStateHAMTNodesSQL      = `DELETE FROM roomserver_state_hamt_nodes WHERE node_hash IN ($1)`
)

type stateHAMTStatements struct{ db *sql.DB }

func PrepareStateHAMTTable(db *sql.DB) (tables.StateHAMT, error) {
	if _, err := db.Exec(stateHAMTSchema); err != nil {
		return nil, err
	}
	return &stateHAMTStatements{db: db}, nil
}

func (s *stateHAMTStatements) exec(
	ctx context.Context, txn *sql.Tx, query string, args ...any,
) (sql.Result, error) {
	if txn != nil {
		return txn.ExecContext(ctx, query, args...)
	}
	return s.db.ExecContext(ctx, query, args...)
}

func (s *stateHAMTStatements) queryRow(
	ctx context.Context, txn *sql.Tx, query string, args ...any,
) *sql.Row {
	if txn != nil {
		return txn.QueryRowContext(ctx, query, args...)
	}
	return s.db.QueryRowContext(ctx, query, args...)
}

func (s *stateHAMTStatements) query(
	ctx context.Context, txn *sql.Tx, query string, args ...any,
) (*sql.Rows, error) {
	if txn != nil {
		return txn.QueryContext(ctx, query, args...)
	}
	return s.db.QueryContext(ctx, query, args...)
}

func (s *stateHAMTStatements) InsertStateHAMT(
	ctx context.Context, txn *sql.Tx, snapshotNID types.StateSnapshotNID,
	roomNID types.RoomNID, rootHash, stateGroupID, rootRecord []byte, entryCount int,
) error {
	_, err := s.exec(
		ctx, txn, insertStateHAMTSQL,
		snapshotNID, roomNID, rootHash, stateGroupID, rootRecord, entryCount,
	)
	return err
}

// insertStateHAMTNodesBatch bounds the rows in one multi-row insert. Each row
// binds two parameters, well below the protocol limit.
const insertStateHAMTNodesBatch = 1000

func (s *stateHAMTStatements) InsertStateHAMTNodes(
	ctx context.Context, txn *sql.Tx, nodes []tables.StateHAMTNode,
) error {
	for start := 0; start < len(nodes); start += insertStateHAMTNodesBatch {
		end := min(start+insertStateHAMTNodesBatch, len(nodes))
		batch := nodes[start:end]
		placeholders := make([]string, len(batch))
		args := make([]any, 0, len(batch)*2)
		for i, node := range batch {
			first := len(args) + 1
			placeholders[i] = fmt.Sprintf("($%d, $%d)", first, first+1)
			args = append(args, node.Hash, node.Bytes)
		}
		query := `INSERT INTO roomserver_state_hamt_nodes (node_hash, node_bytes) VALUES ` +
			strings.Join(placeholders, ", ") + ` ON CONFLICT (node_hash) DO NOTHING`
		if _, err := s.exec(ctx, txn, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *stateHAMTStatements) SelectStateHAMT(
	ctx context.Context, txn *sql.Tx, snapshotNID types.StateSnapshotNID,
) (*tables.StateHAMTRoot, error) {
	root := tables.StateHAMTRoot{SnapshotNID: snapshotNID}
	err := s.queryRow(ctx, txn, selectStateHAMTSQL, snapshotNID).
		Scan(&root.RoomNID, &root.RootHash, &root.StateGroupID, &root.RootRecord, &root.EntryCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &root, nil
}

func (s *stateHAMTStatements) SelectStateHAMTNodes(
	ctx context.Context, txn *sql.Tx, hashes [][]byte,
) ([]tables.StateHAMTNode, error) {
	var nodes []tables.StateHAMTNode
	for start := 0; start < len(hashes); start += stateHAMTLookupBatch {
		end := min(start+stateHAMTLookupBatch, len(hashes))
		batch, err := s.selectStateHAMTNodesBatch(ctx, txn, hashes[start:end])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, batch...)
	}
	return nodes, nil
}

func (s *stateHAMTStatements) selectStateHAMTNodesBatch(
	ctx context.Context, txn *sql.Tx, hashes [][]byte,
) ([]tables.StateHAMTNode, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	params := make([]any, len(hashes))
	for i, hash := range hashes {
		params[i] = hash
	}
	query := strings.Replace(selectStateHAMTNodesSQL, "($1)", sqlutil.QueryVariadic(len(params)), 1)
	rows, err := s.query(ctx, txn, query, params...)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectStateHAMTNodes rows close failed")
	var nodes []tables.StateHAMTNode
	for rows.Next() {
		var node tables.StateHAMTNode
		if err := rows.Scan(&node.Hash, &node.Bytes); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}

// stateHAMTNodesLockKey is the advisory lock key that orders node writers
// against the unreachable-node sweep.
const stateHAMTNodesLockKey = 0x484d4e44 // "HMND"

func (s *stateHAMTStatements) HasNodeLocks() bool { return true }

func (s *stateHAMTStatements) LockStateHAMTNodes(
	ctx context.Context, txn *sql.Tx, exclusive bool,
) error {
	if txn == nil {
		return fmt.Errorf("LockStateHAMTNodes requires a transaction")
	}
	query := "SELECT pg_advisory_xact_lock_shared($1)"
	if exclusive {
		query = "SELECT pg_advisory_xact_lock($1)"
	}
	_, err := txn.ExecContext(ctx, query, stateHAMTNodesLockKey)
	return err
}

func (s *stateHAMTStatements) DeleteStateHAMTRoom(
	ctx context.Context, txn *sql.Tx, roomNID types.RoomNID,
) error {
	_, err := s.exec(ctx, txn, deleteStateHAMTRoomSQL, roomNID)
	return err
}

func (s *stateHAMTStatements) SelectStateHAMTRootRefs(
	ctx context.Context, txn *sql.Tx,
) ([]tables.StateHAMTRootRef, error) {
	rows, err := s.query(ctx, txn, selectStateHAMTRootRefsSQL)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectStateHAMTRootRefs rows close failed")
	var roots []tables.StateHAMTRootRef
	for rows.Next() {
		var root tables.StateHAMTRootRef
		if err := rows.Scan(&root.RoomID, &root.RootHash); err != nil {
			return nil, err
		}
		roots = append(roots, root)
	}
	return roots, rows.Err()
}

func (s *stateHAMTStatements) SelectStateHAMTNodeHashes(
	ctx context.Context, txn *sql.Tx,
) ([][]byte, error) {
	rows, err := s.query(ctx, txn, selectStateHAMTNodeHashesSQL)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectStateHAMTNodeHashes rows close failed")
	var hashes [][]byte
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		hashes = append(hashes, hash)
	}
	return hashes, rows.Err()
}

func (s *stateHAMTStatements) DeleteStateHAMTNodes(
	ctx context.Context, txn *sql.Tx, hashes [][]byte,
) error {
	for start := 0; start < len(hashes); start += stateHAMTLookupBatch {
		end := min(start+stateHAMTLookupBatch, len(hashes))
		args := make([]any, end-start)
		for i, hash := range hashes[start:end] {
			args[i] = hash
		}
		query := strings.Replace(deleteStateHAMTNodesSQL, "($1)", sqlutil.QueryVariadic(len(args)), 1)
		if _, err := s.exec(ctx, txn, query, args...); err != nil {
			return err
		}
	}
	return nil
}
