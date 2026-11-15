// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package shared_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// TestCollectStateHAMTGarbageKeepsSharedNodes builds two snapshot roots in one
// room that share subtrees, plus an unreferenced node, and checks that the
// sweep keeps every node reachable from either root and deletes only the
// orphan.
func TestCollectStateHAMTGarbageKeepsSharedNodes(t *testing.T) {
	db, clear := mustCreateRoomserverDatabase(t, test.DBTypeSQLite)
	defer clear()
	ctx := context.Background()
	const roomID = "!gc:example.org"

	roomsTable := db.RoomsTable
	hamtTable, err := sqlite3.PrepareStateHAMTTable(db.DB)
	require.NoError(t, err)
	db.RoomsTable = roomsTable
	db.StateHAMTTable = hamtTable

	var roomNID types.RoomNID
	require.NoError(t, db.Writer.Do(db.DB, nil, func(txn *sql.Tx) error {
		var err error
		roomNID, err = roomsTable.InsertRoomNID(ctx, txn, roomID, "10")
		return err
	}))

	entries := make([]statehamt.Entry, 300)
	for i := range entries {
		entries[i] = statehamt.Entry{
			Type: "m.room.member", StateKey: fmt.Sprintf("@u%d:example.org", i), EventID: fmt.Sprintf("$e%d", i),
		}
	}
	first, err := statehamt.Build(roomID, entries)
	require.NoError(t, err)
	// A second root that differs by one entry shares most subtrees with the first.
	second, err := statehamt.Build(roomID, append(entries[:299:299], statehamt.Entry{
		Type: "m.room.member", StateKey: "@u299:example.org", EventID: "$changed",
	}))
	require.NoError(t, err)
	require.NotEqual(t, first.RootHash, second.RootHash)

	orphan := []byte("orphan-node-hash-0000000000000000")
	require.NoError(t, db.Writer.Do(db.DB, nil, func(txn *sql.Tx) error {
		for i, p := range []*statehamt.Persisted{first, second} {
			nodes := make([]tables.StateHAMTNode, len(p.Nodes))
			for j, n := range p.Nodes {
				nodes[j] = tables.StateHAMTNode{Hash: n.Hash, Bytes: n.Bytes}
			}
			if err := hamtTable.InsertStateHAMTNodes(ctx, txn, nodes); err != nil {
				return err
			}
			if err := hamtTable.InsertStateHAMT(
				ctx, txn, types.StateSnapshotNID(i+1), roomNID,
				p.RootHash, p.StateGroupID, p.RootRecord, p.EntryCount,
			); err != nil {
				return err
			}
		}
		return hamtTable.InsertStateHAMTNodes(ctx, txn, []tables.StateHAMTNode{{Hash: orphan, Bytes: []byte("x")}})
	}))

	want := map[string]struct{}{}
	for _, p := range []*statehamt.Persisted{first, second} {
		for _, n := range p.Nodes {
			want[string(n.Hash)] = struct{}{}
		}
	}
	require.Less(t, len(want), len(first.Nodes)+len(second.Nodes), "roots must share nodes")

	require.NoError(t, db.CollectStateHAMTGarbage(ctx))

	var got [][]byte
	require.NoError(t, db.Writer.Do(db.DB, nil, func(txn *sql.Tx) error {
		var err error
		got, err = hamtTable.SelectStateHAMTNodeHashes(ctx, txn)
		return err
	}))
	gotSet := map[string]struct{}{}
	for _, h := range got {
		gotSet[string(h)] = struct{}{}
	}
	require.Equal(t, want, gotSet, "shared nodes survive, orphan is deleted")
}
