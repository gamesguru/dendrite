// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full copyright details.

package shared

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/statehamt"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type countingHAMTTable struct {
	tables.StateHAMT
	nodes  map[string]tables.StateHAMTNode
	loaded map[string]struct{}
}

func (t *countingHAMTTable) SelectStateHAMTNodes(
	_ context.Context, _ *sql.Tx, hashes [][]byte,
) ([]tables.StateHAMTNode, error) {
	var selected []tables.StateHAMTNode
	for _, hash := range hashes {
		if node, ok := t.nodes[string(hash)]; ok {
			selected = append(selected, node)
			t.loaded[string(hash)] = struct{}{}
		}
	}
	return selected, nil
}

func TestLookupStateHAMTKeysLoadsOnlyRequestedPaths(t *testing.T) {
	const roomID = "!lookup:example.org"
	entries := make([]statehamt.Entry, 512)
	for i := range entries {
		entries[i] = statehamt.Entry{
			Type: "m.room.member", StateKey: fmt.Sprintf("@u%d:example.org", i), EventID: fmt.Sprintf("$e%d", i),
		}
	}
	persisted, err := statehamt.Build(roomID, entries)
	require.NoError(t, err)

	stored := &countingHAMTTable{
		nodes:  make(map[string]tables.StateHAMTNode, len(persisted.Nodes)),
		loaded: make(map[string]struct{}),
	}
	for _, node := range persisted.Nodes {
		stored.nodes[string(node.Hash)] = tables.StateHAMTNode{Hash: node.Hash, Bytes: node.Bytes}
	}
	db := &Database{StateHAMTTable: stored}
	values, found, err := db.lookupStateHAMTKeys(
		context.Background(), nil, roomID,
		&tables.StateHAMTRoot{RootHash: persisted.RootHash},
		types.StateSnapshotNID(1),
		[]statehamt.Key{{Type: "m.room.member", StateKey: "@u256:example.org"}},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"$e256"}, values)
	require.Equal(t, []bool{true}, found)
	require.NotEmpty(t, stored.loaded)
	require.Less(t, len(stored.loaded), len(persisted.Nodes), "lookup should not load unrelated subtrees")
}

// Lookup reveals one missing node per call, so a large key set needs far more
// rounds than any fixed keyed bound. It must still resolve, via the batched
// whole-tree fallback, instead of failing once the round budget is spent.
func TestLookupStateHAMTKeysHandlesManyKeys(t *testing.T) {
	const roomID = "!lookup-many:example.org"
	const total, wanted = 512, 64
	entries := make([]statehamt.Entry, total)
	for i := range entries {
		entries[i] = statehamt.Entry{
			Type: "m.room.member", StateKey: fmt.Sprintf("@u%d:example.org", i), EventID: fmt.Sprintf("$e%d", i),
		}
	}
	persisted, err := statehamt.Build(roomID, entries)
	require.NoError(t, err)

	stored := &countingHAMTTable{
		nodes:  make(map[string]tables.StateHAMTNode, len(persisted.Nodes)),
		loaded: make(map[string]struct{}),
	}
	for _, node := range persisted.Nodes {
		stored.nodes[string(node.Hash)] = tables.StateHAMTNode{Hash: node.Hash, Bytes: node.Bytes}
	}
	keys := make([]statehamt.Key, wanted)
	wantValues := make([]string, wanted)
	for i := range keys {
		keys[i] = statehamt.Key{Type: "m.room.member", StateKey: fmt.Sprintf("@u%d:example.org", i)}
		wantValues[i] = fmt.Sprintf("$e%d", i)
	}
	db := &Database{StateHAMTTable: stored}
	values, found, err := db.lookupStateHAMTKeys(
		context.Background(), nil, roomID,
		&tables.StateHAMTRoot{RootHash: persisted.RootHash},
		types.StateSnapshotNID(1),
		keys,
	)
	require.NoError(t, err)
	require.Equal(t, wantValues, values)
	for i, ok := range found {
		require.True(t, ok, "key %d was not found", i)
	}
	// The keyed rounds cannot cover 64 paths, so the batched whole-tree
	// fallback must have run and loaded every persisted node.
	require.Len(t, stored.loaded, len(persisted.Nodes), "fallback should load the whole tree")
}
