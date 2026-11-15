// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package statehamt

import (
	"bytes"
	"fmt"
	"testing"
)

// nodeFetcher serves persisted nodes from a map, as the database does.
type nodeFetcher struct {
	nodes map[string][]byte
	calls int
}

func newFetcher(nodes []Node) *nodeFetcher {
	f := &nodeFetcher{nodes: make(map[string][]byte, len(nodes))}
	for _, node := range nodes {
		f.nodes[string(node.Hash)] = node.Bytes
	}
	return f
}

func (f *nodeFetcher) fetch(hashes [][]byte) ([]Node, error) {
	f.calls++
	out := make([]Node, 0, len(hashes))
	for _, hash := range hashes {
		data, ok := f.nodes[string(hash)]
		if !ok {
			continue
		}
		out = append(out, Node{Hash: hash, Bytes: data})
	}
	return out, nil
}

func testEntries(count int) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		entries[i] = Entry{
			Type:     "m.room.member",
			StateKey: fmt.Sprintf("@user%d:example.org", i),
			EventID:  fmt.Sprintf("$parent%d", i),
		}
	}
	return entries
}

// TestApplyMatchesFullRebuild is the contract the storage layer depends on:
// applying a delta to the persisted parent must produce exactly the records a
// full rebuild of the resulting state produces, including the root hash that
// doubles as the snapshot identity.
func TestApplyMatchesFullRebuild(t *testing.T) {
	const roomID = "!room:example.org"
	parentEntries := testEntries(200)

	parent, err := Build(roomID, parentEntries)
	if err != nil {
		t.Fatalf("Build parent: %v", err)
	}

	// Replace one entry, add two, and remove one.
	replaced := "$replaced3"
	want := make([]Entry, 0, len(parentEntries)+1)
	for i, entry := range parentEntries {
		switch i {
		case 3:
			entry.EventID = replaced
		case 150:
			continue
		}
		want = append(want, entry)
	}
	added := []Entry{
		{Type: "m.room.member", StateKey: "@new:example.org", EventID: "$new"},
		{Type: "m.room.topic", StateKey: "", EventID: "$topic"},
	}
	want = append(want, added...)

	mutations := []Mutation{
		{Type: "m.room.member", StateKey: parentEntries[3].StateKey, EventID: &replaced},
		{Type: "m.room.member", StateKey: parentEntries[150].StateKey},
	}
	for i := range added {
		mutations = append(mutations, Mutation{
			Type: added[i].Type, StateKey: added[i].StateKey, EventID: &added[i].EventID,
		})
	}

	fetcher := newFetcher(parent.Nodes)
	got, err := Apply(roomID, parent.RootRecord, parent.EntryCount, fetcher.fetch, mutations)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if fetcher.calls == 0 {
		t.Fatal("Apply never asked for parent nodes")
	}

	full, err := Build(roomID, want)
	if err != nil {
		t.Fatalf("Build expected state: %v", err)
	}
	assertSamePersisted(t, roomID, got, full, parent)
}

// TestApplyFromEmptyMatchesBuild checks the no-parent case against the empty
// state a full rebuild produces.
func TestApplyFromEmptyMatchesBuild(t *testing.T) {
	const roomID = "!empty:example.org"
	entries := []Entry{
		{Type: "m.room.create", StateKey: "", EventID: "$create"},
		{Type: "m.room.member", StateKey: "@alice:example.org", EventID: "$alice"},
	}

	mutations := make([]Mutation, len(entries))
	for i := range entries {
		mutations[i] = Mutation{Type: entries[i].Type, StateKey: entries[i].StateKey, EventID: &entries[i].EventID}
	}

	empty, err := Apply(roomID, nil, 0, func([][]byte) ([]Node, error) { return nil, nil }, mutations)
	if err != nil {
		t.Fatalf("Apply from empty: %v", err)
	}
	full, err := Build(roomID, entries)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	assertSamePersisted(t, roomID, empty, full)
}

func assertSamePersisted(t *testing.T, roomID string, got, want *Persisted, parents ...*Persisted) {
	t.Helper()
	var nodes []Node
	for _, parent := range parents {
		nodes = append(nodes, parent.Nodes...)
	}
	nodes = append(nodes, got.Nodes...)
	gotEntries, err := Entries(roomID, got.RootHash, nodes)
	if err != nil {
		t.Fatalf("reconstruct persisted entries: %v", err)
	}
	wantEntries, err := Entries(roomID, want.RootHash, want.Nodes)
	if err != nil {
		t.Fatalf("reconstruct expected entries: %v", err)
	}
	if !sameEntries(gotEntries, wantEntries) {
		t.Errorf("reconstructed entries = %v, want %v", gotEntries, wantEntries)
	}
	if !bytes.Equal(got.RootHash, want.RootHash) {
		t.Errorf("root hash = %x, want %x", got.RootHash, want.RootHash)
	}
	if !bytes.Equal(got.StateGroupID, want.StateGroupID) {
		t.Errorf("state group id = %x, want %x", got.StateGroupID, want.StateGroupID)
	}
	if !bytes.Equal(got.RootRecord, want.RootRecord) {
		t.Errorf("root record differs from a full rebuild")
	}
	if got.EntryCount != want.EntryCount {
		t.Errorf("entry count = %d, want %d", got.EntryCount, want.EntryCount)
	}
}

func sameEntries(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[Entry]int, len(a))
	for _, entry := range a {
		counts[entry]++
	}
	for _, entry := range b {
		if counts[entry] == 0 {
			return false
		}
		counts[entry]--
	}
	return true
}
