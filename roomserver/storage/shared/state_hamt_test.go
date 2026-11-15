// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package shared

import (
	"strings"
	"testing"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

func testStateEntry(eventType, stateKey types.EventTypeNID, event types.EventNID) types.StateEntry {
	return types.StateEntry{
		StateKeyTuple: types.StateKeyTuple{
			EventTypeNID:     eventType,
			EventStateKeyNID: types.EventStateKeyNID(stateKey),
		},
		EventNID: event,
	}
}

func TestDiffStateEntries(t *testing.T) {
	create := testStateEntry(1, 0, 10)
	member := testStateEntry(2, 3, 11)

	tests := []struct {
		name      string
		hamt      []types.StateEntry
		blocks    []types.StateEntry
		wantErr   bool
		wantInErr []string
	}{
		{
			name:   "equal sets in different order",
			hamt:   []types.StateEntry{member, create},
			blocks: []types.StateEntry{create, member},
		},
		{
			name:      "entry only in blocks",
			hamt:      []types.StateEntry{create},
			blocks:    []types.StateEntry{create, member},
			wantErr:   true,
			wantInErr: []string{"1 entries in HAMT", "2 in blocks", "1 only in blocks"},
		},
		{
			name:      "entry only in HAMT",
			hamt:      []types.StateEntry{create, member},
			blocks:    []types.StateEntry{create},
			wantErr:   true,
			wantInErr: []string{"1 only in HAMT"},
		},
		{
			name:      "same key different event",
			hamt:      []types.StateEntry{testStateEntry(2, 3, 12)},
			blocks:    []types.StateEntry{member},
			wantErr:   true,
			wantInErr: []string{"1 only in blocks", "1 only in HAMT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := diffStateEntries(42, tt.hamt, tt.blocks)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("expected matching state, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected a mismatch, got nil")
			}
			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}
