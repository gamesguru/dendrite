package sync

import (
	"reflect"
	"testing"
)

func TestSelectPreviousListWindow(t *testing.T) {
	tests := []struct {
		name        string
		roomIDsJSON string
		exists      bool
		wantIDs     []string
		wantResync  bool
		wantErr     bool
	}{
		{
			name:        "saved window is used",
			roomIDsJSON: `["!a:test","!b:test"]`,
			exists:      true,
			wantIDs:     []string{"!a:test", "!b:test"},
		},
		{
			name:        "empty saved window is not mistaken for a missing one",
			roomIDsJSON: `[]`,
			exists:      true,
			wantIDs:     []string{},
		},
		{
			name:       "missing window forces a resync",
			exists:     false,
			wantResync: true,
		},
		{
			name:        "invalid window reports an error",
			roomIDsJSON: `{`,
			exists:      true,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids, resync, err := selectPreviousListWindow(tc.roomIDsJSON, tc.exists)
			if (err != nil) != tc.wantErr {
				t.Fatalf("selectPreviousListWindow() error = %v, wantErr %v", err, tc.wantErr)
			}
			if resync != tc.wantResync {
				t.Fatalf("selectPreviousListWindow() resync = %v, want %v", resync, tc.wantResync)
			}
			if tc.wantErr {
				return
			}
			if !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Fatalf("selectPreviousListWindow() ids = %v, want %v", ids, tc.wantIDs)
			}
			if tc.wantResync {
				ops := GenerateListOperations(ids, []string{"!room:test"}, []int{0, 0}, 5)
				if len(ops) != 1 || ops[0].Op != "SYNC" {
					t.Fatalf("missing position window operations = %v, want initial SYNC", ops)
				}
			}
		})
	}
}
