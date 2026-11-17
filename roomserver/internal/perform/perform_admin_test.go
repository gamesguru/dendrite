// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
)

// TestAdminDuplicateOutcome covers the two results for an event ID that was
// already ingested: a copy with valid signatures is a duplicate, while a copy
// that fails signature verification is reported as an error.
func TestAdminDuplicateOutcome(t *testing.T) {
	tests := []struct {
		name      string
		verifyErr error
		want      string
	}{
		{name: "valid duplicate", want: "duplicate"},
		{name: "bad signature", verifyErr: errors.New("bad signature"), want: "error: signature check failed: bad signature"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := adminDuplicateOutcome(nil, func(gomatrixserverlib.PDU) error {
				return tc.verifyErr
			})
			if got != tc.want {
				t.Fatalf("adminDuplicateOutcome() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAdminRefetchSignatureStatus(t *testing.T) {
	verifyErr := errors.New("signature key unavailable")
	tests := []struct {
		name      string
		version   gomatrixserverlib.RoomVersion
		verifyErr error
		want      string
		wantFatal bool
	}{
		{name: "verified", version: gomatrixserverlib.RoomVersionV2, want: RefetchOutcomeRefetched},
		{name: "v1 failure is fatal", version: gomatrixserverlib.RoomVersionV1, verifyErr: verifyErr, wantFatal: true},
		{name: "v2 failure is fatal", version: gomatrixserverlib.RoomVersionV2, verifyErr: verifyErr, wantFatal: true},
		{name: "modern version can report unverified", version: gomatrixserverlib.RoomVersionV10, verifyErr: verifyErr, want: RefetchOutcomeSigUnverified},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, fatal := adminRefetchSignatureStatus(tc.version, tc.verifyErr)
			if got != tc.want || fatal != tc.wantFatal {
				t.Fatalf("adminRefetchSignatureStatus() = (%q, %t), want (%q, %t)", got, fatal, tc.want, tc.wantFatal)
			}
		})
	}
}

func TestExplicitRefetchServersBoundsLargeLists(t *testing.T) {
	large := make([]spec.ServerName, 100)
	for i := range large {
		large[i] = spec.ServerName(fmt.Sprintf("server-%d.test", i))
	}
	got := explicitRefetchServers("", large, "sender.test")
	want := append(append([]spec.ServerName(nil), large[:maxRefetchServers-1]...), "sender.test")
	if len(got) != len(want) {
		t.Fatalf("got %d candidates, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d = %q, want %q (all %v)", i, got[i], want[i], got)
		}
	}

	duplicates := []spec.ServerName{"a.test", "a.test", "b.test", "b.test"}
	got = explicitRefetchServers("", duplicates, "")
	want = []spec.ServerName{"a.test", "b.test"}
	if len(got) != len(want) {
		t.Fatalf("got %d deduplicated candidates, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d = %q, want %q (all %v)", i, got[i], want[i], got)
		}
	}
}

func TestExplicitRefetchServersReservesSenderSlotOnlyWhenNeeded(t *testing.T) {
	explicit := make([]spec.ServerName, maxRefetchServers)
	for i := range explicit {
		explicit[i] = spec.ServerName(fmt.Sprintf("server-%d.test", i))
	}
	tests := []struct {
		name   string
		first  spec.ServerName
		list   []spec.ServerName
		sender spec.ServerName
		want   []spec.ServerName
	}{
		{
			name:   "sender already first does not drop an explicit candidate",
			first:  "sender.test",
			list:   explicit,
			sender: "sender.test",
			want:   append([]spec.ServerName{"sender.test"}, explicit[:maxRefetchServers-1]...),
		},
		{
			name:   "sender late in explicit list is retained within cap",
			list:   append(append([]spec.ServerName(nil), explicit...), "sender.test"),
			sender: "sender.test",
			want:   append(append([]spec.ServerName(nil), explicit[:maxRefetchServers-1]...), "sender.test"),
		},
		{
			name:   "sender not explicit gets a reserved slot",
			list:   explicit,
			sender: "sender.test",
			want:   append(append([]spec.ServerName(nil), explicit[:maxRefetchServers-1]...), "sender.test"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := explicitRefetchServers(tc.first, tc.list, tc.sender)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d candidates, want %d: %v", len(got), len(tc.want), got)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("candidate %d = %q, want %q (all %v)", i, got[i], tc.want[i], got)
				}
			}
		})
	}
}
