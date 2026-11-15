// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
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
