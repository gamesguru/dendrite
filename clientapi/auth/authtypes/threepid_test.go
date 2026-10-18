// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package authtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanonicalThreePIDAddress(t *testing.T) {
	for _, tc := range []struct {
		medium, address, want string
	}{
		{"email", "bob@example.com", "bob@example.com"},
		{"email", "Bob@Example.COM", "bob@example.com"},
		// The example of the spec's 3PID types appendix.
		{"email", "Strauß@Example.com", "strauss@example.com"},
		{"email", "  ΣΊΣΥΦΟΣ@example.com ", "σίσυφοσ@example.com"},
		{"msisdn", "Bob@Example.COM", "Bob@Example.COM"},
	} {
		assert.Equal(t, tc.want, CanonicalThreePIDAddress(tc.medium, tc.address), tc.address)
	}
}
