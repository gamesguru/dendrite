// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
)

// While a room only has partial state, this server can let a user into a public room
// itself, but any other join rule needs members or invites it may not know yet.
func TestJoinRulePermitsLocalJoinDuringResync(t *testing.T) {
	for rule, want := range map[string]bool{
		spec.Public:          true,
		spec.Invite:          false,
		spec.Knock:           false,
		spec.Restricted:      false,
		spec.KnockRestricted: false,
		"":                   false,
		"some_future_rule":   false,
	} {
		t.Run(rule, func(t *testing.T) {
			assert.Equal(t, want, joinRulePermitsLocalJoinDuringResync(rule))
		})
	}
}
