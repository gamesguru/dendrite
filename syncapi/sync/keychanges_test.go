// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Clients read changed and left as arrays, so a response with no changes must not
// carry null for them.
func TestKeyChangesResponseListsAreArrays(t *testing.T) {
	for name, tc := range map[string]struct {
		changed, left []string
		want          string
	}{
		"nothing changed": {want: `{"changed":[],"left":[]}`},
		"only changed":    {changed: []string{"@a:test"}, want: `{"changed":["@a:test"],"left":[]}`},
		"only left":       {left: []string{"@b:test"}, want: `{"changed":[],"left":["@b:test"]}`},
		"both":            {changed: []string{"@a:test"}, left: []string{"@b:test"}, want: `{"changed":["@a:test"],"left":["@b:test"]}`},
	} {
		t.Run(name, func(t *testing.T) {
			resp := keyChangesResponse(tc.changed, tc.left)
			assert.Equal(t, 200, resp.Code)
			body, err := json.Marshal(resp.JSON)
			require.NoError(t, err)
			assert.JSONEq(t, tc.want, string(body))
		})
	}
}
