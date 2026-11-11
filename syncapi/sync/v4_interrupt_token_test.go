// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func TestInterruptStreamTokenKeepsLaggingRoomToken(t *testing.T) {
	since := &types.SlidingSyncStreamToken{
		ConnectionPosition: 42,
		StreamToken:        types.StreamingToken{PDUPosition: 100},
	}
	behind := types.StreamingToken{PDUPosition: 80}.String()
	ahead := types.StreamingToken{PDUPosition: 120}.String()

	connState := &V4ConnectionState{
		PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
			"!behind:test":  {"events": {LastToken: behind}},
			"!ahead:test":   {"events": {LastToken: ahead}},
			"!invalid:test": {"events": {LastToken: "not-a-token"}},
			"!empty:test":   {"events": {}},
		},
	}

	require.Equal(t, behind, interruptStreamToken(connState, "!behind:test", since))
	require.Equal(t, since.StreamToken.String(), interruptStreamToken(connState, "!ahead:test", since))
	require.Equal(t, since.StreamToken.String(), interruptStreamToken(connState, "!invalid:test", since))
	require.Equal(t, since.StreamToken.String(), interruptStreamToken(connState, "!empty:test", since))
	require.Equal(t, since.StreamToken.String(), interruptStreamToken(connState, "!new:test", since))
	require.Equal(t, since.StreamToken.String(), interruptStreamToken(nil, "!new:test", since))
}
