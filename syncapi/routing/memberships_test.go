// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func TestParseMembershipAtToken(t *testing.T) {
	topologyToken := types.TopologyToken{Depth: 12, PDUPosition: 34}
	streamToken := types.StreamingToken{PDUPosition: 56}

	t.Run("topology token", func(t *testing.T) {
		got, err := parseMembershipAtToken(topologyToken.String())
		require.NoError(t, err)
		require.Equal(t, topologyToken, got)
	})

	t.Run("stream token", func(t *testing.T) {
		got, err := parseMembershipAtToken(streamToken.String())
		require.NoError(t, err)
		require.Equal(t, types.TopologyToken{Depth: math.MaxInt64, PDUPosition: streamToken.PDUPosition}, got)
	})
}
