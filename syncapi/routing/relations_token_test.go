// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full copyright details.

package routing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// A saved backward continuation token keeps the depth it had when issued; a
// late-parent repair may have lifted the event it sits before since then.
func TestParseRelationsTokenReanchorsBackwardTopologyCursor(t *testing.T) {
	ctx := context.Background()
	lifted := anchorSnapshot{anchor: types.TopologyToken{Depth: 9, PDUPosition: 11}}
	saved := types.TopologyToken{Depth: 5, PDUPosition: 10}
	stream := types.StreamingToken{PDUPosition: 20}

	topology, fromStream, err := parseRelationsToken(ctx, lifted, "!r:test", saved.String(), true, true)
	require.NoError(t, err)
	assert.Nil(t, fromStream)
	assert.Equal(t, types.StreamPosition(9), topology.Depth, "bare topology token is re-anchored")
	assert.Equal(t, saved.PDUPosition, topology.PDUPosition)

	combined := saved.String() + "|" + stream.String()
	topology, fromStream, err = parseRelationsToken(ctx, lifted, "!r:test", combined, true, true)
	require.NoError(t, err)
	require.NotNil(t, fromStream)
	assert.Equal(t, types.StreamPosition(9), topology.Depth, "topology|stream token is re-anchored")

	topology, _, err = parseRelationsToken(ctx, lifted, "!r:test", saved.String(), false, false)
	require.NoError(t, err)
	assert.Equal(t, saved.Depth, topology.Depth, "forward tokens are left alone")

	shallower := anchorSnapshot{anchor: types.TopologyToken{Depth: 2, PDUPosition: 11}}
	topology, _, err = parseRelationsToken(ctx, shallower, "!r:test", saved.String(), true, true)
	require.NoError(t, err)
	assert.Equal(t, saved.Depth, topology.Depth, "the depth is never lowered")

	// A forward page's `to` bound is not a cursor: re-anchoring it would raise
	// the upper bound and admit relations beyond the requested end.
	topology, _, err = parseRelationsToken(ctx, lifted, "!r:test", saved.String(), true, false)
	require.NoError(t, err)
	assert.Equal(t, saved.Depth, topology.Depth, "a `to` bound is not re-anchored")
}
