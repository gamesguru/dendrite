// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
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

func TestInterruptListsRequireRoomData(t *testing.T) {
	index := 0
	resp := &types.SlidingSyncResponse{
		Lists: map[string]types.SlidingList{
			"list": {
				Ops: []types.SlidingOperation{
					{Op: "INSERT", Index: &index, RoomIDs: []string{"!present:test", "!missing:test"}},
					{Op: "DELETE", Index: &index},
				},
			},
		},
		Rooms: map[string]types.SlidingRoomData{"!present:test": {}},
	}
	require.False(t, interruptListsHaveRoomData(resp))

	resp.Rooms["!missing:test"] = types.SlidingRoomData{}
	require.True(t, interruptListsHaveRoomData(resp))
}

// acknowledgeDB stubs the position and window writes acknowledgeInterruptLists
// makes before it carries room state forward; the per-room writes go to the
// embedded cancellingWriteDB.
type acknowledgeDB struct {
	*cancellingWriteDB
}

func (acknowledgeDB) CreateConnectionPosition(context.Context, int64) (int64, error) { return 2, nil }

func (acknowledgeDB) UpdateConnectionListWindow(context.Context, int64, string, int64, string) error {
	return nil
}

func (acknowledgeDB) GetOrCreateRequiredStateID(context.Context, int64, string) (int64, error) {
	return 1, nil
}

// A timeout response that sent list operations is only acknowledged when the
// state of every room the client was sent before has been carried onto the new
// position. Otherwise the old position is deleted and those rooms look like
// they were never sent.
func TestAcknowledgeInterruptListsRequiresCopyForward(t *testing.T) {
	index := 0
	newResp := func() *types.SlidingSyncResponse {
		return &types.SlidingSyncResponse{
			Lists: map[string]types.SlidingList{
				"list": {Ops: []types.SlidingOperation{{Op: "INSERT", Index: &index, RoomIDs: []string{"!sent:test"}}}},
			},
			Rooms: map[string]types.SlidingRoomData{"!sent:test": {}},
		}
	}
	since := &types.SlidingSyncStreamToken{ConnectionPosition: 1, StreamToken: types.StreamingToken{PDUPosition: 100}}

	t.Run("copy-forward failure drops the operations", func(t *testing.T) {
		resp := newResp()
		rp := &RequestPool{db: acknowledgeDB{&cancellingWriteDB{failRoom: "!room3:localhost"}}}

		rp.acknowledgeInterruptLists(context.Background(), resp, persistedConnState(5), since, nil)

		require.Empty(t, resp.Lists["list"].Ops)
		require.Empty(t, resp.Pos, "the response must keep the position the client holds")
	})

	t.Run("successful copy-forward acknowledges", func(t *testing.T) {
		resp := newResp()
		rp := &RequestPool{db: acknowledgeDB{&cancellingWriteDB{}}}

		rp.acknowledgeInterruptLists(context.Background(), resp, persistedConnState(5), since, nil)

		require.NotEmpty(t, resp.Lists["list"].Ops)
		require.NotEmpty(t, resp.Pos)
	})
}
