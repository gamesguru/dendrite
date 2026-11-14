package shared_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage/shared"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

type lateParentExtremities struct {
	tables.BackwardsExtremities
	children []string
}

func (f *lateParentExtremities) SelectEventsReferencingPrevEvent(
	context.Context, *sql.Tx, string, string,
) ([]string, error) {
	return f.children, nil
}

type lateParentTopology struct {
	tables.Topology
	positions map[string]types.TopologyToken
	fullReads int
}

func (f *lateParentTopology) SelectPositionsInTopology(
	_ context.Context, _ *sql.Tx, ids []string,
) (map[string]types.TopologyToken, error) {
	out := map[string]types.TopologyToken{}
	for _, id := range ids {
		if pos, ok := f.positions[id]; ok {
			out[id] = pos
		}
	}
	return out, nil
}

func (f *lateParentTopology) SelectRoomEventTopology(
	context.Context, *sql.Tx, string, types.StreamPosition,
) ([]tables.StoredEventTopology, error) {
	f.fullReads++
	return nil, nil
}

// The room's history is only read when the late parent moves a child.
func TestRepairLateParentTopologyReadsHistoryOnlyWhenAChildMoves(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	parent := room.Events()[0]
	child := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "child"})

	run := func(parentDepth, childDepth types.StreamPosition) *lateParentTopology {
		topology := &lateParentTopology{positions: map[string]types.TopologyToken{
			parent.EventID(): {Depth: parentDepth, PDUPosition: 1},
			child.EventID():  {Depth: childDepth, PDUPosition: 2},
		}}
		db := &shared.Database{
			Topology:            topology,
			BackwardExtremities: &lateParentExtremities{children: []string{child.EventID()}},
		}
		require.NoError(t, shared.RepairLateParentTopology(db, context.Background(), nil, parent))
		return topology
	}

	assert.Zero(t, run(3, 4).fullReads, "child already above the parent: nothing to repair")
	assert.Equal(t, 1, run(4, 4).fullReads, "child at the parent's position moves")
	assert.Equal(t, 1, run(5, 4).fullReads, "child below the parent moves")
}
