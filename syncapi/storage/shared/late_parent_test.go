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
	stored    []tables.StoredEventTopology
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
	return f.stored, nil
}

func (f *lateParentTopology) UpdateEventPosition(
	_ context.Context, _ *sql.Tx, eventID string, position types.StreamPosition,
) error {
	token := f.positions[eventID]
	token.Depth = position
	f.positions[eventID] = token
	return nil
}

// A legacy descendant can be below its direct parent even when the direct
// child is already above the newly arrived parent. The full branch still needs
// traversing so that descendant is repaired.
func TestRepairLateParentTopologyRepairsMisorderedGrandchild(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	parent := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "late parent"}, test.WithDepth(10))
	child := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "child"},
		test.WithDepth(20), test.WithPrevEvents(parent.EventID()))
	grandchild := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "grandchild"},
		test.WithDepth(1), test.WithPrevEvents(child.EventID()))
	topology := &lateParentTopology{
		positions: map[string]types.TopologyToken{
			parent.EventID():     {Depth: 10, PDUPosition: 1},
			child.EventID():      {Depth: 20, PDUPosition: 2},
			grandchild.EventID(): {Depth: 1, PDUPosition: 3},
		},
		stored: []tables.StoredEventTopology{
			{Event: parent, Position: 10},
			{Event: child, Position: 20},
			{Event: grandchild, Position: 1},
		},
	}
	db := &shared.Database{
		Topology:            topology,
		BackwardExtremities: &lateParentExtremities{children: []string{child.EventID()}},
	}
	require.NoError(t, shared.RepairLateParentTopology(db, context.Background(), nil, parent))
	assert.Equal(t, types.StreamPosition(20), topology.positions[child.EventID()].Depth,
		"the direct child already followed the late parent")
	assert.Equal(t, types.StreamPosition(21), topology.positions[grandchild.EventID()].Depth,
		"the legacy grandchild must still be lifted above its parent")
}
