package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// chunkRecordingDB serves one single-entry block per snapshot and records the
// size of every query, so a test can check no query binds too many NIDs.
type chunkRecordingDB struct {
	StateResolutionStorage
	blockNIDQueries []int
	entryQueries    []int
}

func (d *chunkRecordingDB) StateBlockNIDs(_ context.Context, nids []types.StateSnapshotNID) ([]types.StateBlockNIDList, error) {
	d.blockNIDQueries = append(d.blockNIDQueries, len(nids))
	out := make([]types.StateBlockNIDList, 0, len(nids))
	for _, nid := range nids {
		out = append(out, types.StateBlockNIDList{
			StateSnapshotNID: nid,
			StateBlockNIDs:   []types.StateBlockNID{types.StateBlockNID(nid)},
		})
	}
	return out, nil
}

func (d *chunkRecordingDB) StateEntries(_ context.Context, blocks []types.StateBlockNID) ([]types.StateEntryList, error) {
	d.entryQueries = append(d.entryQueries, len(blocks))
	out := make([]types.StateEntryList, 0, len(blocks))
	for _, b := range blocks {
		out = append(out, types.StateEntryList{
			StateBlockNID: b,
			StateEntries: []types.StateEntry{{
				StateKeyTuple: types.StateKeyTuple{EventTypeNID: 1, EventStateKeyNID: types.EventStateKeyNID(b)},
				EventNID:      types.EventNID(b),
			}},
		})
	}
	return out, nil
}

func TestLoadLegacyStatesBatchedChunksQueries(t *testing.T) {
	const snapshots = 3*legacyStateQueryChunk + 7
	nids := make([]types.StateSnapshotNID, snapshots)
	for i := range nids {
		nids[i] = types.StateSnapshotNID(i + 1)
	}
	db := &chunkRecordingDB{}
	v := &StateResolution{db: db}

	got, err := v.loadLegacyStatesBatched(context.Background(), nids, nil)
	require.NoError(t, err)
	require.Len(t, got, snapshots)
	for _, nid := range nids {
		require.Len(t, got[nid], 1)
		require.Equal(t, types.EventNID(nid), got[nid][0].EventNID)
	}
	for _, n := range append(append([]int{}, db.blockNIDQueries...), db.entryQueries...) {
		require.LessOrEqual(t, n, legacyStateQueryChunk, "a query bound too many NIDs")
	}
	require.Len(t, db.blockNIDQueries, 4, "one StateBlockNIDs query per chunk")
	require.Len(t, db.entryQueries, 4, "one StateEntries query per chunk")
}
