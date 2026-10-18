package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/storage/shared"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

type threadedReceiptDatabase struct {
	storage.Database
	snapshot *threadedReceiptSnapshot
}

func (d threadedReceiptDatabase) NewDatabaseSnapshot(context.Context) (*shared.DatabaseTransaction, error) {
	return &shared.DatabaseTransaction{Database: &shared.Database{Receipts: d.snapshot}}, nil
}

type threadedReceiptSnapshot struct {
	tables.Receipts
	pending []types.OutputReceiptEvent
}

func (s *threadedReceiptSnapshot) SelectLatestUserReceiptsForConnection(context.Context, *sql.Tx, int64, []string, string) ([]types.OutputReceiptEvent, error) {
	return s.pending, nil
}

func TestSlidingReceiptThreadCollision(t *testing.T) {
	snapshot := &threadedReceiptSnapshot{pending: []types.OutputReceiptEvent{
		{RoomID: "!room:test", EventID: "$same", UserID: "@alice:test", Type: "m.read", Timestamp: 1},
		{RoomID: "!room:test", EventID: "$same", UserID: "@alice:test", Type: "m.read", ThreadID: "main", Timestamp: 2},
		{RoomID: "!room:test", EventID: "$reply", UserID: "@bob:test", Type: "m.read.private", ThreadID: "$root", Timestamp: 3},
	}}
	pool := RequestPool{db: threadedReceiptDatabase{snapshot: snapshot}}
	for _, wantThread := range []string{"", "main"} {
		response, _, delivered, err := pool.processReceiptsExtension(t.Context(), nil, 1, "@alice:test", nil, []string{"!room:test"}, nil, types.StreamingToken{}, nil, map[string]bool{"!room:test": true})
		require.NoError(t, err)
		require.Len(t, delivered, 1)
		require.Equal(t, wantThread, delivered[0].ThreadID)
		var content map[string]map[string]map[string]ReceiptTS
		require.NoError(t, json.Unmarshal(response.Rooms["!room:test"].Content, &content))
		require.Equal(t, wantThread, content["$same"]["m.read"]["@alice:test"].ThreadID)
		require.NotContains(t, content, "$reply")
		for i, receipt := range snapshot.pending {
			if receipt == delivered[0] {
				snapshot.pending = append(snapshot.pending[:i], snapshot.pending[i+1:]...)
				break
			}
		}
	}
}
