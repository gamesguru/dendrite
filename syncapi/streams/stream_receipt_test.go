package streams

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type receiptSnapshot struct {
	storage.DatabaseTransaction
	receipts []types.OutputReceiptEvent
}

func (s receiptSnapshot) RoomReceiptsAfter(context.Context, []string, types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	return 10, s.receipts, nil
}

func TestReceiptSyncPreservesThreadAndPrivacy(t *testing.T) {
	snapshot := receiptSnapshot{receipts: []types.OutputReceiptEvent{
		{RoomID: "!room:test", UserID: "@alice:test", EventID: "$same", Type: "m.read", ThreadID: "", Timestamp: 1},
		{RoomID: "!room:test", UserID: "@alice:test", EventID: "$same", Type: "m.read", ThreadID: "main", Timestamp: 2},
		{RoomID: "!room:test", UserID: "@alice:test", EventID: "$reply", Type: "m.read.private", ThreadID: "$root", Timestamp: 3},
		{RoomID: "!room:test", UserID: "@bob:test", EventID: "$other", Type: "m.read.private", ThreadID: "$root", Timestamp: 4},
	}}
	for _, user := range []string{"@alice:test", "@bob:test"} {
		t.Run(user, func(t *testing.T) {
			req := &types.SyncRequest{Device: &userapi.Device{UserID: user}, Rooms: map[string]string{"!room:test": "join"}, Response: types.NewResponse()}
			provider := &ReceiptStreamProvider{}
			require.Equal(t, types.StreamPosition(10), provider.IncrementalSync(t.Context(), snapshot, req, 0, 10))
			events := req.Response.Rooms.Join["!room:test"].Ephemeral.Events
			require.Len(t, events, 3)
			found := map[string]int{}
			for _, event := range events {
				var content map[string]map[string]map[string]ReceiptTS
				require.NoError(t, json.Unmarshal(event.Content, &content))
				for _, receiptTypes := range content {
					for receiptType, users := range receiptTypes {
						for userID, receipt := range users {
							if receiptType == "m.read.private" {
								require.Equal(t, user, userID)
							}
							found[receipt.ThreadID]++
						}
					}
				}
			}
			require.Equal(t, map[string]int{"": 1, "main": 1, "$root": 1}, found)
		})
	}
}
