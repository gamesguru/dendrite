package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// orderRecordingClient records the bodies of the PDUs it is asked to send, in
// transaction order.
type orderRecordingClient struct {
	fclient.FederationClient
	mu     sync.Mutex
	bodies []string
}

func (c *orderRecordingClient) SendTransaction(_ context.Context, t gomatrixserverlib.Transaction) (fclient.RespSend, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, raw := range t.PDUs {
		var pdu struct {
			Content struct {
				Body string `json:"body"`
			} `json:"content"`
		}
		if err := json.Unmarshal(raw, &pdu); err != nil {
			return fclient.RespSend{}, err
		}
		c.bodies = append(c.bodies, pdu.Content.Body)
	}
	return fclient.RespSend{}, nil
}

func (c *orderRecordingClient) sent() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.bodies...)
}

func orderedPDU(t *testing.T, body string) *types.HeaderedEvent {
	t.Helper()
	content := fmt.Sprintf(`{"type":"m.room.message","room_id":"!room:a","content":{"body":%q}}`, body)
	ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON([]byte(content), false)
	require.NoError(t, err)
	return &types.HeaderedEvent{PDU: ev}
}

// TestQueuedBacklogIsSentInOrder models a process that starts with PDUs a
// previous process queued but never delivered, then queues a new one. The
// receiver must see them in the order they were queued: a new event sent
// before the events it references can be rejected for missing prev_events.
func TestQueuedBacklogIsSentInOrder(t *testing.T) {
	db := test.NewInMemoryFederationDatabase()
	pc := process.NewProcessContext()
	t.Cleanup(func() {
		pc.ShutdownZendrite()
		<-pc.WaitForShutdown()
	})
	destination := spec.ServerName("remotehost")
	var want []string
	for i := range 5 {
		body := fmt.Sprintf("backlog-%d", i)
		want = append(want, body)
		encoded, err := json.Marshal(orderedPDU(t, body))
		require.NoError(t, err)
		nid, err := db.StoreJSON(pc.Context(), string(encoded))
		require.NoError(t, err)
		require.NoError(t, db.AssociatePDUWithDestinations(pc.Context(), map[spec.ServerName]struct{}{destination: {}}, nid))
	}

	client := &orderRecordingClient{}
	stats := statistics.NewStatistics(db, 16, 17, false)
	signing := []*fclient.SigningIdentity{{KeyID: "ed21019:auto", PrivateKey: test.PrivateKeyA, ServerName: "localhost"}}
	queues := NewOutgoingQueues(db, pc, false, "localhost", client, &stats, signing)
	require.NoError(t, queues.SendEvent(orderedPDU(t, "live"), "localhost", []spec.ServerName{destination}))
	want = append(want, "live")

	require.Eventually(t, func() bool { return len(client.sent()) == len(want) }, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, want, client.sent())
}
