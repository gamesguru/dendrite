package consumers

import (
	"encoding/json"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/queue"
	syncTypes "codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// Once an EDU is in the destination queue, the queue owns its delivery. A
// consumer that refused the message anyway would have JetStream redeliver it
// forever, queueing the same EDU again on every attempt.

func isLocalhost(name spec.ServerName) bool { return name == "localhost" }

// disabledQueues accepts every EDU without sending it anywhere.
func disabledQueues() *queue.OutgoingQueues {
	return queue.NewOutgoingQueues(test.NewInMemoryFederationDatabase(), nil, true, "localhost", nil, nil, nil)
}

func TestQueuedReceiptIsAcknowledged(t *testing.T) {
	consumer := &OutputReceiptConsumer{db: test.NewInMemoryFederationDatabase(), queues: disabledQueues(), isLocalServerName: isLocalhost}
	message := &nats.Msg{Header: nats.Header{}}
	message.Header.Set("type", "m.read")
	message.Header.Set("user_id", "@alice:localhost")
	message.Header.Set("room_id", "!room:localhost")
	message.Header.Set("event_id", "$event")
	message.Header.Set("timestamp", "1700000000000")
	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{message}))
}

func TestQueuedSendToDeviceIsAcknowledged(t *testing.T) {
	consumer := &OutputSendToDeviceConsumer{db: test.NewInMemoryFederationDatabase(), queues: disabledQueues(), isLocalServerName: isLocalhost}
	data, err := json.Marshal(syncTypes.OutputSendToDeviceEvent{
		UserID:            "@bob:remote",
		DeviceID:          "DEVICE",
		SendToDeviceEvent: gomatrixserverlib.SendToDeviceEvent{Sender: "@alice:localhost", Type: "m.room_key", Content: json.RawMessage(`{}`)},
	})
	require.NoError(t, err)
	message := &nats.Msg{Header: nats.Header{}, Data: data}
	message.Header.Set("sender", "@alice:localhost")
	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{message}))
}
