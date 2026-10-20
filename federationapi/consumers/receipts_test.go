package consumers

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestPrivateThreadedReceiptsAreNotFederated(t *testing.T) {
	for _, receiptType := range []string{"m.read.private", "m.fully_read"} {
		t.Run(receiptType, func(t *testing.T) {
			consumer := &OutputReceiptConsumer{}
			message := &nats.Msg{Header: nats.Header{}}
			message.Header.Set("type", receiptType)
			message.Header.Set("thread_id", "$root")
			// No federation database or queue is configured: private receipts must exit
			// before consulting either dependency.
			require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{message}))
		})
	}
}
