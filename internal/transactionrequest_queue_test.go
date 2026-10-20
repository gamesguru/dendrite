package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// refusingJetStream refuses every publication, as a broker or ownership
// barrier that rejects it does.
type refusingJetStream struct {
	nats.JetStreamContext
	mu       sync.Mutex
	subjects []string
}

func (r *refusingJetStream) PublishMsg(m *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjects = append(r.subjects, m.Subject)
	return nil, errors.New("refused")
}

func refusedTransaction(t *testing.T, edus []gomatrixserverlib.EDU) (TxnReq, *refusingJetStream) {
	t.Helper()
	ctx := process.NewProcessContext()
	t.Cleanup(ctx.ShutdownZendrite)
	txn, _, _ := createTransactionWithEDU(ctx, edus)
	js := &refusingJetStream{}
	txn.producer.JetStream = js
	return txn, js
}

func receiptEDU(t *testing.T) gomatrixserverlib.EDU {
	t.Helper()
	content, err := json.Marshal(map[string]any{
		"!room:kaer.morhen": map[string]any{"m.read": map[string]any{
			"@john:kaer.morhen": map[string]any{"data": map[string]any{"ts": 1}, "event_ids": []string{"$read:kaer.morhen"}},
		}},
	})
	require.NoError(t, err)
	return gomatrixserverlib.EDU{Type: "m.receipt", Content: content}
}

func toDeviceEDU(t *testing.T, messageID string) gomatrixserverlib.EDU {
	t.Helper()
	content, err := json.Marshal(map[string]any{
		"sender": "@john:kaer.morhen", "type": "m.room_key_request", "message_id": messageID,
		"messages": map[string]any{"@alice:ourserver": map[string]any{"DEVICE": map[string]any{"k": "v"}}},
	})
	require.NoError(t, err)
	return gomatrixserverlib.EDU{Type: "m.direct_to_device", Content: content}
}

// TestTransactionRefusedWhenEDUCannotBeQueued checks that a receipt or
// to-device message the broker did not take fails the transaction, so that
// its sender retries it, instead of being dropped behind a 200.
func TestTransactionRefusedWhenEDUCannotBeQueued(t *testing.T) {
	for name, edu := range map[string]gomatrixserverlib.EDU{"receipt": receiptEDU(t), "to-device": toDeviceEDU(t, "m1")} {
		t.Run(name, func(t *testing.T) {
			txn, js := refusedTransaction(t, []gomatrixserverlib.EDU{edu})
			res, jsonRes := txn.ProcessTransaction(t.Context())
			require.Nil(t, res)
			require.NotNil(t, jsonRes)
			require.Equal(t, http.StatusServiceUnavailable, jsonRes.Code)
			require.NotEmpty(t, js.subjects)
		})
	}
}

// TestTransactionAcceptsLostTyping checks that typing notifications stay best
// effort: one that cannot be queued does not make the sender resend the
// transaction.
func TestTransactionAcceptsLostTyping(t *testing.T) {
	content, err := json.Marshal(map[string]any{"room_id": "!room:kaer.morhen", "user_id": "@john:kaer.morhen", "typing": true})
	require.NoError(t, err)
	txn, js := refusedTransaction(t, []gomatrixserverlib.EDU{{Type: "m.typing", Content: content}})
	res, jsonRes := txn.ProcessTransaction(t.Context())
	require.Nil(t, jsonRes)
	require.NotNil(t, res)
	require.Len(t, js.subjects, 1)
}

// queueFailingRsAPI cannot queue events into the roomserver input stream.
type queueFailingRsAPI struct{ FakeRsAPI }

func (r *queueFailingRsAPI) InputRoomEvents(_ context.Context, _ *rsAPI.InputRoomEventsRequest, res *rsAPI.InputRoomEventsResponse) {
	res.ErrMsg = "r.JetStream.PublishMsg: context canceled"
}

// TestTransactionRefusedWhenEventCannotBeQueued checks that an event the
// input stream did not take fails the transaction rather than being reported
// as a per-event error, which senders do not retry.
func TestTransactionRefusedWhenEventCannotBeQueued(t *testing.T) {
	txn := NewTxnReq(&queueFailingRsAPI{}, nil, "ourserver", &test.NopJSONVerifier{}, nil, nil, false, []json.RawMessage{testEvent}, nil, "", "", "")
	res, jsonRes := txn.ProcessTransaction(t.Context())
	require.Nil(t, res)
	require.NotNil(t, jsonRes)
	require.Equal(t, http.StatusServiceUnavailable, jsonRes.Code)
}

// TestRetriedToDeviceIsStoredOnce processes the same to-device message twice,
// as a sender retrying a refused transaction does, and checks that the stream
// holds it once.
func TestRetriedToDeviceIsStoredOnce(t *testing.T) {
	ctx := process.NewProcessContext()
	t.Cleanup(ctx.ShutdownZendrite)
	txn, js, cfg := createTransactionWithEDU(ctx, []gomatrixserverlib.EDU{toDeviceEDU(t, "retried")})
	stream := cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent)
	// Interest streams keep messages only for consumers that exist.
	_, err := js.AddConsumer(stream, &nats.ConsumerConfig{Durable: "RetriedToDevice", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)
	before, err := js.StreamInfo(stream)
	require.NoError(t, err)
	for range 2 {
		_, jsonRes := txn.ProcessTransaction(t.Context())
		require.Nil(t, jsonRes)
	}
	other := txn
	other.EDUs = []gomatrixserverlib.EDU{toDeviceEDU(t, "another")}
	_, jsonRes := other.ProcessTransaction(t.Context())
	require.Nil(t, jsonRes)
	after, err := js.StreamInfo(stream)
	require.NoError(t, err)
	require.Equal(t, uint64(2), after.State.LastSeq-before.State.LastSeq, "one message per distinct message ID")
}
