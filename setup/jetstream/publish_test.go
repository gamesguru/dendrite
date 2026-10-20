package jetstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// TestPublishRetriesLostAcknowledgement models a broker that fails after
// receiving a publication but before acknowledging it. The first request is
// swallowed; Publish must give up on it within its attempt bound and retry
// under the same message ID rather than wait on the caller's context.
func TestPublishRetriesLostAcknowledgement(t *testing.T) {
	restore := publishAttemptTimeout
	publishAttemptTimeout = 200 * time.Millisecond
	t.Cleanup(func() { publishAttemptTimeout = restore })

	s := storageTestServer(t, "standalone", nil)
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	var mu sync.Mutex
	var ids []string
	_, err = nc.Subscribe("lost", func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		ids = append(ids, msg.Header.Get(nats.MsgIdHdr))
		if len(ids) == 1 {
			return
		}
		_ = msg.Respond([]byte(`{"stream":"lost","seq":1}`))
	})
	require.NoError(t, err)
	js, err := nc.JetStream()
	require.NoError(t, err)

	// The caller's context has no deadline, like an inbound federation request.
	started := time.Now()
	ack, err := Publish(t.Context(), js, &nats.Msg{Subject: "lost", Data: []byte("event")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), ack.Sequence)
	require.Less(t, time.Since(started), 5*time.Second)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, ids, 2)
	require.NotEmpty(t, ids[0])
	require.Equal(t, ids[0], ids[1], "a retry must reuse the message ID so the stream can deduplicate it")
}

// TestPublishDeduplicatesPersistedAttempt checks the other half: when the lost
// attempt was in fact stored, the retry does not store the message again.
func TestPublishDeduplicatesPersistedAttempt(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "dedupe", Subjects: []string{"dedupe"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)
	msg := &nats.Msg{Subject: "dedupe", Data: []byte("event")}
	_, err = Publish(t.Context(), js, msg)
	require.NoError(t, err)
	_, err = Publish(t.Context(), js, msg)
	require.NoError(t, err)
	info, err := js.StreamInfo("dedupe")
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.State.Msgs)
}

// TestPublishReturnsRefusals checks that a refusal, such as the HA ownership
// barrier, is returned rather than retried.
func TestPublishReturnsRefusals(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	owner := &testOwnership{generation: 1}
	owner.lose(errors.New("retired"), true)
	js := Protected(storageTestClient(t, s), owner)
	started := time.Now()
	_, err := Publish(t.Context(), js, &nats.Msg{Subject: "refused", Data: []byte("event")})
	require.Error(t, err)
	require.Less(t, time.Since(started), time.Second)
}

// TestPublishCountsFailures checks that a publication given up on is visible
// in the metrics, labeled by stream and reason, with the per-room suffix of
// the input stream removed.
func TestPublishCountsFailures(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	owner := &testOwnership{generation: 1}
	owner.lose(errors.New("retired"), true)
	js := Protected(storageTestClient(t, s), owner)
	refused := publishFailures.WithLabelValues("PrefixInputRoomEvent", "refused")
	before := counterValue(refused)
	_, err := Publish(t.Context(), js, &nats.Msg{Subject: "PrefixInputRoomEvent._room_server", Data: []byte("event")})
	require.Error(t, err)
	require.Equal(t, before+1, counterValue(refused))

	// The caller stops waiting while the stream cannot answer.
	ctx, cancel := context.WithCancelCause(t.Context())
	canceled := publishFailures.WithLabelValues("missing", "canceled")
	retries := publishRetries.WithLabelValues("missing")
	before, retriedBefore := counterValue(canceled), counterValue(retries)
	time.AfterFunc(time.Second, func() { cancel(errors.New("the caller gave up")) })
	_, err = Publish(ctx, storageTestClient(t, s), &nats.Msg{Subject: "missing", Data: []byte("event")})
	require.Error(t, err)
	require.Equal(t, before+1, counterValue(canceled))
	require.Greater(t, counterValue(retries), retriedBefore)
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		panic(err)
	}
	return m.GetCounter().GetValue()
}
