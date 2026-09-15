package jetstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// testOwnership is read by consumer goroutines while a test mutates it, so its
// state is guarded rather than written directly.
type testOwnership struct {
	mu         sync.Mutex
	generation int64
	identity   string
	err        error
	retired    bool
}

func (o *testOwnership) Generation() int64 { return o.generation }
func (o *testOwnership) Identity() string  { return o.identity }

func (o *testOwnership) Valid() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}

func (o *testOwnership) Retired() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retired
}

func (o *testOwnership) lose(err error, retired bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err, o.retired = err, retired
}

func TestProtectedPublishStampsTheGeneration(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "owned", Subjects: []string{"owned"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)

	owner := &testOwnership{generation: 7, identity: "candidate-a"}
	protected := protectedJetStream{JetStreamContext: js, ownership: owner}
	_, err = protected.PublishMsg(&nats.Msg{Subject: "owned", Data: []byte("event")})
	require.NoError(t, err)

	sub, err := js.PullSubscribe("owned", "reader")
	require.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	require.NoError(t, err)
	require.Equal(t, "7", msgs[0].Header.Get(GenerationHeader))
}

func TestProtectedPublishRefusesARetiredOwner(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "owned", Subjects: []string{"owned"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)

	retired := errors.New("ownership generation is retired")
	protected := protectedJetStream{JetStreamContext: js, ownership: &testOwnership{generation: 7, err: retired}}
	_, err = protected.PublishMsg(&nats.Msg{Subject: "owned", Data: []byte("stale")})
	require.ErrorIs(t, err, retired)

	info, err := js.StreamInfo("owned")
	require.NoError(t, err)
	require.Zero(t, info.State.Msgs, "a retired owner's message must never reach the stream")
}

func TestHAConnectionIsNamedAfterItsActivation(t *testing.T) {
	cfg := &config.JetStream{Matrix: &config.Global{}}
	cfg.Matrix.HA.Enabled = true
	owner := &testOwnership{generation: 7, identity: "candidate-a"}

	opts, err := natsConnectOptions(nil, cfg, nil, owner)
	require.NoError(t, err)
	var applied nats.Options
	for _, opt := range opts {
		require.NoError(t, opt(&applied))
	}
	require.Contains(t, applied.Name, "candidate-a", "the connection must identify its activation")
}

// TestConsumerDoesNotAcknowledgeAfterOwnershipLoss covers ADR 0003's
// acknowledgement rule. A retired owner that acknowledges work it did not
// complete would discard it, so the consumer must stop instead, leaving the
// message pending for the promoted activation.
func TestConsumerDoesNotAcknowledgeAfterOwnershipLoss(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "owned", Subjects: []string{"owned"}, Storage: nats.MemoryStorage, Retention: nats.LimitsPolicy})
	require.NoError(t, err)
	_, err = js.Publish("owned", []byte("work"))
	require.NoError(t, err)

	retired := errors.New("ownership generation is retired")
	owner := &testOwnership{generation: 7, identity: "candidate-a"}
	protected := protectedJetStream{JetStreamContext: js, ownership: owner}

	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(errors.New("test finished")) })
	delivered := make(chan struct{}, 1)
	require.NoError(t, JetStreamConsumer(ctx, protected, "owned", "worker", 1,
		func(context.Context, []*nats.Msg) bool {
			// Ownership is lost between doing the work and acknowledging it.
			owner.lose(retired, true)
			select {
			case delivered <- struct{}{}:
			default:
			}
			return true
		}))

	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer never received the message")
	}

	require.Eventually(t, func() bool {
		info, infoErr := js.ConsumerInfo("owned", "workerPull")
		return infoErr == nil && info.AckFloor.Consumer == 0 && info.NumAckPending == 1
	}, 5*time.Second, 50*time.Millisecond, "a retired owner must leave the message unacknowledged")
}

// TestTransientStalenessDoesNotStopAConsumer covers a database blip longer than
// the freshness window. Ownership is unconfirmed, not lost, so the consumer must
// wait rather than exit: a worker that returns is never restarted, leaving a
// leader that still holds the Lease and serves HTTP but consumes nothing for the
// rest of the process lifetime.
func TestTransientStalenessDoesNotStopAConsumer(t *testing.T) {
	owner := &testOwnership{generation: 7, err: errors.New("not confirmed within the freshness window")}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	recovered := make(chan error, 1)
	go func() { recovered <- AwaitOwnership(ctx, owner) }()
	select {
	case <-recovered:
		t.Fatal("a stale consumer must wait instead of giving up")
	case <-time.After(200 * time.Millisecond):
	}

	owner.lose(nil, false)
	require.NoError(t, <-recovered, "the consumer must resume once ownership is confirmed again")
}

// TestRetirementStopsAConsumer covers the terminal case, which must not be
// retried: acknowledging work a retired owner did not complete would discard it.
func TestRetirementStopsAConsumer(t *testing.T) {
	retired := errors.New("ownership generation is retired")
	owner := &testOwnership{generation: 7, err: retired, retired: true}
	require.ErrorIs(t, AwaitOwnership(t.Context(), owner), retired)
}

func TestHAConnectionKeepsItsReconnectBuffer(t *testing.T) {
	cfg := &config.JetStream{Matrix: &config.Global{}}
	cfg.Matrix.HA.Enabled = true
	opts, err := natsConnectOptions(nil, cfg, nil, &testOwnership{generation: 7, identity: "candidate-a"})
	require.NoError(t, err)
	applied := nats.GetDefaultOptions()
	for _, opt := range opts {
		require.NoError(t, opt(&applied))
	}
	require.Positive(t, applied.ReconnectBufSize,
		"disabling the buffer drops output events for already committed writes on every reconnect")
}
