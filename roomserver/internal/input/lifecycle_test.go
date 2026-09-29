package input

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestWorkerRejectedAfterShutdown(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	p.ShutdownZendrite()
	w := &worker{r: &Inputer{ProcessContext: p}}
	w.Lock()
	w.start()
	w.Unlock()
	if w.ctx != nil || w.cancel != nil {
		t.Fatal("rejected worker is marked running")
	}
	p.WaitForComponentsToFinish()
}

func TestRoomWorkerRetriesAfterBrokerDisconnect(t *testing.T) {
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	require.NoError(t, err)
	s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	require.True(t, s.ReadyForConnections(5*time.Second))
	nc, err := nats.Connect(s.ClientURL(), nats.MaxReconnects(-1))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "room", Subjects: []string{"room"}})
	require.NoError(t, err)
	sub, err := js.PullSubscribe("room", "worker")
	require.NoError(t, err)
	p := process.NewProcessContextWithParent(t.Context())
	t.Cleanup(p.ShutdownZendrite)
	w := &worker{
		r: &Inputer{ProcessContext: p, JetStream: js}, roomID: "!room:test",
		subscription: sub, sentryHub: sentry.CurrentHub().Clone(), ch: make(chan struct{}, 1),
		cancel: func(err error) { t.Errorf("broker disconnect retired room worker: %v", err) },
	}
	finished := make(chan struct{})
	go func() { defer close(finished); w._next() }()
	require.Eventually(t, func() bool {
		info, infoErr := js.ConsumerInfo("room", "worker")
		return infoErr == nil && info.NumWaiting == 1
	}, 5*time.Second, 20*time.Millisecond)
	s.Shutdown()
	s.WaitForShutdown()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("room fetch did not return after broker disconnect")
	}
	require.Same(t, sub, w.subscription, "durable subscription must survive disconnect")
	require.Len(t, w.ch, 1, "queued room work must retry without waiting for new activity")
}

// TestWorkerReplacesStoppingWorker covers a room worker that has stopped
// itself, for inactivity, but not yet exited when new room activity arrives.
// The activity must start a fresh worker instead of being left to one that
// exits without processing it.
func TestWorkerReplacesStoppingWorker(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	t.Cleanup(func() { p.ShutdownZendrite(); p.WaitForComponentsToFinish() })
	stopping, stop := context.WithCancelCause(t.Context())
	stop(errors.New("inactivity timeout"))
	w := &worker{r: &Inputer{ProcessContext: p}, roomID: "!room:test", ch: make(chan struct{}, 1), ctx: stopping}
	w.Lock()
	w.start()
	running := w.ctx
	w.Unlock()
	require.NotNil(t, running)
	require.NoError(t, running.Err(), "room activity was handed to a worker that is exiting")
}

// failingConsumerSetup fails the first creations of a room consumer, as a
// replicated JetStream does when the consumer's Raft group elects its leader
// more slowly than the client waits.
type failingConsumerSetup struct {
	nats.JetStreamContext
	failures atomic.Int32
}

func (f *failingConsumerSetup) AddConsumer(stream string, cfg *nats.ConsumerConfig, opts ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	if f.failures.Add(-1) >= 0 {
		return nil, context.DeadlineExceeded
	}
	return f.JetStreamContext.AddConsumer(stream, cfg, opts...)
}

// TestRoomWorkerRetriesConsumerSetup covers a room whose durable consumer
// cannot be created at first. Its queued events were all announced already,
// as createRoom announces every initial event at once and then waits for
// them, so the worker must keep trying on its own rather than wait for room
// activity that never comes.
func TestRoomWorkerRetriesConsumerSetup(t *testing.T) {
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	require.NoError(t, err)
	s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	require.True(t, s.ReadyForConnections(5*time.Second))
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)

	cfg := &config.RoomServer{Matrix: &config.Global{JetStream: config.JetStream{TopicPrefix: "Test"}}}
	stream := cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEvent)
	// As the input stream is: a message lives only while a consumer needs it.
	_, err = js.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}, Retention: nats.InterestPolicy})
	require.NoError(t, err)
	// The stream-wide consumer that announces each event to its room.
	announcer, err := js.PullSubscribe("", "announcer", nats.BindStream(stream), nats.HeadersOnly())
	require.NoError(t, err)
	roomID := "!room:test"
	subject := cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(roomID))
	// Not an input event: the worker terminates it, which is enough to prove
	// that it was fetched without a database behind the worker.
	_, err = js.Publish(subject, []byte("not an input event"))
	require.NoError(t, err)
	announcements, err := announcer.Fetch(1)
	require.NoError(t, err)

	retryDelay := consumerSetupRetryDelay
	consumerSetupRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { consumerSetupRetryDelay = retryDelay })
	flaky := &failingConsumerSetup{JetStreamContext: js}
	flaky.failures.Store(3)
	p := process.NewProcessContextWithParent(t.Context())
	t.Cleanup(func() { p.ShutdownZendrite(); p.WaitForComponentsToFinish() })
	r := &Inputer{Cfg: cfg, ProcessContext: p, JetStream: flaky, InputRoomEventTopic: stream}

	// The only announcement of the room's event.
	r.startWorkerForRoom(roomID, 1, announcements[0])

	consumer := cfg.Matrix.JetStream.Prefixed("RoomInput" + jetstream.Tokenise(roomID))
	require.Eventually(t, func() bool {
		info, infoErr := js.ConsumerInfo(stream, consumer)
		return infoErr == nil && info.Delivered.Stream == 1 && info.NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond, "the queued room event was never fetched")
	require.Eventually(t, func() bool {
		info, infoErr := js.ConsumerInfo(stream, "announcer")
		return infoErr == nil && info.NumAckPending == 0
	}, 5*time.Second, 20*time.Millisecond, "the announcement was never acknowledged")
	require.LessOrEqual(t, flaky.failures.Load(), int32(0), "consumer creation did not fail as arranged")
}
