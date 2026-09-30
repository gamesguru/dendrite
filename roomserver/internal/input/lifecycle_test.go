package input

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
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

// cuttableProxy forwards TCP connections to a NATS server and can cut them,
// refusing new ones until restored, as a network outage would.
type cuttableProxy struct {
	mu    sync.Mutex
	ln    net.Listener
	conns []net.Conn
	down  bool
}

func newCuttableProxy(t *testing.T, target string) *cuttableProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &cuttableProxy{ln: ln}
	t.Cleanup(func() { _ = ln.Close(); p.cut(true) })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			upstream, err := net.Dial("tcp", target)
			if p.down || err != nil {
				p.mu.Unlock()
				_ = client.Close()
				continue
			}
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
			go func() { _, _ = io.Copy(client, upstream); _ = client.Close() }()
		}
	}()
	return p
}

func (p *cuttableProxy) cut(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, c := range p.conns {
			_ = c.Close()
		}
		p.conns = nil
	}
}

// startAnnouncedInput runs an Inputer's stream-wide announcements over the
// proxy against an interest-retention input stream, as the roomserver does.
// The caller starts watchAnnouncements.
func startAnnouncedInput(t *testing.T) (r *Inputer, sub *nats.Subscription, proxy *cuttableProxy, admin nats.JetStreamContext) {
	s, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true,
	})
	require.NoError(t, err)
	s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	require.True(t, s.ReadyForConnections(5*time.Second))
	adminConn, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(adminConn.Close)
	admin, err = adminConn.JetStream()
	require.NoError(t, err)

	proxy = newCuttableProxy(t, s.Addr().String())
	nc, err := nats.Connect("nats://"+proxy.ln.Addr().String(), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)

	cfg := &config.RoomServer{Matrix: &config.Global{JetStream: config.JetStream{TopicPrefix: "Test"}}}
	stream := cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEvent)
	_, err = admin.AddStream(&nats.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}, Retention: nats.InterestPolicy})
	require.NoError(t, err)
	p := process.NewProcessContextWithParent(t.Context())
	t.Cleanup(func() { p.ShutdownZendrite(); p.WaitForComponentsToFinish() })
	r = &Inputer{Cfg: cfg, ProcessContext: p, JetStream: js, InputRoomEventTopic: stream}
	sub, err = r.subscribeAnnouncements()
	require.NoError(t, err)
	return r, sub, proxy, admin
}

// requireRoomInputProcessed queues an event for a room and waits for the
// room's worker to fetch it.
func requireRoomInputProcessed(t *testing.T, r *Inputer, admin nats.JetStreamContext, roomID string) {
	seq := queueRoomInput(t, r, admin, roomID)
	requireRoomFetched(t, r, admin, roomID, seq, "the new room's event was never processed")
}

// queueRoomInput publishes an event for a room to the input stream. It is not
// an input event: the worker terminates it, which is enough to prove that it
// was fetched without a database behind the worker.
func queueRoomInput(t *testing.T, r *Inputer, admin nats.JetStreamContext, roomID string) uint64 {
	msg := nats.NewMsg(r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(roomID)))
	msg.Header.Set(jetstream.RoomID, roomID)
	msg.Data = []byte("not an input event")
	ack, err := admin.PublishMsg(msg)
	require.NoError(t, err)
	return ack.Sequence
}

// requireRoomFetched waits for the room's worker to have fetched and finished
// the event at seq.
func requireRoomFetched(t *testing.T, r *Inputer, admin nats.JetStreamContext, roomID string, seq uint64, msg string) {
	consumer := r.Cfg.Matrix.JetStream.Prefixed("RoomInput" + jetstream.Tokenise(roomID))
	require.Eventually(t, func() bool {
		info, infoErr := admin.ConsumerInfo(r.InputRoomEventTopic, consumer)
		return infoErr == nil && info.Delivered.Stream == seq && info.NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond, msg)
}

// TestAnnouncementsSurviveDisconnect covers a broker outage longer than the
// five seconds the broker keeps an ephemeral consumer without a listener by
// default. Had the stream-wide consumer been deleted, an event for a new room
// published afterwards would have had no consumer interested in it and been
// discarded, and a synchronous input would have waited for it forever.
func TestAnnouncementsSurviveDisconnect(t *testing.T) {
	r, sub, proxy, admin := startAnnouncedInput(t)
	r.ProcessContext.Go(func() { r.watchAnnouncements(sub) })
	requireRoomInputProcessed(t, r, admin, "!before:test")

	proxy.cut(true)
	time.Sleep(7 * time.Second)
	proxy.cut(false)

	requireRoomInputProcessed(t, r, admin, "!after:test")
}

// TestAnnouncementsRecreatedAfterDeletion covers a stream-wide consumer the
// broker deleted anyway, as after an outage longer than
// announcerInactiveThreshold. It must be recreated, and must announce the
// events queued meanwhile for rooms whose durable consumer outlived their
// worker, which nothing else would start again.
func TestAnnouncementsRecreatedAfterDeletion(t *testing.T) {
	interval := announcerCheckInterval
	announcerCheckInterval = 50 * time.Millisecond
	t.Cleanup(func() { announcerCheckInterval = interval })
	r, sub, _, admin := startAnnouncedInput(t)

	var deleted int
	for info := range admin.Consumers(r.InputRoomEventTopic) {
		if info.Config.Durable == "" {
			require.NoError(t, admin.DeleteConsumer(r.InputRoomEventTopic, info.Name))
			deleted++
		}
	}
	require.Equal(t, 1, deleted, "expected one stream-wide consumer")
	// A room whose worker stopped for inactivity keeps its durable consumer,
	// which keeps the room's events on the stream until they are processed.
	idle := "!idle:test"
	_, err := admin.AddConsumer(r.InputRoomEventTopic, &nats.ConsumerConfig{
		Durable:           r.Cfg.Matrix.JetStream.Prefixed("RoomInput" + jetstream.Tokenise(idle)),
		AckPolicy:         nats.AckExplicitPolicy,
		DeliverPolicy:     nats.DeliverAllPolicy,
		FilterSubject:     r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(idle)),
		AckWait:           MaximumMissingProcessingTime + roomAckSlack,
		InactiveThreshold: inactiveThreshold,
	})
	require.NoError(t, err)
	seq := queueRoomInput(t, r, admin, idle)

	r.ProcessContext.Go(func() { r.watchAnnouncements(sub) })
	requireRoomFetched(t, r, admin, idle, seq, "the event queued while nothing announced it was never processed")
	requireRoomInputProcessed(t, r, admin, "!new:test")
}
