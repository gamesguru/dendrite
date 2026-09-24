package input

import (
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

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
