package jetstream

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestHADeliveryRecoversFromFrozenConsumer(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "freeze", Subjects: []string{"freeze"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)
	old, err := js.PullSubscribe("freeze", "workerPull", nats.AckWait(5*time.Minute))
	require.NoError(t, err)
	_, err = js.Publish("freeze", []byte("committed work"))
	require.NoError(t, err)
	held, err := old.Fetch(1, nats.MaxWait(time.Second))
	require.NoError(t, err)
	require.Len(t, held, 1)
	// The former owner stays connected and never acknowledges or resumes.
	current := Protected(js, &testOwnership{generation: 2})
	delivered := make(chan string, 1)
	require.NoError(t, JetStreamConsumer(t.Context(), current, "freeze", "worker", 1, func(_ context.Context, msgs []*nats.Msg) bool {
		delivered <- string(msgs[0].Data)
		return true
	}))
	select {
	case body := <-delivered:
		require.Equal(t, "committed work", body)
	case <-time.After(HAConsumerAckWait + 3*time.Second):
		t.Fatal("successor waited for the frozen consumer")
	}
}

func TestHADeliveryKeepsLongHandlerAlive(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "long", Subjects: []string{"long"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)
	sub, err := js.PullSubscribe("long", "worker", nats.AckWait(HAConsumerAckWait))
	require.NoError(t, err)
	_, err = js.Publish("long", []byte("work"))
	require.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	require.NoError(t, err)
	stop := KeepAliveDelivery(t.Context(), &testOwnership{generation: 2}, msgs)
	defer stop()
	competing, err := js.PullSubscribe("long", "worker", nats.Bind("long", "worker"))
	require.NoError(t, err)
	_, err = competing.Fetch(1, nats.MaxWait(HAConsumerAckWait+time.Second))
	require.ErrorIs(t, err, nats.ErrTimeout, "healthy processing must not be redelivered")
	stop()
	require.NoError(t, msgs[0].AckSync())
}

// A progress update whose reply is lost, as during a stream leader election,
// must not stop the keepalive: the handler would otherwise lose its message to
// redelivery while it is still processing it.
func TestHADeliveryKeepAliveOutlastsALostReply(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	_, err := js.AddStream(&nats.StreamConfig{Name: "lostprogress", Subjects: []string{"lostprogress"}, Storage: nats.MemoryStorage})
	require.NoError(t, err)
	sub, err := js.PullSubscribe("lostprogress", "worker", nats.AckWait(HAConsumerAckWait))
	require.NoError(t, err)
	_, err = js.Publish("lostprogress", []byte("work"))
	require.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	require.NoError(t, err)

	// A subscriber that takes the progress updates and never answers.
	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	updates := make(chan struct{}, 16)
	const blackHole = "lost_progress_replies"
	_, err = nc.Subscribe(blackHole, func(*nats.Msg) { updates <- struct{}{} })
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
	msgs[0].Reply = blackHole

	stop := KeepAliveDelivery(t.Context(), &testOwnership{generation: 2}, msgs)
	defer stop()
	for i := range 2 {
		select {
		case <-updates:
		case <-time.After(HAConsumerAckWait):
			t.Fatalf("the keepalive stopped after %d progress update(s) got no reply", i)
		}
	}
}

// An owner that hands over must not release the Lease while a pull request it
// sent can still take a successor's message.
func TestWaitForOwnedPulls(t *testing.T) {
	ownedPullsExpire.Store(time.Now().Add(200 * time.Millisecond).UnixNano())
	started := time.Now()
	WaitForOwnedPulls(t.Context())
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond {
		t.Fatalf("returned after %s, before the pull request expired", elapsed)
	}
	started = time.Now()
	WaitForOwnedPulls(t.Context())
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("waited %s with no pull request outstanding", elapsed)
	}
}
