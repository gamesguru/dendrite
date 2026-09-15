package jetstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestRedeliveryBacksOffExponentially(t *testing.T) {
	for _, tc := range []struct {
		attempts uint64
		want     time.Duration
	}{
		{1, redeliveryBaseDelay},
		{2, 500 * time.Millisecond},
		{3, time.Second},
		{4, 2 * time.Second},
	} {
		require.Equal(t, tc.want, redeliveryBackoff(tc.attempts), "attempt %d", tc.attempts)
	}
}

func TestRedeliveryIsCapped(t *testing.T) {
	// A consumer failing against a dependency that stays down must keep
	// checking, but never faster than the cap, and the calculation must not
	// spin or overflow on a message that has been retried for a long time.
	for _, attempts := range []uint64{16, 64, 1 << 20, 1<<64 - 1} {
		require.Equal(t, redeliveryMaxDelay, redeliveryBackoff(attempts), "attempt %d", attempts)
	}
}

func TestRedeliveryWithoutMetadataStillDelays(t *testing.T) {
	// Messages that carry no usable ack metadata must not fall back to an
	// immediate redelivery, which is the behavior this replaced.
	require.Equal(t, redeliveryBaseDelay, redeliveryDelay(&nats.Msg{Subject: "test.subject"}))
}

func TestRedeliveryNeverReturnsZero(t *testing.T) {
	// Zero would restore the spin this exists to prevent, so guard the whole
	// input domain rather than the values the callers happen to produce.
	for _, attempts := range []uint64{0, 1, 2, 7, 1 << 32} {
		require.Positive(t, redeliveryBackoff(attempts), "attempt %d", attempts)
	}
}

// TestRefusedMessageIsRedeliveredAfterADelay is the behavioral half: it fails
// if the worker ever goes back to acknowledging negatively without a delay,
// which the unit tests above cannot catch.
func TestRefusedMessageIsRedeliveredAfterADelay(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)

	const subject = "redelivery_test_subject"
	_, err := js.AddStream(&nats.StreamConfig{
		Name: subject, Subjects: []string{subject}, Storage: nats.MemoryStorage,
		Retention: nats.InterestPolicy,
	})
	require.NoError(t, err)

	var mu sync.Mutex
	var seen []time.Time
	done := make(chan struct{})

	p := process.NewProcessContextWithParent(t.Context())
	defer p.ShutdownZendrite()

	require.NoError(t, JetStreamConsumer(p.Context(), js, subject, "redeliveryTest", 1,
		func(_ context.Context, _ []*nats.Msg) bool {
			mu.Lock()
			seen = append(seen, time.Now())
			count := len(seen)
			mu.Unlock()
			if count == 2 {
				close(done)
				return true
			}
			// Refuse the first delivery, which must come back only after a delay.
			return false
		}, nats.DeliverAll(), nats.ManualAck()))

	_, err = js.Publish(subject, []byte("{}"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("refused message was never redelivered")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 2)
	require.GreaterOrEqual(t, seen[1].Sub(seen[0]), redeliveryBaseDelay,
		"a refused message came back immediately, so it was negatively acknowledged without a delay")
}
