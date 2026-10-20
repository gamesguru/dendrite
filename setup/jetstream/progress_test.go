package jetstream

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func gaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		panic(err)
	}
	return m.GetGauge().GetValue()
}

// TestBacklogVisibleWhileConsumerIsStuck runs a consumer whose handler never
// finishes while more messages arrive. No delivery reports them, so only the
// broker poll shows the backlog growing behind the stuck message.
func TestBacklogVisibleWhileConsumerIsStuck(t *testing.T) {
	restore := consumerStatsInterval.Swap(int64(50 * time.Millisecond))
	t.Cleanup(func() { consumerStatsInterval.Store(restore) })

	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	const subject = "backlog_test_subject"
	_, err := js.AddStream(&nats.StreamConfig{Name: subject, Subjects: []string{subject}, Storage: nats.MemoryStorage, Retention: nats.InterestPolicy})
	require.NoError(t, err)

	p := process.NewProcessContextWithParent(t.Context())
	defer p.ShutdownZendrite()
	release := make(chan struct{})
	defer close(release)
	require.NoError(t, JetStreamConsumer(p.Context(), js, subject, "backlogTest", 1,
		func(ctx context.Context, _ []*nats.Msg) bool {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return true
		}))
	for range 4 {
		_, err = Publish(t.Context(), js, &nats.Msg{Subject: subject, Data: []byte("event")})
		require.NoError(t, err)
	}
	pending := consumerPending.WithLabelValues("backlogTestPull")
	require.Eventually(t, func() bool { return gaugeValue(pending) == 3 }, 5*time.Second, 20*time.Millisecond)
}
