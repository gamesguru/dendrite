package jetstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const testConsumer = "TestConsumerProgressPull"

func gauge(t *testing.T, vec *prometheus.GaugeVec) float64 {
	t.Helper()
	out := &dto.Metric{}
	if err := vec.WithLabelValues(testConsumer).Write(out); err != nil {
		t.Fatal(err)
	}
	return out.GetGauge().GetValue()
}

// deliveredMsg returns a message whose metadata says it was published at
// published with pending messages behind it.
func deliveredMsg(published time.Time, pending uint64) *nats.Msg {
	return &nats.Msg{
		Subject: "test",
		Reply:   fmt.Sprintf("$JS.ACK.stream.consumer.1.10.10.%d.%d", published.UnixNano(), pending),
		Sub:     &nats.Subscription{},
	}
}

func TestConsumerProgressShowsWorkInFlight(t *testing.T) {
	progress := newConsumerProgress(testConsumer)
	published := time.Now().Add(-time.Minute)

	progress.start([]*nats.Msg{deliveredMsg(published, 3)})

	started := gauge(t, consumerProcessingStarted)
	if started == 0 || time.Since(time.Unix(0, int64(started*float64(time.Second)))) > time.Minute {
		t.Fatalf("processing started timestamp is %v while busy, want about now", started)
	}
	if got := gauge(t, consumerProcessingMessage); int64(got) != published.Unix() {
		t.Fatalf("processing message timestamp is %v, want %d", got, published.Unix())
	}
	if got := gauge(t, consumerPending); got != 3 {
		t.Fatalf("pending is %v, want 3", got)
	}

	progress.finish()
	if got := gauge(t, consumerProcessingStarted); got != 0 {
		t.Fatalf("processing started timestamp is %v while idle, want 0", got)
	}
	if got := gauge(t, consumerProcessingMessage); got != 0 {
		t.Fatalf("processing message timestamp is %v while idle, want 0", got)
	}
	if got := gauge(t, consumerLastAck); got != 0 {
		t.Fatalf("last ack timestamp is %v before any acknowledgement, want 0", got)
	}
	progress.acked()
	if got := gauge(t, consumerLastAck); got == 0 {
		t.Fatal("last ack timestamp was not set by an acknowledgement")
	}
	histogram, ok := consumerProcessingSeconds.WithLabelValues(testConsumer).(prometheus.Histogram)
	if !ok {
		t.Fatal("processing duration observer is not a histogram")
	}
	out := &dto.Metric{}
	if err := histogram.Write(out); err != nil {
		t.Fatal(err)
	}
	if got := out.GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("processing duration observed %d times, want 1", got)
	}
}
