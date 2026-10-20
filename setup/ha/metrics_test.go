package ha

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func resetHAMetrics() {
	currentPhase.Reset()
	leadershipTransitions.Reset()
	fencingFailures.Reset()
	recoveryDuration.Set(0)
	shutdownDuration.Set(0)
	for _, phase := range allPhases {
		currentPhase.WithLabelValues(string(phase)).Set(0)
	}
	for _, transition := range []string{"acquired", "lost"} {
		leadershipTransitions.WithLabelValues(transition).Add(0)
	}
	for _, boundary := range requiredBoundaries {
		fencingFailures.WithLabelValues(string(boundary)).Add(0)
	}
}

func collect(t *testing.T, collector prometheus.Collector) []*dto.Metric {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	collector.Collect(ch)
	close(ch)
	var metrics []*dto.Metric
	for metric := range ch {
		out := &dto.Metric{}
		require.NoError(t, metric.Write(out))
		metrics = append(metrics, out)
	}
	return metrics
}

func counterValue(t *testing.T, vec *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	out := &dto.Metric{}
	counter, err := vec.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	metric, ok := counter.(prometheus.Metric)
	require.True(t, ok)
	require.NoError(t, metric.Write(out))
	return out.GetCounter().GetValue()
}

func gaugeVecValue(t *testing.T, vec *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	out := &dto.Metric{}
	gauge, err := vec.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	require.NoError(t, gauge.Write(out))
	return out.GetGauge().GetValue()
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	out := &dto.Metric{}
	require.NoError(t, gauge.Write(out))
	return out.GetGauge().GetValue()
}

func counterValue2(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	out := &dto.Metric{}
	require.NoError(t, counter.Write(out))
	return out.GetCounter().GetValue()
}
