package jetstream

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func replicaWarnings(hook *logrustest.Hook, level logrus.Level, message string) []*logrus.Entry {
	var out []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level == level && strings.Contains(entry.Message, message) {
			out = append(out, entry)
		}
	}
	return out
}

// A follower that stays behind its leader, as one that stopped applying the
// stream after its broker rejoined does (#473), is exported at once and
// reported once it has been behind for longer than a restart takes.
func TestReplicaMonitorReportsStuckFollower(t *testing.T) {
	hook := logrustest.NewGlobal()
	defer hook.Reset()
	m := newReplicaMonitor()
	const stream = "ReplicaMonitorTest"
	cluster := func(current bool, lag uint64) *nats.ClusterInfo {
		return &nats.ClusterInfo{Leader: "nats-0", Replicas: []*nats.PeerInfo{
			{Name: "nats-1", Current: true},
			{Name: "nats-2", Current: current, Lag: lag},
		}}
	}
	start := time.Now()

	m.observe(start, stream, cluster(false, 690))
	require.InDelta(t, 1, gaugeValue(streamReplicaCurrent.WithLabelValues(stream, "nats-1")), 0)
	require.InDelta(t, 0, gaugeValue(streamReplicaCurrent.WithLabelValues(stream, "nats-2")), 0)
	require.InDelta(t, 690, gaugeValue(streamReplicaLag.WithLabelValues(stream, "nats-2")), 0)

	m.observe(start.Add(replicaStuckAfter-time.Second), stream, cluster(false, 690))
	require.Empty(t, replicaWarnings(hook, logrus.WarnLevel, "not catching up"), "a follower briefly behind is not reported")

	m.observe(start.Add(replicaStuckAfter), stream, cluster(false, 700))
	m.observe(start.Add(replicaStuckAfter+time.Minute), stream, cluster(false, 710))
	warnings := replicaWarnings(hook, logrus.WarnLevel, "not catching up")
	require.Len(t, warnings, 1, "a stuck follower is reported once")
	require.Equal(t, "nats-2", warnings[0].Data["replica"])
	require.Equal(t, stream, warnings[0].Data["stream"])
	require.Equal(t, uint64(700), warnings[0].Data["lag"])

	m.observe(start.Add(replicaStuckAfter+2*time.Minute), stream, cluster(true, 0))
	require.Len(t, replicaWarnings(hook, logrus.InfoLevel, "caught up"), 1)
	require.InDelta(t, 1, gaugeValue(streamReplicaCurrent.WithLabelValues(stream, "nats-2")), 0)

	// Falling behind again starts a new period.
	m.observe(start.Add(replicaStuckAfter+3*time.Minute), stream, cluster(false, 5))
	m.observe(start.Add(replicaStuckAfter+4*time.Minute), stream, cluster(false, 5))
	require.Len(t, replicaWarnings(hook, logrus.WarnLevel, "not catching up"), 1)
}

// A broker replaced under another name leaves no series behind for the peer
// that is gone.
func TestReplicaMonitorDropsDepartedPeers(t *testing.T) {
	m := newReplicaMonitor()
	const stream = "ReplicaMonitorDepartedTest"
	now := time.Now()
	m.observe(now, stream, &nats.ClusterInfo{Leader: "a", Replicas: []*nats.PeerInfo{{Name: "b", Current: true}, {Name: "c"}}})
	m.observe(now, stream, &nats.ClusterInfo{Leader: "a", Replicas: []*nats.PeerInfo{{Name: "b", Current: true}, {Name: "d", Current: true}}})
	_, cExported := replicaGauge(streamReplicaCurrent, stream, "c")
	require.False(t, cExported)
	_, dExported := replicaGauge(streamReplicaCurrent, stream, "d")
	require.True(t, dExported)
	require.NotContains(t, m.behind, replicaKey{stream, "c"})
}

// Without a leader nobody can tell how far the followers are, so a stale
// reading is not left standing, while a follower that was behind before the
// election keeps its place in line for a report.
func TestReplicaMonitorWithoutLeader(t *testing.T) {
	m := newReplicaMonitor()
	const stream = "ReplicaMonitorLeaderlessTest"
	now := time.Now()
	m.observe(now, stream, &nats.ClusterInfo{Leader: "a", Replicas: []*nats.PeerInfo{{Name: "b", Current: true}, {Name: "c"}}})
	m.observe(now.Add(time.Minute), stream, &nats.ClusterInfo{Replicas: []*nats.PeerInfo{{Name: "b"}, {Name: "c"}}})
	_, exported := replicaGauge(streamReplicaCurrent, stream, "c")
	require.False(t, exported)
	require.Contains(t, m.behind, replicaKey{stream, "c"})
	m.observe(now.Add(2*time.Minute), stream, &nats.ClusterInfo{Leader: "b", Replicas: []*nats.PeerInfo{{Name: "a", Current: true}, {Name: "c"}}})
	value, exported := replicaGauge(streamReplicaCurrent, stream, "c")
	require.True(t, exported)
	require.InDelta(t, 0, value, 0)
	require.Equal(t, now, m.behind[replicaKey{stream, "c"}].since)
}

// replicaGauge reads a replica series without creating it.
func replicaGauge(vec *prometheus.GaugeVec, stream, replica string) (float64, bool) {
	ch := make(chan prometheus.Metric, 64)
	go func() { vec.Collect(ch); close(ch) }()
	value, found := 0.0, false
	for metric := range ch {
		var m dto.Metric
		if metric.Write(&m) != nil {
			continue
		}
		labels := map[string]string{}
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["stream"] == stream && labels["replica"] == replica {
			value, found = m.GetGauge().GetValue(), true
		}
	}
	return value, found
}

// The monitor reads a real cluster: a follower whose broker is gone is
// exported as not current.
func TestReplicaMonitorReadsCluster(t *testing.T) {
	seed := storageTestServer(t, "node-0", nil)
	route, err := url.Parse("nats://" + seed.ClusterAddr().String())
	require.NoError(t, err)
	servers := []*natsserver.Server{seed}
	for i := 1; i < 3; i++ {
		servers = append(servers, storageTestServer(t, fmt.Sprintf("node-%d", i), []*url.URL{route}))
	}
	js := storageTestClient(t, seed)
	cfg := &config.JetStream{Matrix: &config.Global{HA: config.HA{Enabled: true}}, Addresses: []string{seed.ClientURL()}, TopicPrefix: "replicamon_", Replicas: 3}
	p := process.NewProcessContextWithParent(t.Context())
	defer p.ShutdownZendrite()
	require.Eventually(t, func() bool { return configureStreams(p, cfg, js) == nil }, 30*time.Second, 100*time.Millisecond)

	info, err := js.StreamInfo(cfg.Prefixed(InputRoomEvent))
	require.NoError(t, err)
	var follower *natsserver.Server
	for _, s := range servers[1:] {
		if s.Name() != info.Cluster.Leader {
			follower = s
			break
		}
	}
	require.NotNil(t, follower)
	follower.Shutdown()
	follower.WaitForShutdown()

	go monitorReplicas(p, cfg, js, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		info, err := js.StreamInfo(cfg.Prefixed(InputRoomEvent))
		if err != nil || info.Cluster == nil {
			return false
		}
		for _, peer := range info.Cluster.Replicas {
			if peer.Name == follower.Name() && (peer.Offline || !peer.Current) {
				value, found := replicaGauge(streamReplicaCurrent, InputRoomEvent, follower.Name())
				return found && value == 0
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)
}
