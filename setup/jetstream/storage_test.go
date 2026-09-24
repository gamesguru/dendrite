package jetstream

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func storageTestServer(t *testing.T, name string, routes []*url.URL) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{ServerName: name, Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoSigs: true, NoLog: true, SyncAlways: true}
	if name != "standalone" {
		opts.Cluster = natsserver.ClusterOpts{Name: "ha-storage-test", Host: "127.0.0.1", Port: -1}
		opts.Routes = routes
		if len(routes) == 0 {
			opts.Routes = []*url.URL{{Scheme: "nats", Host: "127.0.0.1:1"}}
		}
	}
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	require.True(t, s.ReadyForConnections(10*time.Second))
	return s
}

func storageTestClient(t *testing.T, s *natsserver.Server) nats.JetStreamContext {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(2 * time.Second))
	require.NoError(t, err)
	return js
}

func TestStreamUpdatePreservesNamespaceAndPendingData(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	cfg := &config.JetStream{TopicPrefix: "custom_", Replicas: 1}
	name := cfg.Prefixed(InputRoomEvent)
	_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name, name + ".>"}, Storage: nats.FileStorage, Retention: nats.InterestPolicy, MaxAge: 48 * time.Hour, MaxBytes: 1024 * 1024})
	require.NoError(t, err)
	_, err = js.AddConsumer(name, &nats.ConsumerConfig{Durable: "pending", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)
	ack, err := js.Publish(name, []byte("preserve me"))
	require.NoError(t, err)
	p := process.NewProcessContextWithParent(t.Context())
	require.NoError(t, configureStreams(p, cfg, js))
	info, err := js.StreamInfo(name)
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, info.Config.MaxAge)
	require.EqualValues(t, 1024*1024, info.Config.MaxBytes)
	msg, err := js.GetMsg(name, ack.Sequence)
	require.NoError(t, err)
	require.Equal(t, "preserve me", string(msg.Data))
	_, err = js.StreamInfo(InputRoomEvent)
	require.ErrorIs(t, err, nats.ErrStreamNotFound)
	// An unsupported storage conversion must leave the stream and message intact.
	cfg.InMemory = true
	require.Error(t, configureStreams(p, cfg, js))
	_, err = js.GetMsg(name, ack.Sequence)
	require.NoError(t, err)
	require.Equal(t, nats.FileStorage, streams[0].Storage)
}

func TestHAInsufficientReplicasDoesNotFallBack(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	cfg := &config.JetStream{Matrix: &config.Global{HA: config.HA{Enabled: true}}, TopicPrefix: "ha_", Replicas: 3}
	p := process.NewProcessContextWithParent(t.Context())
	require.Error(t, configureStreams(p, cfg, js))
	_, err := js.StreamInfo(cfg.Prefixed(InputRoomEvent))
	require.ErrorIs(t, err, nats.ErrStreamNotFound)
	degraded, _ := p.IsDegraded()
	require.False(t, degraded)
}

func TestHAReplicationUpgradeAndBrokerFailover(t *testing.T) {
	seed := storageTestServer(t, "node-0", nil)
	route, err := url.Parse("nats://" + seed.ClusterAddr().String())
	require.NoError(t, err)
	servers := []*natsserver.Server{seed}
	for i := 1; i < 3; i++ {
		servers = append(servers, storageTestServer(t, fmt.Sprintf("node-%d", i), []*url.URL{route}))
	}
	js := storageTestClient(t, seed)
	cfg := &config.JetStream{Matrix: &config.Global{HA: config.HA{Enabled: true}}, TopicPrefix: "ha_", Replicas: 3}
	// Existing R1 streams must be upgraded, including streams with pending work.
	for _, template := range streams {
		desired := *template
		desired.Name = cfg.Prefixed(template.Name)
		desired.Subjects = []string{desired.Name, desired.Name + ".>"}
		desired.Storage = nats.FileStorage
		desired.Replicas = 1
		require.Eventually(t, func() bool { _, addErr := js.AddStream(&desired); return addErr == nil }, 15*time.Second, 100*time.Millisecond)
	}
	name := cfg.Prefixed(InputRoomEvent)
	_, err = js.AddConsumer(name, &nats.ConsumerConfig{Durable: "pending", AckPolicy: nats.AckExplicitPolicy})
	require.NoError(t, err)
	ack, err := js.Publish(name, []byte("acknowledged"))
	require.NoError(t, err)
	p := process.NewProcessContextWithParent(t.Context())
	require.NoError(t, configureStreams(p, cfg, js))
	// Also exercise fresh R3 creation without replacing any occupied stream.
	emptyName := cfg.Prefixed(OutputTypingEvent)
	require.NoError(t, js.DeleteStream(emptyName))
	require.NoError(t, configureStreams(p, cfg, js))
	info, err := js.StreamInfo(name)
	require.NoError(t, err)
	for _, server := range servers {
		if server.Name() == info.Cluster.Leader {
			server.Shutdown()
			server.WaitForShutdown()
			break
		}
	}
	require.Eventually(t, func() bool {
		msg, getErr := js.GetMsg(name, ack.Sequence)
		return getErr == nil && string(msg.Data) == "acknowledged"
	}, 15*time.Second, 100*time.Millisecond)
	// A successor must configure the existing streams with one broker still
	// absent, while preserving every stream's configured replica count.
	require.NoError(t, configureStreams(p, cfg, js))
	// Consumer and stream Raft groups can elect their new leaders at different times.
	require.Eventually(t, func() bool {
		consumer, readErr := js.ConsumerInfo(name, "pending")
		return readErr == nil && consumer.NumPending == 1
	}, 15*time.Second, 100*time.Millisecond)
}

func TestSyncRecoveryWaitsForUnacknowledgedMessages(t *testing.T) {
	s := storageTestServer(t, "standalone", nil)
	js := storageTestClient(t, s)
	cfg := &config.JetStream{TopicPrefix: "sync_"}
	p := process.NewProcessContextWithParent(t.Context())
	require.NoError(t, configureStreams(p, cfg, js))
	for _, pair := range syncRecoveryConsumers {
		_, err := js.AddConsumer(cfg.Prefixed(pair.stream), &nats.ConsumerConfig{Durable: cfg.Durable(pair.consumer) + "Pull", AckPolicy: nats.AckExplicitPolicy})
		require.NoError(t, err)
	}
	pair := syncRecoveryConsumers[0]
	stream, durable := cfg.Prefixed(pair.stream), cfg.Durable(pair.consumer)+"Pull"
	_, err := js.Publish(stream, []byte("must apply before ready"))
	require.NoError(t, err)
	sub, err := js.PullSubscribe(stream, durable, nats.Bind(stream, durable))
	require.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.Context(t.Context()))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- RecoverSync(t.Context(), js, cfg) }()
	select {
	case err := <-done:
		t.Fatalf("recovery finished before acknowledgement: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, msgs[0].AckSync())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not complete")
	}
}

func TestSyncRecoveryPresenceConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		inbound, outbound, present bool
		wantError                  bool
	}{
		{name: "disabled by default"},
		{name: "inbound missing", inbound: true, wantError: true},
		{name: "outbound missing", outbound: true, wantError: true},
		{name: "inbound enabled", inbound: true, present: true},
		{name: "outbound enabled", outbound: true, present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := storageTestServer(t, "standalone", nil)
			js := storageTestClient(t, s)
			cfg := &config.JetStream{TopicPrefix: "presence_", Matrix: &config.Global{}}
			cfg.Matrix.Presence.EnableInbound = tc.inbound
			cfg.Matrix.Presence.EnableOutbound = tc.outbound
			p := process.NewProcessContextWithParent(t.Context())
			require.NoError(t, configureStreams(p, cfg, js))
			for _, pair := range syncRecoveryConsumers {
				if pair.stream == OutputPresenceEvent && !tc.present {
					continue
				}
				_, err := js.AddConsumer(cfg.Prefixed(pair.stream), &nats.ConsumerConfig{Durable: cfg.Durable(pair.consumer) + "Pull", AckPolicy: nats.AckExplicitPolicy})
				require.NoError(t, err)
			}
			err := RecoverSync(t.Context(), js, cfg)
			if tc.wantError {
				require.ErrorIs(t, err, nats.ErrConsumerNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type replicaSnapshot struct {
	nats.JetStreamContext
	info *nats.StreamInfo
}

func (s replicaSnapshot) StreamInfo(string, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return s.info, nil
}

func TestReplicaReadinessUnderLoad(t *testing.T) {
	for _, tc := range []struct {
		name             string
		current, offline bool
		lag              uint64
		wantError        bool
	}{
		{name: "caught up", current: true},
		{name: "current under load", current: true, lag: 100},
		{name: "not current", wantError: true},
		{name: "offline", current: true, offline: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			js := replicaSnapshot{info: &nats.StreamInfo{Config: nats.StreamConfig{Replicas: 3}, Cluster: &nats.ClusterInfo{Leader: "leader", Replicas: []*nats.PeerInfo{
				{Name: "peer-1", Current: true}, {Name: "peer-2", Current: tc.current, Offline: tc.offline, Lag: tc.lag},
			}}}}
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			err := waitForReplicas(ctx, js, "stream", 3, true)
			if tc.wantError {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHAAllowsRoomWakeupConsumer(t *testing.T) {
	seed := storageTestServer(t, "node-0", nil)
	route, err := url.Parse("nats://" + seed.ClusterAddr().String())
	require.NoError(t, err)
	for i := 1; i < 3; i++ {
		storageTestServer(t, fmt.Sprintf("node-%d", i), []*url.URL{route})
	}
	nc, err := nats.Connect(seed.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	require.NoError(t, err)
	consumerJS, err := natsjs.New(nc)
	require.NoError(t, err)
	cfg := &config.JetStream{TopicPrefix: "wake_", Replicas: 3, Matrix: &config.Global{HA: config.HA{Enabled: true}}}
	p := process.NewProcessContextWithParent(t.Context())
	// Wait for metadata election before preparing the application streams.
	require.Eventually(t, func() bool { _, infoErr := js.AccountInfo(); return infoErr == nil }, 15*time.Second, 100*time.Millisecond)
	require.NoError(t, configureStreams(p, cfg, js))
	stream := cfg.Prefixed(InputRoomEvent)
	// Match Inputer.Start, including the server-assigned ephemeral consumer name.
	sub, err := js.Subscribe("", func(*nats.Msg) {}, nats.HeadersOnly(), nats.DeliverAll(), nats.AckExplicit(), nats.ReplayInstant(), nats.BindStream(stream))
	require.NoError(t, err)
	info, err := sub.ConsumerInfo()
	require.NoError(t, err)
	require.Empty(t, info.Config.Durable)
	require.NoError(t, validateConsumers(t.Context(), consumerJS, stream, cfg))
	// This is the full stream/consumer check performed before activation.
	require.NotPanics(t, func() { checkAndConfigureStreams(p, cfg, js, nc) })
	for _, tc := range []struct {
		name   string
		config nats.ConsumerConfig
	}{
		{"durable push", nats.ConsumerConfig{Durable: "legacy", DeliverSubject: "legacy.deliver", HeadersOnly: true, AckPolicy: nats.AckExplicitPolicy}},
		{"memory durable", nats.ConsumerConfig{Durable: "memory", MemoryStorage: true, AckPolicy: nats.AckExplicitPolicy}},
		{"ephemeral payload push", nats.ConsumerConfig{DeliverSubject: "payload.deliver", AckPolicy: nats.AckExplicitPolicy}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid, createErr := js.AddConsumer(stream, &tc.config)
			require.NoError(t, createErr)
			require.ErrorContains(t, validateConsumers(t.Context(), consumerJS, stream, cfg), "offline")
			require.NoError(t, js.DeleteConsumer(stream, invalid.Name))
		})
	}
}

type streamConfigRecorder struct {
	nats.JetStreamContext
	configs map[string]nats.StreamConfig
	updates int
}

func (r *streamConfigRecorder) StreamInfo(name string, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	cfg, ok := r.configs[name]
	if !ok {
		return nil, nats.ErrStreamNotFound
	}
	return &nats.StreamInfo{Config: cfg}, nil
}

func (r *streamConfigRecorder) AddStream(cfg *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	r.configs[cfg.Name] = *cfg
	return &nats.StreamInfo{Config: *cfg}, nil
}

func (r *streamConfigRecorder) UpdateStream(cfg *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	r.updates++
	r.configs[cfg.Name] = *cfg
	return &nats.StreamInfo{Config: *cfg}, nil
}

func TestStandaloneReplicaPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		existing, configured, want int
		changeAge                  bool
	}{
		{name: "preserve R3", existing: 3, want: 3},
		{name: "preserve R5", existing: 5, want: 5},
		{name: "preserve R3 during another update", existing: 3, want: 3, changeAge: true},
		{name: "explicit upgrade", existing: 1, configured: 3, want: 3},
		{name: "explicit downscale", existing: 3, configured: 1, want: 1},
		{name: "default new stream", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.JetStream{TopicPrefix: "standalone_", Replicas: tc.configured}
			js := &streamConfigRecorder{configs: make(map[string]nats.StreamConfig)}
			name := cfg.Prefixed(InputRoomEvent)
			if tc.existing > 0 {
				original := *streams[0]
				original.Name = name
				original.Subjects = []string{name, name + ".>"}
				original.Replicas = tc.existing
				if tc.changeAge {
					original.MaxAge = 48 * time.Hour
				}
				js.configs[name] = original
			}
			p := process.NewProcessContextWithParent(t.Context())
			require.NoError(t, configureStreams(p, cfg, js))
			require.Equal(t, tc.want, js.configs[name].Replicas)
			if tc.existing == tc.want && !tc.changeAge {
				require.Zero(t, js.updates)
			} else if tc.existing > 0 {
				require.Equal(t, 1, js.updates)
			}
		})
	}
}

type consumerListClient struct {
	natsjs.JetStream
	stream natsjs.Stream
}

func (j consumerListClient) Stream(context.Context, string) (natsjs.Stream, error) {
	return j.stream, nil
}

type consumerListStream struct {
	natsjs.Stream
	list func(context.Context) natsjs.ConsumerInfoLister
}

func (s consumerListStream) ListConsumers(ctx context.Context) natsjs.ConsumerInfoLister {
	return s.list(ctx)
}

type consumerListResult struct {
	info <-chan *natsjs.ConsumerInfo
	err  error
}

func (l consumerListResult) Info() <-chan *natsjs.ConsumerInfo { return l.info }
func (l consumerListResult) Err() error                        { return l.err }

func TestConsumerValidationReportsListingErrors(t *testing.T) {
	failure := errors.New("listing failed after a page")
	js := consumerListClient{stream: consumerListStream{list: func(context.Context) natsjs.ConsumerInfoLister {
		infos := make(chan *natsjs.ConsumerInfo, 1)
		infos <- &natsjs.ConsumerInfo{Name: "valid", Config: natsjs.ConsumerConfig{Durable: "valid", AckPolicy: natsjs.AckExplicitPolicy}}
		close(infos)
		return consumerListResult{info: infos, err: failure}
	}}}
	require.ErrorIs(t, validateConsumers(t.Context(), js, "stream", &config.JetStream{Replicas: 3}), failure)
}

func TestConsumerValidationDrainsBlockedEnumeration(t *testing.T) {
	stopped := make(chan struct{})
	infos := make(chan *natsjs.ConsumerInfo)
	// Also unblock the producer if a regression makes validation return too soon.
	t.Cleanup(func() {
		for range infos {
		}
	})
	js := consumerListClient{stream: consumerListStream{list: func(ctx context.Context) natsjs.ConsumerInfoLister {
		go func() {
			defer close(infos)
			defer close(stopped)
			invalid := &natsjs.ConsumerInfo{Name: "unreplicated", Config: natsjs.ConsumerConfig{Durable: "unreplicated", Replicas: 1, AckPolicy: natsjs.AckExplicitPolicy}}
			infos <- invalid
			// Model the real lister's race: it has passed its cancellation check
			// and must finish this unconditional send even when validation cancels.
			infos <- invalid
			<-ctx.Done()
		}()
		return consumerListResult{info: infos}
	}}}
	require.ErrorContains(t, validateConsumers(t.Context(), js, "stream", &config.JetStream{Replicas: 3}), "offline")
	select {
	case <-stopped:
	default:
		t.Fatal("validation returned before enumeration stopped")
	}
}

type trackedConsumerStream struct {
	natsjs.Stream
	listing natsjs.ConsumerInfoLister
}

func (s *trackedConsumerStream) ListConsumers(ctx context.Context) natsjs.ConsumerInfoLister {
	s.listing = s.Stream.ListConsumers(ctx)
	return s.listing
}

func TestConsumerValidationDrainsRealLister(t *testing.T) {
	server := storageTestServer(t, "standalone", nil)
	nc, err := nats.Connect(server.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := natsjs.New(nc)
	require.NoError(t, err)
	stream, err := js.CreateStream(t.Context(), natsjs.StreamConfig{Name: "validation", Subjects: []string{"validation"}})
	require.NoError(t, err)
	for _, name := range []string{"invalid-first", "invalid-second", "invalid-third"} {
		_, err = stream.CreateConsumer(t.Context(), natsjs.ConsumerConfig{Durable: name, MemoryStorage: true, AckPolicy: natsjs.AckExplicitPolicy})
		require.NoError(t, err)
	}
	tracked := &trackedConsumerStream{Stream: stream}
	client := consumerListClient{stream: tracked}
	require.ErrorContains(t, validateConsumers(t.Context(), client, "validation", &config.JetStream{Replicas: 1}), "offline")
	// Channel closure synchronizes with the real producer's deferred cleanup.
	select {
	case _, open := <-tracked.listing.Info():
		require.False(t, open, "lister still has an undrained send")
	default:
		t.Fatal("real lister has not closed its channel")
	}
}

func TestExistingStreamsRequireQuorum(t *testing.T) {
	for _, tc := range []struct {
		name      string
		current   int
		replicas  int
		leader    string
		wantError bool
	}{
		{name: "one failed broker", current: 1, replicas: 3, leader: "leader"},
		{name: "lost quorum", current: 0, replicas: 3, leader: "leader", wantError: true},
		{name: "no elected leader", current: 2, replicas: 3, wantError: true},
		{name: "wrong replica configuration", current: 2, replicas: 1, leader: "leader", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peers := []*nats.PeerInfo{{Name: "peer-1", Current: tc.current > 0}, {Name: "peer-2", Current: tc.current > 1}}
			js := replicaSnapshot{info: &nats.StreamInfo{Config: nats.StreamConfig{Replicas: tc.replicas}, Cluster: &nats.ClusterInfo{Leader: tc.leader, Replicas: peers}}}
			ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
			defer cancel()
			err := waitForReplicas(ctx, js, "stream", 3, false)
			if tc.wantError {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
