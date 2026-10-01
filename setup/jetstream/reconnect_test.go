package jetstream

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func TestHAReconnectDoesNotBootstrapStorage(t *testing.T) {
	for _, state := range []string{"active", "retired", "stopped"} {
		t.Run(state, func(t *testing.T) {
			server := storageTestServer(t, "standalone", nil)
			nc, err := nats.Connect(server.ClientURL())
			require.NoError(t, err)
			t.Cleanup(nc.Close)
			p := process.NewProcessContextWithParent(t.Context())
			owner := &testOwnership{generation: 1, identity: "candidate"}
			if state == "retired" {
				owner.lose(errors.New("retired"), true)
			}
			if state == "stopped" {
				p.ShutdownZendrite()
			}
			cfg := &config.JetStream{TopicPrefix: "ha_", Replicas: 3, Matrix: &config.Global{HA: config.HA{Enabled: true}}}
			opts, err := natsConnectOptions(p, cfg, []nats.JSOpt{nats.MaxWait(50 * time.Millisecond)}, owner)
			require.NoError(t, err)
			var applied nats.Options
			for _, opt := range opts {
				require.NoError(t, opt(&applied))
			}
			// A reconnect is not authorization to create or migrate missing
			// storage, nor to wait for all replicas during broker failover.
			require.NotPanics(t, func() { applied.ReconnectedCB(nc) })
			js, err := nc.JetStream()
			require.NoError(t, err)
			info, err := js.AccountInfo()
			require.NoError(t, err)
			require.Zero(t, info.Streams)
		})
	}
}

func TestHAReconnectResumesDurableWorkWithOneBrokerDown(t *testing.T) {
	seed := storageTestServer(t, "node-0", nil)
	route, err := url.Parse("nats://" + seed.ClusterAddr().String())
	require.NoError(t, err)
	for i := 1; i < 3; i++ {
		storageTestServer(t, fmt.Sprintf("node-%d", i), []*url.URL{route})
	}
	p := process.NewProcessContextWithParent(t.Context())
	t.Cleanup(func() { p.ShutdownZendrite(); p.WaitForComponentsToFinish() })
	cfg := &config.JetStream{TopicPrefix: "reconnect_", Replicas: 3, Matrix: &config.Global{HA: config.HA{Enabled: true}}}
	owner := &testOwnership{generation: 7, identity: "same-activation"}
	opts, err := natsConnectOptions(p, cfg, nil, owner)
	require.NoError(t, err)
	var applied nats.Options
	for _, opt := range opts {
		require.NoError(t, opt(&applied))
	}
	completed := make(chan struct{})
	var once sync.Once
	opts = append(opts, nats.ReconnectWait(20*time.Millisecond), nats.ReconnectJitter(0, 0), nats.ReconnectHandler(func(c *nats.Conn) {
		applied.ReconnectedCB(c)
		once.Do(func() { close(completed) })
	}))
	nc, err := nats.Connect(seed.ClientURL(), opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	raw, err := nc.JetStream()
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, infoErr := raw.AccountInfo(); return infoErr == nil }, 15*time.Second, 100*time.Millisecond)
	js, _ := setupNATS(p, cfg, nc, owner)
	name := cfg.Prefixed(InputRoomEvent)
	// A freshly formed cluster can take longer than one request to place the consumer.
	require.NoError(t, retryAPI(t.Context(), "add consumer", func() error {
		_, addErr := js.AddConsumer(name, &nats.ConsumerConfig{Durable: "pending", AckPolicy: nats.AckExplicitPolicy, AckWait: 500 * time.Millisecond})
		return addErr
	}))
	sub, err := js.PullSubscribe(name, "pending", nats.Bind(name, "pending"))
	require.NoError(t, err)
	_, err = js.Publish(name, []byte("committed before disconnect"))
	require.NoError(t, err)
	before, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	require.NoError(t, err)
	require.Len(t, before, 1)
	liveSubject := cfg.Prefixed(OutputClientData)
	delivered := make(chan string, 1)
	require.NoError(t, JetStreamConsumer(p.Context(), js, liveSubject, "live", 1, func(_ context.Context, msgs []*nats.Msg) bool {
		delivered <- string(msgs[0].Data)
		return true
	}))
	require.Eventually(t, func() bool {
		info, infoErr := js.ConsumerInfo(liveSubject, "livePull")
		return infoErr == nil && info.NumWaiting == 1
	}, 5*time.Second, 20*time.Millisecond)
	// Leave delivery unacknowledged and remove the server this application
	// connection actually uses. Keep it down throughout the recovery checks.
	require.Equal(t, seed.ClientURL(), nc.ConnectedUrl())
	seed.Shutdown()
	seed.WaitForShutdown()
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect callback waited for the unavailable third replica")
	}
	require.NotEqual(t, seed.ClientURL(), nc.ConnectedUrl())
	var messages []*nats.Msg
	require.Eventually(t, func() bool {
		messages, err = sub.Fetch(1, nats.MaxWait(time.Second))
		return err == nil && len(messages) == 1
	}, 15*time.Second, 100*time.Millisecond)
	require.Equal(t, before[0].Data, messages[0].Data)
	require.Equal(t, "7", messages[0].Header.Get(GenerationHeader))
	require.NoError(t, messages[0].AckSync())
	_, err = js.Publish(name, []byte("committed after reconnect"))
	require.NoError(t, err)
	messages, err = sub.Fetch(1, nats.MaxWait(5*time.Second))
	require.NoError(t, err)
	require.Equal(t, "committed after reconnect", string(messages[0].Data))
	require.NoError(t, messages[0].AckSync())
	_, err = js.Publish(liveSubject, []byte("worker resumes after disconnect"))
	require.NoError(t, err)
	select {
	case body := <-delivered:
		require.Equal(t, "worker resumes after disconnect", body)
	case <-time.After(5 * time.Second):
		t.Fatal("the application consumer stopped permanently on broker disconnect")
	}
	retired := errors.New("retired after reconnect")
	owner.lose(retired, true)
	_, err = js.Publish(name, []byte("must not publish"))
	require.ErrorIs(t, err, retired, "reconnection must preserve ownership fencing")
}
