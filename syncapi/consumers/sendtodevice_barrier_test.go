// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// The barrier holds while a published message is unhandled in a real JetStream
// consumer, and returns once the consumer has acknowledged it.
func TestSendToDeviceBarrierAgainstJetStream(t *testing.T) {
	cfg, processCtx, closeRig := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeRig()
	natsInstance := jetstream.NATSInstance{}
	js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(js, &cfg.Global.JetStream)

	topic := cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent)
	durable := cfg.Global.JetStream.Durable("SyncAPISendToDeviceConsumer")
	release := make(chan struct{})
	require.NoError(t, jetstream.JetStreamConsumer(
		processCtx.Context(), js, topic, durable, 1,
		func(ctx context.Context, msgs []*nats.Msg) bool {
			<-release
			return true
		}, nats.DeliverAll(), nats.ManualAck(),
	))
	consumer := &OutputSendToDeviceEventConsumer{
		jetstream:   js,
		topic:       topic,
		pullDurable: durable + "Pull",
	}

	_, err := js.Publish(topic, []byte("first"))
	require.NoError(t, err)

	// The message is not handled yet, so the barrier runs out its timeout.
	started := time.Now()
	consumer.WaitUntilCaughtUp(context.Background(), 150*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(started), 100*time.Millisecond)

	done := make(chan struct{})
	go func() {
		consumer.WaitUntilCaughtUp(context.Background(), 5*time.Second)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the barrier did not return once the message was handled")
	}
}
