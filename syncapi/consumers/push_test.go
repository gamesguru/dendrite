// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/producers"
)

// pushJetStream records published messages instead of sending them. Only
// PublishMsg is overridden; anything else panics on a nil interface, which
// keeps the test honest about what it exercises.
type pushJetStream struct {
	nats.JetStreamContext

	published []*nats.Msg
	err       error
}

func (m *pushJetStream) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.published = append(m.published, msg)
	return &nats.PubAck{}, nil
}

func TestProducePushEvent_NoProducerConfigured(t *testing.T) {
	t.Parallel()

	s := &OutputRoomEventConsumer{}

	require.NoError(t, s.producePushEvent(&nats.Msg{Subject: "OutputRoomEvent"}))
}

func TestProducePushEvent_Republishes(t *testing.T) {
	t.Parallel()

	js := &pushJetStream{}
	s := &OutputRoomEventConsumer{
		pushProducer: &producers.PushEventProducer{Topic: "OutputPushEvent", JetStream: js},
	}

	require.NoError(t, s.producePushEvent(&nats.Msg{Subject: "OutputRoomEvent", Data: []byte("event")}))
	require.Len(t, js.published, 1)
	assert.Equal(t, "OutputPushEvent", js.published[0].Subject)
}

// A publish failure must reach the caller so that onMessage NAKs: swallowing it
// would ack the roomserver message and lose the push notification for good.
func TestProducePushEvent_PropagatesPublishError(t *testing.T) {
	t.Parallel()

	js := &pushJetStream{err: errors.New("publish failed")}
	s := &OutputRoomEventConsumer{
		pushProducer: &producers.PushEventProducer{Topic: "OutputPushEvent", JetStream: js},
	}

	assert.Error(t, s.producePushEvent(&nats.Msg{Subject: "OutputRoomEvent"}))
}
