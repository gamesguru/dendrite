// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package producers

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockJetStream implements nats.JetStreamContext for testing, recording
// published messages instead of sending them. Only PublishMsg is overridden;
// anything else panics on a nil interface, which keeps the test honest about
// what it exercises.
type mockJetStream struct {
	nats.JetStreamContext

	published []*nats.Msg
	err       error
}

func (m *mockJetStream) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.published = append(m.published, msg)
	return &nats.PubAck{}, nil
}

func TestPushEventProducer_ProduceRoomEvents_RepublishesOnTheConfiguredTopic(t *testing.T) {
	t.Parallel()

	js := &mockJetStream{}
	p := &PushEventProducer{Topic: "OutputPushEvent", JetStream: js}

	err := p.ProduceRoomEvents(&nats.Msg{Subject: "OutputRoomEvent", Data: []byte("event")})

	require.NoError(t, err)
	require.Len(t, js.published, 1)
	assert.Equal(t, "OutputPushEvent", js.published[0].Subject)
	assert.Equal(t, []byte("event"), js.published[0].Data)
}

func TestPushEventProducer_ProduceRoomEvents_PropagatesPublishError(t *testing.T) {
	t.Parallel()

	js := &mockJetStream{err: errors.New("publish failed")}
	p := &PushEventProducer{Topic: "OutputPushEvent", JetStream: js}

	err := p.ProduceRoomEvents(&nats.Msg{Subject: "OutputRoomEvent"})

	assert.Error(t, err)
}
