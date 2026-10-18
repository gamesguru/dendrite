// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package producers

import (
	"github.com/nats-io/nats.go"
)

// PushEventProducer republishes a room event on Topic only after syncapi has
// durably committed it, so userapi's push consumer (which reads this topic,
// not the raw roomserver output) can never notify a device before the event
// is queryable via /sync.
type PushEventProducer struct {
	Topic     string
	JetStream nats.JetStreamContext
}

func (p *PushEventProducer) ProduceRoomEvents(
	msg *nats.Msg,
) error {
	msg.Subject = p.Topic
	_, err := p.JetStream.PublishMsg(msg)
	return err
}
