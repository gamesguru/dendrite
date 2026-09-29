package msc2836

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	roomserver "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
)

// consumerName is the durable of the relationship index on the roomserver
// output stream.
const consumerName = "MSC2836RoomServerConsumer"

// relationshipConsumer builds the relationship index from the roomserver
// output stream. The index is derived from events the roomserver has already
// accepted, so a failed write must not fail the event: it is refused instead
// and JetStream redelivers it, to this process or to an HA successor, until the
// write succeeds. Every write is idempotent, so redelivery is harmless.
type relationshipConsumer struct {
	jetstream nats.JetStreamContext
	durable   string
	topic     string
	db        Database
}

func newRelationshipConsumer(cfg *config.Zendrite, js nats.JetStreamContext, db Database) *relationshipConsumer {
	return &relationshipConsumer{
		jetstream: js,
		durable:   cfg.Global.JetStream.Durable(consumerName),
		topic:     cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent),
		db:        db,
	}
}

func (c *relationshipConsumer) Start(ctx context.Context, opts ...nats.SubOpt) error {
	return jetstream.JetStreamConsumer(
		ctx, c.jetstream, c.topic, c.durable, 1,
		c.onMessage, append([]nats.SubOpt{nats.DeliverAll(), nats.ManualAck()}, opts...)...,
	)
}

func (c *relationshipConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	var event *types.HeaderedEvent
	switch roomserver.OutputType(msg.Header.Get(jetstream.RoomEventType)) {
	case roomserver.OutputTypeNewRoomEvent, roomserver.OutputTypeOldRoomEvent:
		var output roomserver.OutputEvent
		if err := json.Unmarshal(msg.Data, &output); err != nil {
			// A message that cannot be parsed will never index.
			logrus.WithError(err).Error("msc2836: roomserver output log: message parse failure")
			return true
		}
		switch {
		case output.NewRoomEvent != nil:
			event = output.NewRoomEvent.Event
		case output.OldRoomEvent != nil:
			event = output.OldRoomEvent.Event
		}
	default:
		return true
	}
	if event == nil {
		return true
	}
	if err := indexEvent(ctx, c.db, event); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.WithError(err).WithField("event_id", event.EventID()).
				Warn("msc2836: failed to index relationship; it will be redelivered")
		}
		return false
	}
	return true
}

// indexEvent stores the relationship an event declares and the child metadata
// it carries. Both writes are idempotent, so an event may be indexed again.
func indexEvent(ctx context.Context, db Database, ev *types.HeaderedEvent) error {
	if err := db.StoreRelation(ctx, ev); err != nil {
		return fmt.Errorf("store relation: %w", err)
	}
	// Child metadata can originate from /send transactions as well as from
	// remote /event_relationships requests, so it is updated here too.
	if err := db.UpdateChildMetadata(ctx, ev); err != nil {
		return fmt.Errorf("update child metadata: %w", err)
	}
	return nil
}

// RemoveConsumer deletes the durable of a disabled relationship index. The
// roomserver output stream is interest-based, so an abandoned durable would
// otherwise retain every message on it forever. HA refuses consumer deletion,
// so there the durable must be removed during the offline upgrade that
// disables the MSC.
func RemoveConsumer(cfg *config.Zendrite, js nats.JetStreamContext) {
	if jetstream.OwnershipOf(js) != nil {
		return
	}
	stream := cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent)
	durable := cfg.Global.JetStream.Durable(consumerName) + "Pull"
	if err := js.DeleteConsumer(stream, durable); err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		logrus.WithError(err).Warnf("Failed to remove the consumer %q of the disabled MSC2836", durable)
	}
}
