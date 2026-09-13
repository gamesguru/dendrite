package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// RecoverSync waits for each sync consumer to apply its startup backlog before
// readiness. New publications do not move the recovery target indefinitely.
func RecoverSync(ctx context.Context, js nats.JetStreamContext, cfg *config.JetStream) error {
	const recoveryTimeout = 120 * time.Second
	ctx, cancel := context.WithTimeout(ctx, recoveryTimeout)
	defer cancel()
	for _, pair := range syncRecoveryConsumers {
		if pair.stream == OutputPresenceEvent && (cfg.Matrix == nil || (!cfg.Matrix.Presence.EnableInbound && !cfg.Matrix.Presence.EnableOutbound)) {
			continue
		}
		stream, durable := cfg.Prefixed(pair.stream), cfg.Durable(pair.consumer)+"Pull"
		streamInfo, err := js.StreamInfo(stream, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect recovery stream %s: %w", stream, err)
		}
		info, err := js.ConsumerInfo(stream, durable, nats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect recovery consumer %s: %w", durable, err)
		}
		target := streamInfo.State.LastSeq
		ticker := time.NewTicker(100 * time.Millisecond)
		for info.AckFloor.Stream < target && (info.NumPending > 0 || info.NumAckPending > 0) {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return fmt.Errorf("recover consumer %s: %w", durable, ctx.Err())
			case <-ticker.C:
			}
			info, err = js.ConsumerInfo(stream, durable, nats.Context(ctx))
			if err != nil {
				ticker.Stop()
				return fmt.Errorf("recover consumer %s: %w", durable, err)
			}
		}
		ticker.Stop()
	}
	return ctx.Err()
}

var syncRecoveryConsumers = []struct{ stream, consumer string }{
	{OutputRoomEvent, "SyncAPIRoomServerConsumer"},
	{OutputKeyChangeEvent, "SyncAPIKeyChangeConsumer"},
	{OutputClientData, "SyncAPIAccountDataConsumer"},
	{OutputNotificationData, "SyncAPINotificationDataConsumer"},
	{OutputTypingEvent, "SyncAPITypingConsumer"},
	{OutputSendToDeviceEvent, "SyncAPISendToDeviceConsumer"},
	{OutputReceiptEvent, "SyncAPIReceiptConsumer"},
	{OutputPresenceEvent, "SyncAPIPresenceConsumer"},
}
