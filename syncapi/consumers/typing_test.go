package consumers

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/setup/jetstream"
	"github.com/element-hq/dendrite/syncapi/notifier"
	"github.com/element-hq/dendrite/syncapi/streams"
)

func TestTypingConsumerUsesDefaultTimeoutForZeroTimeout(t *testing.T) {
	const roomID = "!room:test"
	const userID = "@alice:test"

	cache := caching.NewTypingCache()
	consumer := &OutputTypingEventConsumer{
		ctx:      context.Background(),
		eduCache: cache,
		stream:   &streams.TypingStreamProvider{EDUCache: cache},
		notifier: notifier.NewNotifier(nil),
	}
	msg := nats.NewMsg("typing")
	msg.Header.Set(jetstream.RoomID, roomID)
	msg.Header.Set(jetstream.UserID, userID)
	msg.Header.Set("typing", "true")
	msg.Header.Set("timeout_ms", "0")

	if !consumer.onMessage(context.Background(), []*nats.Msg{msg}) {
		t.Fatal("expected typing message to be acknowledged")
	}

	users := cache.GetTypingUsers(roomID)
	if len(users) != 1 || users[0] != userID {
		t.Fatalf("expected %q to be typing, got %+v", userID, users)
	}
}
