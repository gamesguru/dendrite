package consumers

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// unavailableDatabase fails every write, as the database does while a
// PostgreSQL primary is being replaced.
type unavailableDatabase struct {
	storage.Database
}

// errDatabaseUnavailable is transient: the connection failed, not the write.
var errDatabaseUnavailable = fmt.Errorf("failed to connect to the database: %w", driver.ErrBadConn)

func (unavailableDatabase) WriteEvent(context.Context, *rstypes.HeaderedEvent, []*rstypes.HeaderedEvent, []string, []string, *api.TransactionID, bool, gomatrixserverlib.HistoryVisibility) (types.StreamPosition, error) {
	return 0, errDatabaseUnavailable
}

func (unavailableDatabase) StoreReceipt(context.Context, string, string, string, string, string, spec.Timestamp) (types.StreamPosition, error) {
	return 0, errDatabaseUnavailable
}

type senderResolver struct {
	api.SyncRoomserverAPI
	user *test.User
}

func (r senderResolver) QueryUserIDForSender(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(r.user.ID, true)
}

// TestWriteFailureStopsOnShutdown checks that a write failing because the
// database is unreachable is retried rather than crashing the process, and
// that it ends when the process shuts down, leaving the message for
// redelivery.
func TestWriteFailureStopsOnShutdown(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	ev := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hello"})
	s := &OutputRoomEventConsumer{db: unavailableDatabase{}, rsAPI: senderResolver{user: alice}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NotPanics(t, func() {
		require.ErrorIs(t, s.onNewRoomEvent(ctx, api.OutputNewRoomEvent{Event: ev}), errDatabaseUnavailable)
	})
}

// TestReceiptWriteFailureIsRedelivered checks that a receipt the database
// could not store is left for redelivery rather than acknowledged and lost,
// as happened to a receipt sent while a successor fenced the old owner.
func TestReceiptWriteFailureIsRedelivered(t *testing.T) {
	s := &OutputReceiptEventConsumer{ctx: t.Context(), db: unavailableDatabase{}}
	msg := nats.NewMsg("receipt")
	msg.Header.Set(jetstream.UserID, "@alice:test")
	msg.Header.Set(jetstream.RoomID, "!room:test")
	msg.Header.Set(jetstream.EventID, "$event")
	msg.Header.Set("type", "m.read")
	msg.Header.Set("timestamp", "1")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.False(t, s.onMessage(ctx, []*nats.Msg{msg}), "an unstored receipt must not be acknowledged")
}
