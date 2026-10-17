package msc2836

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	roomserver "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// failingDatabase refuses relationship writes while failing is set, like a
// database that is unreachable or rejects the index insert.
type failingDatabase struct {
	Database
	failing  atomic.Bool
	attempts atomic.Int32
}

func (d *failingDatabase) StoreRelation(ctx context.Context, ev *types.HeaderedEvent) error {
	d.attempts.Add(1)
	if d.failing.Load() {
		return errors.New("injected relationship index failure")
	}
	return d.Database.StoreRelation(ctx, ev)
}

type consumerFixture struct {
	cfg    *config.Zendrite
	js     nats.JetStreamContext
	db     *failingDatabase
	parent *types.HeaderedEvent
	child  *types.HeaderedEvent
}

func newConsumerFixture(t *testing.T, dbType test.DBType) *consumerFixture {
	t.Helper()
	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	t.Cleanup(closeRig)
	natsInstance := jetstream.NATSInstance{}
	js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	db, err := NewDatabase(cm, &cfg.MSCs.Database)
	require.NoError(t, err)

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	parent := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "parent"})
	child := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
		"body":           "child",
		"m.relationship": map[string]any{"rel_type": constRelType, "event_id": parent.EventID()},
	})
	return &consumerFixture{cfg: cfg, js: js, db: &failingDatabase{Database: db}, parent: parent, child: child}
}

// start runs a consumer until ctx is done or the test ends. NATS refuses to
// fetch with a context that can never be canceled. A stopped consumer cannot
// refuse its message, so the short ack wait bounds how long the message stays
// with it before it is redelivered.
func (f *consumerFixture) start(t *testing.T, ctx context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(ctx)
	t.Cleanup(func() { cancel(context.Canceled) })
	require.NoError(t, newRelationshipConsumer(f.cfg, f.js, f.db).Start(ctx, nats.AckWait(time.Second)))
}

func (f *consumerFixture) publish(t *testing.T, output roomserver.OutputEvent) {
	t.Helper()
	testrig.MustPublishMsgs(t, f.js, testrig.NewOutputEventMsg(t, f.cfg, f.child.RoomID().String(), output))
}

func (f *consumerFixture) publishChild(t *testing.T) {
	t.Helper()
	f.publish(t, roomserver.OutputEvent{
		Type:         roomserver.OutputTypeNewRoomEvent,
		NewRoomEvent: &roomserver.OutputNewRoomEvent{Event: f.child},
	})
}

func (f *consumerFixture) children(t *testing.T) []eventInfo {
	t.Helper()
	children, err := f.db.ChildrenForParent(context.Background(), f.parent.EventID(), constRelType, true)
	require.NoError(t, err)
	return children
}

// waitIndexed waits until the consumer has acknowledged every message, so
// nothing is left pending for redelivery.
func (f *consumerFixture) waitIndexed(t *testing.T) {
	t.Helper()
	waitConsumed(t, f.js, f.cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent), f.cfg.Global.JetStream.Durable(consumerName)+"Pull")
}

// waitConsumed waits until the durable has acknowledged every message
// published to the stream so far. The consumer's pending counts alone are
// not enough: the server updates them after a publish asynchronously, so
// right after publishing they can read zero before the consumer has seen
// anything. The ack floor reaching the stream's last sequence cannot.
func waitConsumed(t *testing.T, js nats.JetStreamContext, stream, durable string) {
	t.Helper()
	streamInfo, err := js.StreamInfo(stream)
	require.NoError(t, err)
	last := streamInfo.State.LastSeq
	require.Eventually(t, func() bool {
		info, err := js.ConsumerInfo(stream, durable)
		return err == nil && info.AckFloor.Stream >= last && info.NumAckPending == 0
	}, 10*time.Second, 20*time.Millisecond, "relationship consumer did not acknowledge every message up to stream sequence %d", last)
}

func TestRelationshipConsumerIndexesNewAndOldEvents(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		f := newConsumerFixture(t, dbType)
		f.start(t, context.Background())

		// A backfilled child is indexed like a new one.
		f.publish(t, roomserver.OutputEvent{
			Type:         roomserver.OutputTypeOldRoomEvent,
			OldRoomEvent: &roomserver.OutputOldRoomEvent{Event: f.child},
		})
		// Other output types and unparseable messages are acknowledged
		// rather than redelivered forever.
		f.publish(t, roomserver.OutputEvent{
			Type:    roomserver.OutputTypeNewPeek,
			NewPeek: &roomserver.OutputNewPeek{RoomID: f.child.RoomID().String()},
		})
		broken := testrig.NewOutputEventMsg(t, f.cfg, f.child.RoomID().String(), roomserver.OutputEvent{Type: roomserver.OutputTypeNewRoomEvent})
		broken.Data = []byte("{")
		testrig.MustPublishMsgs(t, f.js, broken)

		f.waitIndexed(t)
		children := f.children(t)
		require.Len(t, children, 1)
		require.Equal(t, f.child.EventID(), children[0].EventID)
	})
}

// A failed index write must not be lost: the event is redelivered until the
// write succeeds, and indexing it again after that changes nothing.
func TestRelationshipConsumerRetriesFailedWrites(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		f := newConsumerFixture(t, dbType)
		f.db.failing.Store(true)
		f.start(t, context.Background())
		f.publishChild(t)

		require.Eventually(t, func() bool { return f.db.attempts.Load() >= 2 }, 10*time.Second, 20*time.Millisecond,
			"a refused relationship write was not redelivered")
		require.Empty(t, f.children(t))

		f.db.failing.Store(false)
		f.waitIndexed(t)
		children := f.children(t)
		require.Len(t, children, 1)
		require.Equal(t, f.child.EventID(), children[0].EventID)

		// A duplicate delivery of the same event, as after a crash between
		// the write and its acknowledgement, leaves a single relationship.
		f.publishChild(t)
		f.waitIndexed(t)
		require.Len(t, f.children(t), 1)
	})
}

// A process that stops before its write succeeds leaves the event pending on
// the durable, so the next owner of the consumer indexes it.
func TestRelationshipConsumerRecoversAfterRestart(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		f := newConsumerFixture(t, dbType)
		f.db.failing.Store(true)
		ctx, stop := context.WithCancelCause(context.Background())
		f.start(t, ctx)
		f.publishChild(t)
		require.Eventually(t, func() bool { return f.db.attempts.Load() >= 1 }, 10*time.Second, 20*time.Millisecond)
		stop(errors.New("the process stopped"))
		require.Empty(t, f.children(t))

		f.db.failing.Store(false)
		f.start(t, context.Background())
		f.waitIndexed(t)
		children := f.children(t)
		require.Len(t, children, 1)
		require.Equal(t, f.child.EventID(), children[0].EventID)
	})
}
