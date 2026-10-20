package roomserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

func reaction(targetEventID, key string) map[string]any {
	return map[string]any{"m.relates_to": map[string]any{
		"rel_type": "m.annotation",
		"event_id": targetEventID,
		"key":      key,
	}}
}

// Accepted annotations are indexed by the input path and forgotten once
// redacted, also when the redaction arrived first (MSC2677).
func TestAnnotationIndex(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
	target := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "hi"})
	thumbsUp := room.CreateAndInsert(t, alice, "m.reaction", reaction(target.EventID(), "👍"))
	// The redaction of this reaction is sent before the reaction itself.
	late := room.CreateEvent(t, alice, "m.reaction", reaction(target.EventID(), "late"))
	room.CreateAndInsert(t, alice, spec.MRoomRedaction, map[string]any{"redacts": late.EventID()}, test.WithRedacts(late.EventID()))
	room.InsertEvent(t, late)
	initial := append([]*types.HeaderedEvent{}, room.Events()...)
	bobReaction := room.CreateAndInsert(t, bob, "m.reaction", reaction(target.EventID(), "👍"))
	redaction := room.CreateAndInsert(t, alice, spec.MRoomRedaction, map[string]any{"redacts": thumbsUp.EventID()}, test.WithRedacts(thumbsUp.EventID()))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
		defer closeRig()
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, initial, "test", "test", "test", nil, false))

		exists := func(sender *test.User, key string, txn *api.TransactionID) bool {
			t.Helper()
			found, err := rsAPI.QueryAnnotationExists(ctx, room.ID, target.EventID(), spec.SenderID(sender.ID), "m.reaction", key, txn)
			require.NoError(t, err)
			return found
		}
		assert.True(t, exists(alice, "👍", nil))
		assert.False(t, exists(alice, "👎", nil), "different key")
		assert.False(t, exists(bob, "👍", nil), "different sender")
		assert.False(t, exists(alice, "late", nil), "redacted before it arrived")

		// The annotation a transaction produced does not count against a retry
		// of that transaction.
		txn := &api.TransactionID{SessionID: 1, TransactionID: "txn-1"}
		_, err := api.SendEventForTransaction(ctx, rsAPI, bobReaction, "test", "test", "test", txn)
		require.NoError(t, err)
		assert.True(t, exists(bob, "👍", nil))
		assert.False(t, exists(bob, "👍", txn), "retry of the same transaction")
		assert.True(t, exists(bob, "👍", &api.TransactionID{SessionID: 1, TransactionID: "txn-2"}))

		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{redaction}, "test", "test", "test", nil, false))
		assert.False(t, exists(alice, "👍", nil), "redacted")
	})
}
