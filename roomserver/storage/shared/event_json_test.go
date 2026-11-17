package shared_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustCreateFullDatabase(t *testing.T, dbType test.DBType) (storage.Database, func()) {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.Open(context.Background(), cm, &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, caches)
	require.NoError(t, err)
	return db, closeDB
}

// storeMember stores a member event and returns its event NID.
func storeMember(
	ctx context.Context, t *testing.T, db storage.Database, event gomatrixserverlib.PDU,
) types.EventNID {
	t.Helper()
	roomInfo, err := db.GetOrCreateRoomInfo(ctx, event)
	require.NoError(t, err)
	typeNID, err := db.GetOrCreateEventTypeNID(ctx, event.Type())
	require.NoError(t, err)
	keyNID, err := db.GetOrCreateEventStateKeyNID(ctx, event.StateKey())
	require.NoError(t, err)
	nid, _, err := db.StoreEvent(ctx, event, roomInfo, typeNID, keyNID, nil, false)
	require.NoError(t, err)
	return nid
}

func storedDisplayName(ctx context.Context, t *testing.T, db storage.Database, version gomatrixserverlib.RoomVersion, nid types.EventNID) string {
	t.Helper()
	events, err := db.Events(ctx, version, []types.EventNID{nid})
	require.NoError(t, err)
	require.Len(t, events, 1)
	return gjson.GetBytes(events[0].JSON(), "content.displayname").String()
}

// A parse that fails the content hash check yields the pruned form of an event,
// which has no display name or avatar. Storing such a copy of an event that is
// already stored in full must not replace the stored JSON, and a full copy must
// still replace a stored pruned one.
func TestStoreEventDoesNotReplaceFullJSONWithPrunedCopy(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		db, closeDB := mustCreateFullDatabase(t, dbType) //nolint:contextcheck
		defer closeDB()

		t.Run("a pruned copy does not replace a stored full event", func(t *testing.T) {
			full := room.CreateAndInsert(t, bob, spec.MRoomMember,
				map[string]any{"membership": spec.Join, "displayname": "Bob"}, test.WithStateKey(bob.ID))
			pruned, err := types.RedactedCopy(full.PDU)
			require.NoError(t, err)
			require.True(t, pruned.Redacted(), "the copy must be a pruned one for this test to mean anything")
			require.Empty(t, gjson.GetBytes(pruned.JSON(), "content.displayname").String())

			nid := storeMember(ctx, t, db, full.PDU)
			require.Equal(t, "Bob", storedDisplayName(ctx, t, db, room.Version, nid))

			require.Equal(t, nid, storeMember(ctx, t, db, pruned))
			require.Equal(t, "Bob", storedDisplayName(ctx, t, db, room.Version, nid),
				"storing the pruned copy must keep the stored full JSON")
		})

		t.Run("a full copy replaces a stored pruned event", func(t *testing.T) {
			full := room.CreateAndInsert(t, bob, spec.MRoomMember,
				map[string]any{"membership": spec.Join, "displayname": "Robert"}, test.WithStateKey(bob.ID))
			pruned, err := types.RedactedCopy(full.PDU)
			require.NoError(t, err)

			nid := storeMember(ctx, t, db, pruned)
			require.Empty(t, storedDisplayName(ctx, t, db, room.Version, nid))

			require.Equal(t, nid, storeMember(ctx, t, db, full.PDU))
			require.Equal(t, "Robert", storedDisplayName(ctx, t, db, room.Version, nid),
				"a verified full copy must replace a stored pruned one")
		})
	})
}
