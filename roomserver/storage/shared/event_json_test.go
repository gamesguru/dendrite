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
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
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

// seedRedaction records a redaction of eventID without going through event
// processing, so the storeEvent duplicate-copy path can be tested directly.
func seedRedaction(t *testing.T, ctx context.Context, db storage.Database, info tables.RedactionInfo) {
	t.Helper()
	switch d := db.(type) {
	case *sqlite3.Database:
		require.NoError(t, d.RedactionsTable.InsertRedaction(ctx, nil, info))
	case *postgres.Database:
		require.NoError(t, d.RedactionsTable.InsertRedaction(ctx, nil, info))
	default:
		t.Fatalf("unsupported database type %T", db)
	}
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

		t.Run("a validated redaction keeps the stored pruned copy", func(t *testing.T) {
			carol := test.NewUser(t)
			full := room.CreateAndInsert(t, carol, spec.MRoomMember,
				map[string]any{"membership": spec.Join, "displayname": "Carol"}, test.WithStateKey(carol.ID))
			pruned, err := types.RedactedCopy(full.PDU)
			require.NoError(t, err)

			nid := storeMember(ctx, t, db, pruned)
			require.Empty(t, storedDisplayName(ctx, t, db, room.Version, nid))
			seedRedaction(t, ctx, db, tables.RedactionInfo{
				Validated:        true,
				RedactsEventID:   full.EventID(),
				RedactionEventID: "$validated-redaction-" + carol.ID,
			})

			require.Equal(t, nid, storeMember(ctx, t, db, full.PDU))
			require.Empty(t, storedDisplayName(ctx, t, db, room.Version, nid),
				"a full copy must not replace the pruned JSON of a validated redaction")
		})

		t.Run("an unvalidated redaction still allows the full copy to write", func(t *testing.T) {
			dave := test.NewUser(t)
			full := room.CreateAndInsert(t, dave, spec.MRoomMember,
				map[string]any{"membership": spec.Join, "displayname": "Dave"}, test.WithStateKey(dave.ID))
			pruned, err := types.RedactedCopy(full.PDU)
			require.NoError(t, err)

			nid := storeMember(ctx, t, db, pruned)
			require.Empty(t, storedDisplayName(ctx, t, db, room.Version, nid))
			seedRedaction(t, ctx, db, tables.RedactionInfo{
				Validated:        false,
				RedactsEventID:   full.EventID(),
				RedactionEventID: "$unvalidated-redaction-" + dave.ID,
			})

			require.Equal(t, nid, storeMember(ctx, t, db, full.PDU))
			require.Equal(t, "Dave", storedDisplayName(ctx, t, db, room.Version, nid),
				"an unvalidated redaction must not block a verified full copy")
		})
	})
}

// A pruned copy stored after its redaction arrived carries no redacted_because
// marker, so the SQL guard alone would let a later complete copy overwrite it
// and undo the redaction. storeEvent must consult the validated redaction.
func TestStoreEventKeepsPrunedJSONCoveredByValidatedRedaction(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.Open(ctx, cm, &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, caches) //nolint:contextcheck
		require.NoError(t, err)
		raw, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, sqlutil.NewExclusiveWriter())
		require.NoError(t, err)
		defer raw.Close() //nolint:errcheck
		insertRedaction := func(t *testing.T, redactsEventID, redactionEventID string, validated bool) {
			t.Helper()
			_, execErr := raw.ExecContext(ctx,
				"INSERT INTO roomserver_redactions (redaction_event_id, redacts_event_id, validated) VALUES ($1, $2, $3)",
				redactionEventID, redactsEventID, validated)
			require.NoError(t, execErr)
		}

		run := func(t *testing.T, displayname string, validated bool, wantName string) {
			t.Helper()
			full := room.CreateAndInsert(t, bob, spec.MRoomMember,
				map[string]any{"membership": spec.Join, "displayname": displayname}, test.WithStateKey(bob.ID))
			pruned, err := types.RedactedCopy(full.PDU)
			require.NoError(t, err)
			require.True(t, pruned.Redacted())

			nid := storeMember(ctx, t, db, pruned)
			require.Empty(t, storedDisplayName(ctx, t, db, room.Version, nid))
			insertRedaction(t, full.EventID(), "$redaction-of-"+displayname, validated)
			has, err := db.HasValidatedRedactionForEvent(ctx, full.EventID())
			require.NoError(t, err)
			require.Equal(t, validated, has)

			require.Equal(t, nid, storeMember(ctx, t, db, full.PDU))
			require.Equal(t, wantName, storedDisplayName(ctx, t, db, room.Version, nid))
		}

		t.Run("a validated redaction keeps the stored pruned copy", func(t *testing.T) {
			run(t, "Covered", true, "")
		})
		t.Run("without a validated redaction the full copy still replaces it", func(t *testing.T) {
			run(t, "Unredacted", false, "Unredacted")
		})
	})
}
