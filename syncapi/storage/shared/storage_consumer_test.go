package shared_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/storage/shared"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

func newSyncDB(t *testing.T, dbType test.DBType) (storage.Database, func()) {
	t.Helper()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	syncDB, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
	if err != nil {
		t.Fatalf("failed to create sync DB: %s", err)
	}

	return syncDB, closeDB
}

func TestReplaceEventJSONPreservesLocalUnsignedAndCurrentStateGuard(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, cleanup := newSyncDB(t, dbType)
		defer cleanup()
		ctx := t.Context()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		const eventType = "com.example.profile"
		const stateKey = "profile"
		writeState := func(event *rstypes.HeaderedEvent, remove []string) {
			_, err := db.WriteEvent(ctx, event, []*rstypes.HeaderedEvent{event}, []string{event.EventID()}, remove, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}
		makePruned := func(event *rstypes.HeaderedEvent, marker string) *rstypes.HeaderedEvent {
			prunedJSON, err := sjson.SetRawBytes(event.JSON(), "content", []byte(`{}`))
			require.NoError(t, err)
			roomVersion, err := gomatrixserverlib.GetRoomVersion(event.Version())
			require.NoError(t, err)
			pdu, err := roomVersion.NewEventFromUntrustedJSON(prunedJSON)
			require.NoError(t, err)
			require.True(t, pdu.Redacted())
			pruned := &rstypes.HeaderedEvent{PDU: pdu}
			require.NoError(t, pruned.SetUnsignedField("prev_content", map[string]string{"marker": marker}))
			return pruned
		}
		storePrunedCopies := func(event *rstypes.HeaderedEvent) {
			transaction, err := db.NewDatabaseTransaction(ctx)
			require.NoError(t, err)
			output := makePruned(event, "output-local")
			state := makePruned(event, "state-local")
			require.NoError(t, transaction.OutputEvents.UpdateEventJSON(ctx, nil, event.EventID(), output))
			_, err = transaction.CurrentRoomState.UpdateEventJSON(ctx, nil, event.EventID(), state)
			require.NoError(t, err)
			require.NoError(t, transaction.Commit())
		}

		initial := room.CreateEvent(t, alice, eventType, map[string]string{"value": "full"}, test.WithStateKey(stateKey))
		writeState(initial, nil)
		storePrunedCopies(initial)
		prunedRows, err := db.Events(ctx, []string{initial.EventID()})
		require.NoError(t, err)
		require.Len(t, prunedRows, 1)
		require.Equal(t, "output-local", gjson.GetBytes(prunedRows[0].Unsigned(), "prev_content.marker").String())
		outStatus, stateStatus, outputHash, stateHash, err := db.ReplaceEventJSON(ctx, initial.EventID(), initial, true)
		require.NoError(t, err)
		require.Equal(t, "would_update_output", outStatus)
		require.Equal(t, "would_update_state", stateStatus)
		require.NotEmpty(t, outputHash)
		require.NotEmpty(t, stateHash)
		stillPruned, err := db.Events(ctx, []string{initial.EventID()})
		require.NoError(t, err)
		require.Len(t, stillPruned, 1)
		require.JSONEq(t, `{}`, string(stillPruned[0].Content()), "dry run must not write the output row")

		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, initial.EventID(), initial, false)
		require.NoError(t, err)
		require.Equal(t, "updated_output", outStatus)
		require.Equal(t, "updated_state", stateStatus)

		output, err := db.Events(ctx, []string{initial.EventID()})
		require.NoError(t, err)
		require.Len(t, output, 1)
		require.JSONEq(t, `{"value":"full"}`, string(output[0].Content()))
		require.Equal(t, "output-local", gjson.GetBytes(output[0].Unsigned(), "prev_content.marker").String())
		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		current, err := snapshot.GetStateEvent(ctx, room.ID, eventType, stateKey)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.NoError(t, snapshot.Rollback())
		require.JSONEq(t, `{"value":"full"}`, string(current.Content()))
		require.Equal(t, "state-local", gjson.GetBytes(current.Unsigned(), "prev_content.marker").String())

		// A newer state event takes the slot before the replacement. The old
		// event may still be refreshed in the timeline but cannot rewind state.
		newer := room.CreateEvent(t, alice, eventType, map[string]string{"value": "newer"}, test.WithStateKey(stateKey))
		writeState(newer, []string{initial.EventID()})
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, initial.EventID(), initial, false)
		require.NoError(t, err)
		require.Equal(t, "updated_output", outStatus)
		require.Equal(t, "not_present", stateStatus)
		snapshot, err = db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		current, err = snapshot.GetStateEvent(ctx, room.ID, eventType, stateKey)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.Equal(t, newer.EventID(), current.EventID())
		require.NoError(t, snapshot.Rollback())

		outputOnly := room.CreateEvent(t, alice, eventType, map[string]string{"value": "output only"}, test.WithStateKey("output-only"))
		writeState(outputOnly, nil)
		transaction, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, transaction.CurrentRoomState.DeleteRoomStateByEventID(ctx, nil, outputOnly.EventID()))
		require.NoError(t, transaction.Commit())
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, outputOnly.EventID(), outputOnly, false)
		require.NoError(t, err)
		require.Equal(t, "updated_output", outStatus)
		require.Equal(t, "not_present", stateStatus)

		stateOnly := room.CreateEvent(t, alice, eventType, map[string]string{"value": "state only"}, test.WithStateKey("state-only"))
		writeState(stateOnly, nil)
		transaction, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, transaction.OutputEvents.DeleteEvent(ctx, nil, stateOnly.EventID()))
		require.NoError(t, transaction.Commit())
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, stateOnly.EventID(), stateOnly, false)
		require.NoError(t, err)
		require.Equal(t, "not_present", outStatus)
		require.Equal(t, "updated_state", stateStatus)

		absent := room.CreateEvent(t, alice, eventType, map[string]string{"value": "absent"}, test.WithStateKey("absent"))
		writeState(absent, nil)
		transaction, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, transaction.OutputEvents.DeleteEvent(ctx, nil, absent.EventID()))
		require.NoError(t, transaction.CurrentRoomState.DeleteRoomStateByEventID(ctx, nil, absent.EventID()))
		require.NoError(t, transaction.Commit())
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, absent.EventID(), absent, false)
		require.NoError(t, err)
		require.Equal(t, "not_present", outStatus)
		require.Equal(t, "not_present", stateStatus)

		redactedLocal := room.CreateEvent(t, alice, eventType, map[string]string{"value": "redacted"}, test.WithStateKey("redacted"))
		writeState(redactedLocal, nil)
		transaction, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		outputRedacted := makePruned(redactedLocal, "output")
		stateRedacted := makePruned(redactedLocal, "state")
		require.NoError(t, outputRedacted.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))
		require.NoError(t, stateRedacted.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))
		require.NoError(t, transaction.OutputEvents.UpdateEventJSON(ctx, nil, redactedLocal.EventID(), outputRedacted))
		_, err = transaction.CurrentRoomState.UpdateEventJSON(ctx, nil, redactedLocal.EventID(), stateRedacted)
		require.NoError(t, err)
		require.NoError(t, transaction.Commit())
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, redactedLocal.EventID(), redactedLocal, false)
		require.NoError(t, err)
		require.Equal(t, "redacted", outStatus)
		require.Equal(t, "redacted", stateStatus)
	})
}

func TestFilterTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, closeDB := newSyncDB(t, dbType)
		defer closeDB()

		// initially create a filter
		filter := &synctypes.Filter{}
		filterID, err := tab.PutFilter(context.Background(), "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		// create the same filter again, we should receive the existing filter
		secondFilterID, err := tab.PutFilter(context.Background(), "alice", filter)
		if err != nil {
			t.Fatal(err)
		}

		if secondFilterID != filterID {
			t.Fatalf("expected second filter to be the same as the first: %s vs %s", filterID, secondFilterID)
		}

		// query the filter again
		targetFilter := &synctypes.Filter{}
		if err = tab.GetFilter(context.Background(), targetFilter, "alice", filterID); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(filter, targetFilter) {
			t.Fatalf("%#v vs %#v", filter, targetFilter)
		}

		// query non-existent filter
		if err = tab.GetFilter(context.Background(), targetFilter, "bob", filterID); err == nil {
			t.Fatalf("expected filter to not exist, but it does exist: %v", targetFilter)
		}
	})
}

func TestIgnores(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		syncDB, closeDB := newSyncDB(t, dbType)
		defer closeDB()

		tab, err := syncDB.NewDatabaseTransaction(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer tab.Rollback() //nolint:errcheck

		ignoredUsers := &types.IgnoredUsers{List: map[string]any{
			bob.ID: "",
		}}
		if err = tab.UpdateIgnoresForUser(context.Background(), alice.ID, ignoredUsers); err != nil {
			t.Fatal(err)
		}

		gotIgnoredUsers, err := tab.IgnoresForUser(context.Background(), alice.ID)
		if err != nil {
			t.Fatal(err)
		}

		// verify the ignored users matches those we stored
		if !reflect.DeepEqual(gotIgnoredUsers, ignoredUsers) {
			t.Fatalf("%#v vs %#v", gotIgnoredUsers, ignoredUsers)
		}

		// Bob doesn't have any ignored users, so should receive sql.ErrNoRows
		if _, err = tab.IgnoresForUser(context.Background(), bob.ID); err == nil {
			t.Fatalf("expected an error but got none")
		}
	})
}

type fakeReceiptTable struct {
	tables.Receipts
	current     string
	selectErrs  []error
	selectCalls int
	upsertCalls int
}

func (f *fakeReceiptTable) SelectUserReceiptEventID(
	context.Context, *sql.Tx, string, string, string, string,
) (string, error) {
	call := f.selectCalls
	f.selectCalls++
	if call < len(f.selectErrs) && f.selectErrs[call] != nil {
		return "", f.selectErrs[call]
	}
	return f.current, nil
}

func (f *fakeReceiptTable) UpsertReceipt(
	context.Context, *sql.Tx, string, string, string, string, string, spec.Timestamp,
) (types.StreamPosition, error) {
	f.upsertCalls++
	return 42, nil
}

type fakeTopologyTable struct {
	tables.Topology
	err error
}

func (f *fakeTopologyTable) SelectPositionInTopology(
	context.Context, *sql.Tx, string,
) (types.StreamPosition, types.StreamPosition, error) {
	return 0, 0, f.err
}

func TestStoreReceiptRetriesBusyLookup(t *testing.T) {
	receipts := &fakeReceiptTable{selectErrs: []error{errors.New("database is locked")}}
	db := &shared.Database{
		Writer:   sqlutil.NewDummyWriter(),
		Receipts: receipts,
		Topology: &fakeTopologyTable{},
	}

	pos, changed, err := db.StoreReceipt(
		context.Background(), "!room:test", "m.read", "@user:test", "$event:test", "", 1,
	)
	if err != nil {
		t.Fatalf("StoreReceipt: %s", err)
	}
	if pos != 42 || !changed {
		t.Fatalf("expected receipt at 42 with changed=true, got %d, %t", pos, changed)
	}
	if receipts.selectCalls != 2 {
		t.Fatalf("expected the busy lookup to be retried, got %d calls", receipts.selectCalls)
	}
	if receipts.upsertCalls != 1 {
		t.Fatalf("expected one upsert, got %d", receipts.upsertCalls)
	}
}

func TestStoreReceiptPropagatesLookupErrors(t *testing.T) {
	lookupErr := errors.New("lookup failed")
	tests := []struct {
		name     string
		receipts *fakeReceiptTable
		topology *fakeTopologyTable
	}{
		{
			name:     "receipt lookup",
			receipts: &fakeReceiptTable{selectErrs: []error{lookupErr}},
			topology: &fakeTopologyTable{},
		},
		{
			name:     "topology lookup",
			receipts: &fakeReceiptTable{current: "$old:test"},
			topology: &fakeTopologyTable{err: lookupErr},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := &shared.Database{
				Writer:   sqlutil.NewDummyWriter(),
				Receipts: tc.receipts,
				Topology: tc.topology,
			}
			pos, changed, err := db.StoreReceipt(
				context.Background(), "!room:test", "m.read", "@user:test", "$event:test", "", 1,
			)
			if !errors.Is(err, lookupErr) {
				t.Fatalf("expected %v, got %v", lookupErr, err)
			}
			if pos != 0 || changed {
				t.Fatalf("expected no stored receipt, got %d, %t", pos, changed)
			}
			if tc.receipts.upsertCalls != 0 {
				t.Fatalf("expected no upsert after a lookup error, got %d", tc.receipts.upsertCalls)
			}
		})
	}
}

func TestStoreReceiptTreatsUnknownTopologyAsNotBackwards(t *testing.T) {
	receipts := &fakeReceiptTable{current: "$old:test"}
	db := &shared.Database{
		Writer:   sqlutil.NewDummyWriter(),
		Receipts: receipts,
		Topology: &fakeTopologyTable{err: sql.ErrNoRows},
	}

	pos, changed, err := db.StoreReceipt(
		context.Background(), "!room:test", "m.read", "@user:test", "$event:test", "", 1,
	)
	if err != nil {
		t.Fatalf("StoreReceipt: %s", err)
	}
	if pos != 42 || !changed {
		t.Fatalf("expected receipt at 42 with changed=true, got %d, %t", pos, changed)
	}
	if receipts.upsertCalls != 1 {
		t.Fatalf("expected one upsert, got %d", receipts.upsertCalls)
	}
}
