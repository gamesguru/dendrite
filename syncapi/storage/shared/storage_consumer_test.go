package shared_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
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

func TestWriteOldEventClassifiesInWriteTransactionAndRetriesClassification(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, cleanup := newSyncDB(t, dbType)
		defer cleanup()
		ctx := t.Context()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		initial := room.CreateEvent(t, alice, spec.MRoomTopic, map[string]string{"topic": "initial"}, test.WithStateKey(""))
		_, err := db.WriteEvent(ctx, initial, []*rstypes.HeaderedEvent{initial}, []string{initial.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		oldEvent := room.CreateEvent(t, alice, spec.MRoomTopic, map[string]string{"topic": "old"}, test.WithStateKey(""))
		classifications := 0
		var expectedExclude bool
		classifier := func(ctx context.Context, state tables.EventStateReader) (bool, error) {
			classifications++
			if classifications == 1 {
				return false, driver.ErrBadConn
			}
			liveStart, ok, err := state.RoomLiveStartPosition(ctx, room.ID)
			if err != nil {
				return false, err
			}
			require.True(t, ok)
			prospective, err := state.ProspectiveEventPosition(ctx, oldEvent)
			if err != nil {
				return false, err
			}
			current, err := state.GetStateEvent(ctx, room.ID, spec.MRoomTopic, "")
			if err != nil {
				return false, err
			}
			require.Equal(t, initial.EventID(), current.EventID())
			require.GreaterOrEqual(t, prospective, liveStart)
			expectedExclude = prospective < liveStart || current.Depth() >= oldEvent.Depth()
			return expectedExclude, nil
		}
		err = sqlutil.RetryTransient(ctx, "test old-event write", func() error {
			_, err := db.WriteOldEvent(ctx, oldEvent, gomatrixserverlib.HistoryVisibilityShared, classifier)
			return err
		})
		require.NoError(t, err)
		require.Equal(t, 2, classifications, "a retry must rerun classification in its new write transaction")

		tx, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		defer tx.Rollback() //nolint:errcheck
		storedDepth, _, err := tx.Topology.SelectPositionInTopology(ctx, nil, oldEvent.EventID())
		require.NoError(t, err)
		var storedExclude bool
		require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT exclude_from_sync FROM syncapi_output_room_events WHERE event_id = $1", oldEvent.EventID()).Scan(&storedExclude))
		require.Equal(t, expectedExclude, storedExclude)
		prospective, err := tx.Topology.ProspectiveEventPosition(ctx, nil, oldEvent)
		require.NoError(t, err)
		require.Equal(t, prospective, storedDepth)
	})
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
			tx, err := db.NewDatabaseTransaction(ctx)
			require.NoError(t, err)
			output := makePruned(event, "output-local")
			state := makePruned(event, "state-local")
			var outputJSON, stateJSON []byte
			require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT headered_event_json FROM syncapi_output_room_events WHERE event_id = $1", event.EventID()).Scan(&outputJSON))
			require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT headered_event_json FROM syncapi_current_room_state WHERE event_id = $1", event.EventID()).Scan(&stateJSON))
			written, err := tx.OutputEvents.UpdateUnredactedEventJSON(ctx, nil, event.EventID(), outputJSON, output)
			require.NoError(t, err)
			require.True(t, written)
			written, err = tx.CurrentRoomState.UpdateUnredactedEventJSON(ctx, nil, event.EventID(), stateJSON, state)
			require.NoError(t, err)
			require.True(t, written)
			_, err = tx.DB.ExecContext(ctx, "UPDATE syncapi_output_room_events SET contains_url = false WHERE event_id = $1", event.EventID())
			require.NoError(t, err)
			_, err = tx.DB.ExecContext(ctx, "UPDATE syncapi_current_room_state SET contains_url = false WHERE event_id = $1", event.EventID())
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
		}

		initial := room.CreateEvent(t, alice, eventType, map[string]string{"value": "full\x00value", "url": "https://example.org"}, test.WithStateKey(stateKey))
		writeState(initial, nil)
		// Both guarded PostgreSQL updates must accept exact stored/expected JSON
		// containing an escaped NUL in event content.
		tx, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		var outputJSON, stateJSON []byte
		require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT headered_event_json FROM syncapi_output_room_events WHERE event_id = $1", initial.EventID()).Scan(&outputJSON))
		require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT headered_event_json FROM syncapi_current_room_state WHERE event_id = $1", initial.EventID()).Scan(&stateJSON))
		written, err := tx.OutputEvents.UpdateUnredactedEventJSON(ctx, nil, initial.EventID(), outputJSON, initial)
		require.NoError(t, err)
		require.True(t, written)
		written, err = tx.CurrentRoomState.UpdateUnredactedEventJSON(ctx, nil, initial.EventID(), stateJSON, initial)
		require.NoError(t, err)
		require.True(t, written)
		require.NoError(t, tx.Commit())

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
		require.JSONEq(t, `{"value":"full\u0000value","url":"https://example.org"}`, string(output[0].Content()))
		require.Equal(t, "output-local", gjson.GetBytes(output[0].Unsigned(), "prev_content.marker").String())
		hasURL := true
		tx, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		filtered, err := tx.OutputEvents.SelectEvents(ctx, nil, []string{initial.EventID()}, &synctypes.RoomEventFilter{ContainsURL: &hasURL, Limit: 10}, true)
		require.NoError(t, err)
		require.Len(t, filtered, 1, "contains_url should be restored with the event JSON")
		require.NoError(t, tx.Rollback())
		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		current, err := snapshot.GetStateEvent(ctx, room.ID, eventType, stateKey)
		require.NoError(t, err)
		require.NotNil(t, current)
		require.NoError(t, snapshot.Rollback())
		require.JSONEq(t, `{"value":"full\u0000value","url":"https://example.org"}`, string(current.Content()))
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
		outputRedacted := makePruned(redactedLocal, "output")
		stateRedacted := makePruned(redactedLocal, "state")
		require.NoError(t, outputRedacted.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))
		require.NoError(t, stateRedacted.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))
		tx, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.OutputEvents.UpdateEventJSON(ctx, nil, redactedLocal.EventID(), outputRedacted))
		stateJSON, err = json.Marshal(stateRedacted)
		require.NoError(t, err)
		_, err = tx.DB.ExecContext(ctx, "UPDATE syncapi_current_room_state SET headered_event_json = $1 WHERE event_id = $2", stateJSON, redactedLocal.EventID())
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		outStatus, stateStatus, _, _, err = db.ReplaceEventJSON(ctx, redactedLocal.EventID(), redactedLocal, false)
		require.NoError(t, err)
		require.Equal(t, "redacted", outStatus)
		require.Equal(t, "redacted", stateStatus)
	})
}

func TestPostgresGuardAcceptsSemanticallyEqualSurrogatePair(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		if dbType != test.DBTypePostgres {
			t.Skip("PostgreSQL jsonb guard behavior")
		}
		db, cleanup := newSyncDB(t, dbType)
		defer cleanup()
		ctx := t.Context()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		event := room.CreateEvent(t, alice, spec.MRoomTopic, map[string]string{"topic": "😀"}, test.WithStateKey(""))
		_, err := db.WriteEvent(ctx, event, []*rstypes.HeaderedEvent{event}, []string{event.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)
		expectedJSON, err := event.MarshalJSON()
		require.NoError(t, err)
		storedJSON := bytes.Replace(expectedJSON, []byte("😀"), []byte(`\ud83d\ude00`), 1)
		require.NotEqual(t, expectedJSON, storedJSON)
		tx, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		for _, table := range []string{"syncapi_output_room_events", "syncapi_current_room_state"} {
			_, err = tx.DB.ExecContext(ctx, "UPDATE "+table+" SET headered_event_json = $1 WHERE event_id = $2", string(storedJSON), event.EventID())
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())

		require.NoError(t, event.SetUnsignedField("age", 1))
		tx, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		written, err := tx.OutputEvents.UpdateUnredactedEventJSON(ctx, nil, event.EventID(), expectedJSON, event)
		require.NoError(t, err)
		require.True(t, written, "output comparison should treat a valid surrogate pair as the same JSON string")
		written, err = tx.CurrentRoomState.UpdateUnredactedEventJSON(ctx, nil, event.EventID(), expectedJSON, event)
		require.NoError(t, err)
		require.True(t, written, "current-state comparison should treat a valid surrogate pair as the same JSON string")
		require.NoError(t, tx.Commit())
	})
}

func TestReplaceEventJSONReindexesRestoredRelation(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, cleanup := newSyncDB(t, dbType)
		defer cleanup()
		ctx := t.Context()
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		parent := room.CreateEvent(t, alice, "com.example.parent", map[string]string{"value": "parent"}, test.WithStateKey(""))
		_, err := db.WriteEvent(ctx, parent, []*rstypes.HeaderedEvent{parent}, []string{parent.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)
		reply := room.CreateEvent(t, alice, "m.room.message", map[string]any{
			"msgtype": "m.text", "body": "reply",
			"m.relates_to": map[string]string{"rel_type": "m.thread", "event_id": parent.EventID()},
		})
		_, err = db.WriteEvent(ctx, reply, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		tx, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Relations.DeleteRelation(ctx, nil, room.ID, reply.EventID()))
		require.NoError(t, tx.Rollback())
		prunedJSON, err := sjson.SetRawBytes(reply.JSON(), "content", []byte(`{}`))
		require.NoError(t, err)
		version, err := gomatrixserverlib.GetRoomVersion(reply.Version())
		require.NoError(t, err)
		prunedPDU, err := version.NewEventFromUntrustedJSON(prunedJSON)
		require.NoError(t, err)
		require.True(t, prunedPDU.Redacted())
		tx, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		var previous []byte
		require.NoError(t, tx.DB.QueryRowContext(ctx, "SELECT headered_event_json FROM syncapi_output_room_events WHERE event_id = $1", reply.EventID()).Scan(&previous))
		pruned := &rstypes.HeaderedEvent{PDU: prunedPDU}
		updated, err := tx.OutputEvents.UpdateUnredactedEventJSON(ctx, nil, reply.EventID(), previous, pruned)
		require.NoError(t, err)
		require.True(t, updated)
		require.NoError(t, tx.Rollback())

		outStatus, stateStatus, _, _, err := db.ReplaceEventJSON(ctx, reply.EventID(), reply, false)
		require.NoError(t, err)
		require.Equal(t, "updated_output", outStatus)
		require.Equal(t, "not_present", stateStatus)
		tx, err = db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		relationTypes, err := tx.Relations.SelectRelationTypesForEvents(ctx, nil, room.ID, []string{parent.EventID()}, []string{"m.thread"}, types.StreamPosition(1<<62))
		require.NoError(t, err)
		require.True(t, relationTypes[parent.EventID()]["m.thread"])
		threadRelations, err := tx.ThreadRelations(ctx, room.ID, types.StreamPosition(1<<62), types.TopologyToken{Depth: types.StreamPosition(1 << 62), PDUPosition: types.StreamPosition(1 << 62)}, 10)
		require.NoError(t, err)
		threadEventIDs := make([]string, 0, len(threadRelations))
		for _, relation := range threadRelations {
			threadEventIDs = append(threadEventIDs, relation.EventID)
		}
		require.Contains(t, threadEventIDs, reply.EventID())
		require.NoError(t, tx.Rollback())
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
