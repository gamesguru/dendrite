package shared_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
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
