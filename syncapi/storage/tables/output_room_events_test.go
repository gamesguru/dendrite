package tables_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func newOutputRoomEventsTable(ctx context.Context, t *testing.T, dbType test.DBType) (tables.Events, *sql.DB, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	var tab tables.Events
	switch dbType {
	case test.DBTypePostgres:
		tab, err = postgres.NewPostgresEventsTable(ctx, db)
	case test.DBTypeSQLite:
		var stream sqlite3.StreamIDStatements
		if err = stream.Prepare(db); err != nil {
			t.Fatalf("failed to prepare stream stmts: %s", err)
		}
		tab, err = sqlite3.NewSqliteEventsTable(ctx, db, &stream)
	}
	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, db, close
}

func TestOutputRoomEventsTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Create test data inside the callback to avoid data races
		// (event methods like EventID() cache on first access)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		tab, db, close := newOutputRoomEventsTable(ctx, t, dbType)
		defer close()
		events := room.Events()
		err := sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			for _, ev := range events {
				_, err := tab.InsertEvent(ctx, txn, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %w", err)
				}
			}
			// order = 2,0,3,1
			wantEventIDs := []string{
				events[2].EventID(), events[0].EventID(), events[3].EventID(), events[1].EventID(),
			}
			gotEvents, err := tab.SelectEvents(ctx, txn, wantEventIDs, nil, true)
			if err != nil {
				return fmt.Errorf("failed to SelectEvents: %w", err)
			}
			gotEventIDs := make([]string, len(gotEvents))
			for i := range gotEvents {
				gotEventIDs[i] = gotEvents[i].EventID()
			}
			if !reflect.DeepEqual(gotEventIDs, wantEventIDs) {
				return fmt.Errorf("SelectEvents\ngot  %v\n want %v", gotEventIDs, wantEventIDs)
			}

			// Test that contains_url is correctly populated
			urlEv := room.CreateEvent(t, alice, "m.text", map[string]any{
				"body": "test.txt",
				"url":  "mxc://test.txt",
			})
			if _, err = tab.InsertEvent(ctx, txn, urlEv, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				return fmt.Errorf("failed to InsertEvent: %w", err)
			}
			wantEventID := []string{urlEv.EventID()}
			t := true
			gotEvents, err = tab.SelectEvents(ctx, txn, wantEventID, &synctypes.RoomEventFilter{Limit: 1, ContainsURL: &t}, true)
			if err != nil {
				return fmt.Errorf("failed to SelectEvents: %w", err)
			}
			gotEventIDs = make([]string, len(gotEvents))
			for i := range gotEvents {
				gotEventIDs[i] = gotEvents[i].EventID()
			}
			if !reflect.DeepEqual(gotEventIDs, wantEventID) {
				return fmt.Errorf("SelectEvents\ngot  %v\n want %v", gotEventIDs, wantEventID)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
	})
}

func TestReindex(t *testing.T) {
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Create test data inside the callback to avoid data races
		// (event methods like EventID() cache on first access)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{
			"name": "my new room name",
		}, test.WithStateKey(""))

		room.CreateAndInsert(t, alice, spec.MRoomTopic, map[string]any{
			"topic": "my new room topic",
		}, test.WithStateKey(""))

		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
			"msgbody": "my room message",
			"type":    "m.text",
		})
		tab, db, close := newOutputRoomEventsTable(ctx, t, dbType)
		defer close()
		err := sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			for _, ev := range room.Events() {
				_, err := tab.InsertEvent(ctx, txn, ev, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return fmt.Errorf("failed to InsertEvent: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			t.Fatalf("err: %s", err)
		}

		events, err := tab.ReIndex(ctx, nil, 10, 0, []string{
			spec.MRoomName,
			spec.MRoomTopic,
			"m.room.message",
		})
		if err != nil {
			t.Fatal(err)
		}

		wantEventCount := 3
		if len(events) != wantEventCount {
			t.Fatalf("expected %d events, got %d", wantEventCount, len(events))
		}
	})
}

// SelectRecentEvents takes a list of rooms and a flag choosing whether events
// excluded from sync are returned. Both queries must accept the room list, which
// the PostgreSQL query used when the flag is false once did not.
func TestSelectRecentEventsExcludedFromSync(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		roomA := test.NewRoom(t, alice)
		roomB := test.NewRoom(t, alice)

		tab, db, close := newOutputRoomEventsTable(ctx, t, dbType)
		defer close()

		visible := roomA.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "visible"})
		hidden := roomA.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hidden"})
		other := roomB.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "other room"})

		var positions []types.StreamPosition
		require.NoError(t, sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			for _, tc := range []struct {
				ev      *rstypes.HeaderedEvent
				exclude bool
			}{{visible, false}, {hidden, true}, {other, false}} {
				pos, err := tab.InsertEvent(ctx, txn, tc.ev, nil, nil, nil, tc.exclude, gomatrixserverlib.HistoryVisibilityShared)
				if err != nil {
					return err
				}
				positions = append(positions, pos)
			}
			return nil
		}))

		rng := types.Range{From: 0, To: positions[len(positions)-1]}
		ids := func(res map[string]types.RecentEvents, roomID string) []string {
			var out []string
			for _, ev := range res[roomID].Events {
				out = append(out, ev.EventID())
			}
			return out
		}
		filter := &synctypes.RoomEventFilter{Limit: 10, Types: &[]string{"m.room.message"}}
		roomIDs := []string{roomA.ID, roomB.ID}

		onlySync, err := tab.SelectRecentEvents(ctx, nil, roomIDs, rng, filter, true, true)
		require.NoError(t, err)
		require.Equal(t, []string{visible.EventID()}, ids(onlySync, roomA.ID), "excluded events are left out for sync")
		require.Equal(t, []string{other.EventID()}, ids(onlySync, roomB.ID))

		all, err := tab.SelectRecentEvents(ctx, nil, roomIDs, rng, filter, true, false)
		require.NoError(t, err)
		require.Equal(t, []string{visible.EventID(), hidden.EventID()}, ids(all, roomA.ID), "excluded events are returned when asked for")
		require.Equal(t, []string{other.EventID()}, ids(all, roomB.ID))
	})
}
