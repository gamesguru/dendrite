package storage_test

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
)

// largeStateEntries is the number of state events the large state tests write.
// The default keeps CI fast; ZENDRITE_LARGE_STATE_ENTRIES=50000 reproduces
// the room from issue #470.
func largeStateEntries(t *testing.T) int {
	t.Helper()
	n := 2000
	if v := os.Getenv("ZENDRITE_LARGE_STATE_ENTRIES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil {
			t.Fatalf("ZENDRITE_LARGE_STATE_ENTRIES=%q: %s", v, err)
		}
	}
	return n
}

// statementCounter reports how many statements PostgreSQL has executed in one
// database, using pg_stat_statements when the server preloads it. Statements
// are the round trips that dominate a large state write over a network.
type statementCounter struct{ db *sql.DB }

func newStatementCounter(t *testing.T, dbType test.DBType, connStr string) *statementCounter {
	t.Helper()
	if dbType != test.DBTypePostgres {
		return nil
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("open statement counter: %s", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		t.Logf("not counting statements: %s", err)
		return nil
	}
	c := &statementCounter{db: db}
	if _, err = c.count(); err != nil {
		t.Logf("not counting statements (start PostgreSQL with shared_preload_libraries=pg_stat_statements): %s", err)
		return nil
	}
	return c
}

func (c *statementCounter) count() (int64, error) {
	var n int64
	err := c.db.QueryRow("SELECT coalesce(sum(calls), 0)::bigint FROM pg_stat_statements" +
		" WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&n)
	return n, err
}

// measure runs f and logs its duration and, where available, the number of
// statements it executed.
func (c *statementCounter) measure(t *testing.T, what string, f func()) {
	t.Helper()
	var before int64
	if c != nil {
		var err error
		if before, err = c.count(); err != nil {
			t.Fatalf("count statements: %s", err)
		}
	}
	start := time.Now()
	f()
	elapsed := time.Since(start)
	if c == nil {
		t.Logf("%s: %s", what, elapsed)
		return
	}
	after, err := c.count()
	if err != nil {
		t.Fatalf("count statements: %s", err)
	}
	// The counting query itself is one statement.
	t.Logf("%s: %s, %d statements", what, elapsed, after-before-1)
}

// TestWriteEventLargeState writes one event carrying the whole state of a
// room with thousands of members, as the sync API does for a state rewrite,
// and checks the result and that a redelivered write changes nothing.
func TestWriteEventLargeState(t *testing.T) {
	n := largeStateEntries(t)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Each backend gets its own room: the backends run in parallel, and
		// WriteEvent sets the history visibility on the events it writes.
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		for i := range n {
			room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite},
				test.WithStateKey(fmt.Sprintf("@member%d:test", i)))
		}
		ev := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "large"}, test.WithStateKey(""))
		state := room.CurrentState()
		stateIDs := make([]string, len(state))
		for i, s := range state {
			stateIDs[i] = s.EventID()
		}

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(ctx, cm, &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)})
		if err != nil {
			t.Fatalf("NewSyncServerDatasource: %s", err)
		}
		counter := newStatementCounter(t, dbType, connStr)

		write := func() {
			if _, err := db.WriteEvent(ctx, ev, state, stateIDs, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				t.Fatalf("WriteEvent: %s", err)
			}
		}
		counter.measure(t, fmt.Sprintf("WriteEvent with %d state events", len(state)), write)
		// The owner can die after the write commits but before the message is
		// acknowledged, so the same write is delivered again.
		counter.measure(t, "redelivered WriteEvent", write)

		WithSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			filter := synctypes.DefaultStateFilter()
			filter.Limit = len(state) + 1
			got, err := snapshot.GetStateEventsForRoom(ctx, room.ID, &filter)
			if err != nil {
				t.Fatalf("GetStateEventsForRoom: %s", err)
			}
			if len(got) != len(state) {
				t.Fatalf("current state has %d events, want %d", len(got), len(state))
			}
			want := make(map[string]bool, len(state))
			for _, s := range state {
				want[s.EventID()] = true
			}
			for _, s := range got {
				if !want[s.EventID()] {
					t.Fatalf("current state has unexpected event %s", s.EventID())
				}
			}

			// Sample the memberships table rather than read it all back.
			for i := 0; i < n; i += max(1, n/500) {
				userID := fmt.Sprintf("@member%d:test", i)
				membership, _, err := snapshot.SelectMembershipForUser(ctx, room.ID, userID, math.MaxInt64)
				if err != nil {
					t.Fatalf("SelectMembershipForUser: %s", err)
				}
				if membership != spec.Invite {
					t.Fatalf("membership of %s is %q, want %q", userID, membership, spec.Invite)
				}
			}

			events, err := snapshot.Events(ctx, []string{ev.EventID()})
			if err != nil {
				t.Fatalf("Events: %s", err)
			}
			if len(events) != 1 {
				t.Fatalf("got %d copies of the written event, want 1", len(events))
			}
		})
	})
}

// TestWriteEventReplacesState checks that the batched current state write
// keeps the replace semantics of the row-by-row one: a removed event's row
// goes, and a later event for the same type and state key wins.
func TestWriteEventReplacesState(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Each backend gets its own room, as in TestWriteEventLargeState.
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		first := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "first"}, test.WithStateKey(""))
		second := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "second"}, test.WithStateKey(""))

		db, closeDB := MustCreateDatabase(t, dbType)
		defer closeDB()
		MustWriteEvents(t, db, room.Events()[:len(room.Events())-2])

		writes := []struct {
			ev      *rstypes.HeaderedEvent
			adds    []*rstypes.HeaderedEvent
			removes []string
		}{
			{first, []*rstypes.HeaderedEvent{first}, nil},
			// Both topics in one write: the later one must win.
			{second, []*rstypes.HeaderedEvent{first, second}, []string{first.EventID()}},
		}
		for _, w := range writes {
			ids := make([]string, len(w.adds))
			for i, a := range w.adds {
				ids[i] = a.EventID()
			}
			if _, err := db.WriteEvent(ctx, w.ev, w.adds, ids, w.removes, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				t.Fatalf("WriteEvent: %s", err)
			}
		}

		WithSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			got, err := snapshot.GetStateEvent(ctx, room.ID, "m.room.topic", "")
			if err != nil {
				t.Fatalf("GetStateEvent: %s", err)
			}
			if got == nil || got.EventID() != second.EventID() {
				t.Fatalf("current topic is %v, want %s", got, second.EventID())
			}
			byKey, err := snapshot.GetStateEvents(ctx, room.ID, []gomatrixserverlib.StateKeyTuple{
				{EventType: "m.room.topic", StateKey: ""},
				{EventType: "m.room.name", StateKey: ""},
				{EventType: spec.MRoomMember, StateKey: alice.ID},
			})
			if err != nil {
				t.Fatalf("GetStateEvents: %s", err)
			}
			if len(byKey) != 2 {
				t.Fatalf("GetStateEvents returned %d events, want 2: %v", len(byKey), byKey)
			}
			if ev := byKey[gomatrixserverlib.StateKeyTuple{EventType: "m.room.topic", StateKey: ""}]; ev == nil || ev.EventID() != second.EventID() {
				t.Fatalf("GetStateEvents topic is %v, want %s", ev, second.EventID())
			}
		})
	})
}

// TestEventsReturnsEveryRequestedEvent asks for more events than SQLite binds
// in one statement. Both backends must return all of them: the sync API
// consumer looks up the whole state of a room this way.
func TestEventsReturnsEveryRequestedEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Each backend gets its own room: the backends run in parallel, and
		// WriteEvent sets the history visibility on the event it writes.
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		for i := range 1200 {
			room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": strconv.Itoa(i), "msgtype": "m.text"})
		}
		ids := make([]string, len(room.Events()))
		for i, ev := range room.Events() {
			ids[i] = ev.EventID()
		}

		db, closeDB := MustCreateDatabase(t, dbType)
		defer closeDB()
		for _, ev := range room.Events() {
			var adds []*rstypes.HeaderedEvent
			var addIDs []string
			if ev.StateKey() != nil {
				adds, addIDs = []*rstypes.HeaderedEvent{ev}, []string{ev.EventID()}
			}
			if _, err := db.WriteEvent(ctx, ev, adds, addIDs, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared); err != nil {
				t.Fatalf("WriteEvent: %s", err)
			}
		}
		got, err := db.Events(ctx, ids)
		if err != nil {
			t.Fatalf("Events: %s", err)
		}
		if len(got) != len(ids) {
			t.Fatalf("Events returned %d events, want %d", len(got), len(ids))
		}
	})
}
