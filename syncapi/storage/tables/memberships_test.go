package tables_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	postgresdeltas "codefloe.com/pat-s/zendrite/syncapi/storage/postgres/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	sqlitedeltas "codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func newMembershipsTable(t *testing.T, dbType test.DBType) (tables.Memberships, *sql.DB, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	var tab tables.Memberships
	switch dbType {
	case test.DBTypePostgres:
		tab, err = postgres.NewPostgresMembershipsTable(db)
	case test.DBTypeSQLite:
		tab, err = sqlite3.NewSqliteMembershipsTable(db)
	}
	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, db, close
}

func TestMembershipHistoryBackfillRecoversOverwrittenTransitions(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		joinAt10 := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		joinAt20 := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join, "displayname": "updated"}, test.WithStateKey(bob.ID))
		leaveAt30 := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))

		table, db, close := newMembershipsTable(t, dbType)
		defer close()
		ctx := context.Background()
		outputEventsSchema := `CREATE TABLE syncapi_output_room_events (id BIGINT PRIMARY KEY, room_id TEXT NOT NULL, event_id TEXT NOT NULL, headered_event_json TEXT NOT NULL, type TEXT NOT NULL, add_state_ids TEXT[])`
		if dbType == test.DBTypeSQLite {
			outputEventsSchema = `CREATE TABLE syncapi_output_room_events (id INTEGER PRIMARY KEY, room_id TEXT NOT NULL, event_id TEXT NOT NULL, headered_event_json TEXT NOT NULL, type TEXT NOT NULL, add_state_ids TEXT)`
		}
		_, err := db.ExecContext(ctx, outputEventsSchema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `CREATE TABLE syncapi_current_room_state (room_id TEXT NOT NULL, type TEXT NOT NULL, state_key TEXT NOT NULL, headered_event_json TEXT NOT NULL)`)
		require.NoError(t, err)
		insertOutput := func(id int, eventID, raw string, addState bool) {
			var addIDs any
			switch {
			case dbType == test.DBTypeSQLite && addState:
				addIDs = `["` + eventID + `"]`
			case dbType == test.DBTypeSQLite:
				addIDs = "" // InsertEvent stores an empty list as an empty string
			case addState:
				addIDs = []string{eventID}
			default:
				addIDs = []string{}
			}
			_, insertErr := db.ExecContext(ctx,
				`INSERT INTO syncapi_output_room_events (id, room_id, event_id, headered_event_json, type, add_state_ids) VALUES ($1, $2, $3, $4, $5, $6)`,
				id, room.ID, eventID, raw, spec.MRoomMember, addIDs,
			)
			require.NoError(t, insertErr)
		}
		for i, event := range []*rstypes.HeaderedEvent{joinAt10, joinAt20, leaveAt30} {
			insertOutput((i+1)*10, event.EventID(), string(event.JSON()), true)
		}
		// An old (backfilled) event is never recorded by the live path, so the
		// backfill must not invent history for it.
		insertOutput(5, "$old", `{"state_key":"@old:test","content":{"membership":"join"}}`, false)
		// Stored content may hold \u0000, which a jsonb cast would reject.
		insertOutput(40, "$nul", `{"state_key":"@nul:test","content":{"membership":"join","displayname":"a\u0000b"}}`, true)
		// Pseudo-ID rooms (MSC4014) store a sender key in state_key, while the
		// live path records the resolved user ID in history and the current
		// state table. The backfill must use the same key.
		carol := test.NewUser(t)
		insertOutput(50, "$pseudo", `{"state_key":"AAAApseudo","content":{"membership":"join"}}`, true)
		_, err = db.ExecContext(ctx,
			`INSERT INTO syncapi_current_room_state (room_id, type, state_key, headered_event_json) VALUES ($1, $2, $3, $4)`,
			room.ID, spec.MRoomMember, carol.ID, `{"state_key":"AAAApseudo","content":{"membership":"join"}}`,
		)
		require.NoError(t, err)

		// A later output event may add an older membership event back to room
		// state. The live writer records it at the later event's position.
		var restoreIDs any = []string{joinAt10.EventID()}
		if dbType == test.DBTypeSQLite {
			restoreIDs = `["` + joinAt10.EventID() + `"]`
		}
		_, err = db.ExecContext(ctx,
			`INSERT INTO syncapi_output_room_events (id, room_id, event_id, headered_event_json, type, add_state_ids) VALUES ($1, $2, $3, $4, $5, $6)`,
			60, room.ID, "$restorer", `{"content":{}}`, "m.room.power_levels", restoreIDs,
		)
		require.NoError(t, err)

		// Simulate the compact rows and incomplete history left by the old
		// backfill: only the latest row for each membership survives.
		for i, event := range []*rstypes.HeaderedEvent{joinAt20, leaveAt30} {
			membership, membershipErr := event.Membership()
			require.NoError(t, membershipErr)
			streamPos := (i + 2) * 10
			_, err = db.ExecContext(ctx,
				`INSERT INTO syncapi_memberships (room_id, user_id, membership, event_id, stream_pos, topological_pos) VALUES ($1, $2, $3, $4, $5, $5)`,
				room.ID, bob.ID, membership, event.EventID(), streamPos,
			)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx,
				`INSERT INTO syncapi_membership_history (room_id, user_id, membership, event_id, stream_pos) VALUES ($1, $2, $3, $4, $5)`,
				room.ID, bob.ID, membership, event.EventID(), streamPos,
			)
			require.NoError(t, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		var backfillErr error
		if dbType == test.DBTypePostgres {
			backfillErr = postgresdeltas.UpBackfillMembershipHistory(ctx, tx)
		} else {
			backfillErr = sqlitedeltas.UpBackfillMembershipHistory(ctx, tx)
		}
		require.NoError(t, backfillErr)
		require.NoError(t, tx.Commit())

		membership, eventID, err := table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, bob.ID, 15)
		require.NoError(t, err)
		require.Equal(t, spec.Join, membership)
		require.Equal(t, joinAt10.EventID(), eventID)

		// Bob's join was restored at position 60, after his leave at 30.
		membership, eventID, err = table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, bob.ID, 65)
		require.NoError(t, err)
		require.Equal(t, spec.Join, membership)
		require.Equal(t, joinAt10.EventID(), eventID, "restored membership must be recorded at the adding event's position")
		membership, _, err = table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, bob.ID, 35)
		require.NoError(t, err)
		require.Equal(t, spec.Leave, membership)

		oldMembership, _, err := table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, "@old:test", 100)
		require.NoError(t, err)
		require.NotEqual(t, spec.Join, oldMembership, "old events must not be backfilled")
		membership, _, err = table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, "@nul:test", 100)
		require.NoError(t, err)
		require.Equal(t, spec.Join, membership)
		membership, eventID, err = table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, carol.ID, 100)
		require.NoError(t, err)
		require.Equal(t, spec.Join, membership)
		require.Equal(t, "$pseudo", eventID, "pseudo-ID history must use the resolved user ID")
	})
}

func TestMembershipsTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Create test data inside the callback to avoid data races
		// (event methods like EventID() cache on first access)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)

		// Create users
		var userEvents []*rstypes.HeaderedEvent
		for _, x := range room.CurrentState() {
			if x.StateKeyEquals(alice.ID) {
				if _, err := x.Membership(); err == nil {
					userEvents = append(userEvents, x)
					break
				}
			}
		}

		if len(userEvents) == 0 {
			t.Fatalf("didn't find creator membership event")
		}

		for i := 0; i < 10; i++ {
			u := test.NewUser(t)

			ev := room.CreateAndInsert(t, u, spec.MRoomMember, map[string]any{
				"membership": "join",
			}, test.WithStateKey(u.ID))
			userEvents = append(userEvents, ev)
		}
		table, _, close := newMembershipsTable(t, dbType)
		defer close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		for _, ev := range userEvents {
			if err := table.UpsertMembership(ctx, nil, ev, types.StreamPosition(ev.Depth()), 1); err != nil {
				t.Fatalf("failed to upsert membership: %s", err)
			}
		}

		testMembershipCount(t, ctx, table, room)
		testUpsert(t, ctx, table, userEvents[0], alice, room)
	})
}

func testMembershipCount(t *testing.T, ctx context.Context, table tables.Memberships, room *test.Room) {
	t.Run("membership counts are correct", func(t *testing.T) {
		// After 10 events, we should have 6 users (5 create related [incl. one member event], 5 member events = 6 users)
		count, err := table.SelectMembershipCount(ctx, nil, room.ID, spec.Join, 10)
		if err != nil {
			t.Fatalf("failed to get membership count: %s", err)
		}
		expectedCount := 6
		if expectedCount != count {
			t.Fatalf("expected member count to be %d, got %d", expectedCount, count)
		}

		// After 100 events, we should have all 11 users
		count, err = table.SelectMembershipCount(ctx, nil, room.ID, spec.Join, 100)
		if err != nil {
			t.Fatalf("failed to get membership count: %s", err)
		}
		expectedCount = 11
		if expectedCount != count {
			t.Fatalf("expected member count to be %d, got %d", expectedCount, count)
		}
	})
}

func testUpsert(t *testing.T, ctx context.Context, table tables.Memberships, membershipEvent *rstypes.HeaderedEvent, user *test.User, room *test.Room) {
	t.Run("upserting works as expected", func(t *testing.T) {
		if err := table.UpsertMembership(ctx, nil, membershipEvent, 1, 1); err != nil {
			t.Fatalf("failed to upsert membership: %s", err)
		}
		membership, pos, err := table.SelectMembershipForUser(ctx, nil, room.ID, user.ID, 1)
		if err != nil {
			t.Fatalf("failed to select membership: %s", err)
		}
		var expectedPos int64 = 1
		if pos != expectedPos {
			t.Fatalf("expected pos to be %d, got %d", expectedPos, pos)
		}
		if membership != spec.Join {
			t.Fatalf("expected membership to be join, got %s", membership)
		}
		// Create a new event which gets upserted and should not cause issues
		ev := room.CreateAndInsert(t, user, spec.MRoomMember, map[string]any{
			"membership": spec.Join,
		}, test.WithStateKey(user.ID))
		// Insert the same event again, but with different positions, which should get updated
		if err = table.UpsertMembership(ctx, nil, ev, 2, 2); err != nil {
			t.Fatalf("failed to upsert membership: %s", err)
		}

		// Verify the position got updated
		membership, pos, err = table.SelectMembershipForUser(ctx, nil, room.ID, user.ID, 10)
		if err != nil {
			t.Fatalf("failed to select membership: %s", err)
		}
		expectedPos = 2
		if pos != expectedPos {
			t.Fatalf("expected pos to be %d, got %d", expectedPos, pos)
		}
		if membership != spec.Join {
			t.Fatalf("expected membership to be join, got %s", membership)
		}

		// If we can't find a membership, it should default to leave
		if membership, _, err = table.SelectMembershipForUser(ctx, nil, room.ID, user.ID, 1); err != nil {
			t.Fatalf("failed to select membership: %s", err)
		}
		if membership != spec.Leave {
			t.Fatalf("expected membership to be leave, got %s", membership)
		}

		// Position-bounded membership reads must retain transitions even when
		// a later event with the same membership overwrites the compact row.
		joinAt10 := room.CreateAndInsert(t, user, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(user.ID))
		joinAt20 := room.CreateAndInsert(t, user, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(user.ID))
		leaveAt30 := room.CreateAndInsert(t, user, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(user.ID))
		for i, ev := range []*rstypes.HeaderedEvent{joinAt10, joinAt20, leaveAt30} {
			position := types.StreamPosition((i + 1) * 10)
			if err = table.UpsertMembership(ctx, nil, ev, position, position); err != nil {
				t.Fatalf("failed to upsert membership history: %s", err)
			}
		}
		membership, eventID, err := table.SelectMembershipForUserAtStreamPosition(ctx, nil, room.ID, user.ID, 15)
		if err != nil {
			t.Fatalf("failed to select membership history: %s", err)
		}
		if membership != spec.Join || eventID != joinAt10.EventID() {
			t.Fatalf("expected join at position 10 when reading at 15, got %s (%s)", membership, eventID)
		}
	})
}

// Moving the event that last changed a membership in the topology moves the
// membership's copy of that position too, and never lowers it.
func TestUpdateMembershipTopologicalPos(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))

		table, _, close := newMembershipsTable(t, dbType)
		defer close()
		ctx := context.Background()
		require.NoError(t, table.UpsertMembership(ctx, nil, join, 5, 10))

		require.NoError(t, table.UpdateMembershipTopologicalPos(ctx, nil, join.EventID(), 20))
		_, pos, err := table.SelectMembershipForUser(ctx, nil, room.ID, bob.ID, 100)
		require.NoError(t, err)
		require.Equal(t, int64(20), pos)

		require.NoError(t, table.UpdateMembershipTopologicalPos(ctx, nil, join.EventID(), 15))
		_, pos, err = table.SelectMembershipForUser(ctx, nil, room.ID, bob.ID, 100)
		require.NoError(t, err)
		require.Equal(t, int64(20), pos, "a position is never lowered")
	})
}
