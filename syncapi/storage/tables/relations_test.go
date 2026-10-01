package tables_test

import (
	"context"
	"database/sql"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func newRelationsTable(t *testing.T, dbType test.DBType) (tables.Relations, *sql.DB, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	if err != nil {
		t.Fatalf("failed to open db: %s", err)
	}

	var tab tables.Relations
	switch dbType {
	case test.DBTypePostgres:
		if _, err = postgres.NewPostgresTopologyTable(db); err != nil {
			t.Fatal(err)
		}
		tab, err = postgres.NewPostgresRelationsTable(db)
	case test.DBTypeSQLite:
		if _, err = sqlite3.NewSqliteTopologyTable(db); err != nil {
			t.Fatal(err)
		}
		var stream sqlite3.StreamIDStatements
		if err = stream.Prepare(db); err != nil {
			t.Fatalf("failed to prepare stream stmts: %s", err)
		}
		tab, err = sqlite3.NewSqliteRelationsTable(db, &stream)
	}
	if err != nil {
		t.Fatalf("failed to make new table: %s", err)
	}
	return tab, db, close
}

func compareRelationsToExpected(t *testing.T, tab tables.Relations, r types.Range, expected []types.RelationEntry) {
	ctx := context.Background()
	relations, _, err := tab.SelectRelationsInRange(ctx, nil, roomID, "a", "", "", r, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations[relType]) != len(expected) {
		t.Fatalf("incorrect number of values returned for range %v (got %d, want %d)", r, len(relations[relType]), len(expected))
	}
	for i := 0; i < len(relations[relType]); i++ {
		got := relations[relType][i]
		want := expected[i]
		if got != want {
			t.Fatalf("range %v position %d should have been %v but got %v", r, i, got, want)
		}
	}
}

const (
	roomID    = "!roomid:server"
	childType = "m.room.something"
	relType   = "m.reaction"
)

func TestRelationsTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, _, close := newRelationsTable(t, dbType)
		defer close()

		// Insert some relations
		for _, child := range []string{"b", "c", "d"} {
			if err := tab.InsertRelation(ctx, nil, roomID, "a", child, childType, relType); err != nil {
				t.Fatal(err)
			}
		}

		// Check the max position, we've inserted three things so it
		// should be 3
		if max, err := tab.SelectMaxRelationID(ctx, nil); err != nil {
			t.Fatal(err)
		} else if max != 3 {
			t.Fatalf("max position should have been 3 but got %d", max)
		}

		// Query some ranges for "a"
		for r, expected := range map[types.Range][]types.RelationEntry{
			{From: 0, To: 10, Backwards: false}: {
				{Position: 1, EventID: "b"},
				{Position: 2, EventID: "c"},
				{Position: 3, EventID: "d"},
			},
			{From: 1, To: 2, Backwards: false}: {
				{Position: 2, EventID: "c"},
			},
			{From: 1, To: 3, Backwards: false}: {
				{Position: 2, EventID: "c"},
				{Position: 3, EventID: "d"},
			},
			{From: 10, To: 0, Backwards: true}: {
				{Position: 3, EventID: "d"},
				{Position: 2, EventID: "c"},
				{Position: 1, EventID: "b"},
			},
			{From: 3, To: 1, Backwards: true}: {
				{Position: 2, EventID: "c"},
				{Position: 1, EventID: "b"},
			},
		} {
			compareRelationsToExpected(t, tab, r, expected) //nolint:contextcheck
		}

		// Now delete one of the relations
		if err := tab.DeleteRelation(ctx, nil, roomID, "c"); err != nil {
			t.Fatal(err)
		}

		// Query some more ranges for "a"
		for r, expected := range map[types.Range][]types.RelationEntry{
			{From: 0, To: 10, Backwards: false}: {
				{Position: 1, EventID: "b"},
				{Position: 3, EventID: "d"},
			},
			{From: 1, To: 2, Backwards: false}: {},
			{From: 1, To: 3, Backwards: false}: {
				{Position: 3, EventID: "d"},
			},
			{From: 10, To: 0, Backwards: true}: {
				{Position: 3, EventID: "d"},
				{Position: 1, EventID: "b"},
			},
			{From: 3, To: 1, Backwards: true}: {
				{Position: 1, EventID: "b"},
			},
		} {
			compareRelationsToExpected(t, tab, r, expected) //nolint:contextcheck
		}

		// Insert some new relations
		for _, child := range []string{"e", "f", "g", "h"} {
			if err := tab.InsertRelation(ctx, nil, roomID, "a", child, childType, relType); err != nil {
				t.Fatal(err)
			}
		}

		// Check the max position, we've inserted four things so it
		// should now be 7
		if max, err := tab.SelectMaxRelationID(ctx, nil); err != nil {
			t.Fatal(err)
		} else if max != 7 {
			t.Fatalf("max position should have been 3 but got %d", max)
		}

		// Query last set of ranges for "a"
		for r, expected := range map[types.Range][]types.RelationEntry{
			{From: 0, To: 10, Backwards: false}: {
				{Position: 1, EventID: "b"},
				{Position: 3, EventID: "d"},
				{Position: 4, EventID: "e"},
				{Position: 5, EventID: "f"},
				{Position: 6, EventID: "g"},
				{Position: 7, EventID: "h"},
			},
			{From: 1, To: 2, Backwards: false}: {},
			{From: 1, To: 3, Backwards: false}: {
				{Position: 3, EventID: "d"},
			},
			{From: 10, To: 0, Backwards: true}: {
				{Position: 7, EventID: "h"},
				{Position: 6, EventID: "g"},
				{Position: 5, EventID: "f"},
				{Position: 4, EventID: "e"},
				{Position: 3, EventID: "d"},
				{Position: 1, EventID: "b"},
			},
			{From: 6, To: 3, Backwards: true}: {
				{Position: 5, EventID: "f"},
				{Position: 4, EventID: "e"},
				{Position: 3, EventID: "d"},
			},
		} {
			compareRelationsToExpected(t, tab, r, expected) //nolint:contextcheck
		}
	})
}

func TestThreadRelationsTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		tab, db, closeDB := newRelationsTable(t, dbType)
		defer closeDB()
		insert := func(room, root, child string, depth, stream int64) {
			require.NoError(t, tab.InsertRelation(ctx, nil, room, root, child, "m.room.message", "m.thread"))
			_, err := db.ExecContext(ctx, `INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, stream_position, room_id) VALUES ($1, $2, $3, $4)`, child, depth, stream, room)
			require.NoError(t, err)
		}
		insert(roomID, "root-a", "a", 10, 1)
		insert(roomID, "root-b", "b", 10, 2)
		insert(roomID, "root-a", "c", 9, 3)
		insert("!other:server", "root-d", "d", 11, 4)
		watermark, err := tab.SelectMaxRelationID(ctx, nil)
		require.NoError(t, err)
		// A late-arriving reply at an old depth is excluded from the snapshot.
		insert(roomID, "root-e", "e", 8, 5)
		before := types.TopologyToken{Depth: 20, PDUPosition: 20}
		var children []string
		for {
			entries, err := tab.SelectThreadRelations(ctx, nil, roomID, types.StreamPosition(watermark), before, 1)
			require.NoError(t, err)
			if len(entries) == 0 {
				break
			}
			require.Len(t, entries, 1)
			children = append(children, entries[0].EventID)
			before = entries[0].Position
		}
		require.Equal(t, []string{"b", "a", "c"}, children)
	})
}

func TestSelectRelationTypesForEvents(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		tab, _, closeDB := newRelationsTable(t, dbType)
		defer closeDB()
		for _, rel := range []struct{ room, parent, child, relType string }{
			{roomID, "a", "a1", "m.thread"},
			{roomID, "a", "a2", "m.thread"},
			{roomID, "a", "a3", "m.replace"},
			{roomID, "b", "b1", "m.annotation"},
			{"!other:server", "c", "c1", "m.reference"},
		} {
			require.NoError(t, tab.InsertRelation(ctx, nil, rel.room, rel.parent, rel.child, "m.room.message", rel.relType))
		}
		watermark, err := tab.SelectMaxRelationID(ctx, nil)
		require.NoError(t, err)
		// A relation inserted after the snapshot's watermark is not reported.
		require.NoError(t, tab.InsertRelation(ctx, nil, roomID, "d", "d1", "m.room.message", "m.reference"))

		relTypes := []string{"m.thread", "m.replace", "m.reference"}
		got, err := tab.SelectRelationTypesForEvents(ctx, nil, roomID, []string{"a", "b", "c", "d", "missing"}, relTypes, types.StreamPosition(watermark))
		require.NoError(t, err)
		require.Equal(t, map[string]map[string]bool{"a": {"m.thread": true, "m.replace": true}}, got)

		got, err = tab.SelectRelationTypesForEvents(ctx, nil, roomID, []string{"a", "d"}, []string{"m.reference"}, types.StreamPosition(watermark+1))
		require.NoError(t, err)
		require.Equal(t, map[string]map[string]bool{"d": {"m.reference": true}}, got)

		got, err = tab.SelectRelationTypesForEvents(ctx, nil, roomID, nil, relTypes, types.StreamPosition(watermark))
		require.NoError(t, err)
		require.Empty(t, got)
	})
}

func TestSelectRelationsInTopologicalRange(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		tab, db, closeDB := newRelationsTable(t, dbType)
		defer closeDB()
		var stream int64
		insert := func(room, parent, child, childType, relType string, depth int64) types.TopologyToken {
			stream++
			require.NoError(t, tab.InsertRelation(ctx, nil, room, parent, child, childType, relType))
			_, err := db.ExecContext(ctx, `INSERT INTO syncapi_output_room_events_topology (event_id, topological_position, stream_position, room_id) VALUES ($1, $2, $3, $4)`, child, depth, stream, room)
			require.NoError(t, err)
			return types.TopologyToken{Depth: types.StreamPosition(depth), PDUPosition: types.StreamPosition(stream)}
		}
		// A late reply backfilled at an older depth sorts before c2 despite arriving later.
		insert(roomID, "p", "c1", "m.room.message", "m.thread", 10)
		c2 := insert(roomID, "p", "c2", "m.room.encrypted", "m.replace", 12)
		g1 := insert(roomID, "c1", "g1", "m.reaction", "m.annotation", 13)
		insert(roomID, "g1", "gg", "m.room.message", "m.reference", 14)
		insert(roomID, "gg", "ggg", "m.room.message", "m.reference", 15)
		insert(roomID, "p", "late", "m.room.message", "m.thread", 11)
		insert("!other:server", "p", "elsewhere", "m.room.message", "m.thread", 16)

		latest := types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}
		earliest := types.TopologyToken{Depth: -1, PDUPosition: -1}
		page := func(relType, eventType string, maxDepth int, from, to types.TopologyToken, backwards bool, limit int) []string {
			entries, err := tab.SelectRelationsInTopologicalRange(ctx, nil, roomID, "p", relType, eventType, maxDepth, from, to, backwards, limit)
			require.NoError(t, err)
			ids := []string{}
			for _, entry := range entries {
				ids = append(ids, entry.EventID)
			}
			return ids
		}
		require.Equal(t, []string{"c2", "late", "c1"}, page("", "", 1, latest, earliest, true, 10))
		require.Equal(t, []string{"gg", "g1", "c2", "late", "c1"}, page("", "", 3, latest, earliest, true, 10), "depth 4 is out of reach")
		require.Equal(t, []string{"late", "c1"}, page("m.thread", "", 3, latest, earliest, true, 10))
		require.Equal(t, []string{"c2"}, page("", "m.room.encrypted", 3, latest, earliest, true, 10))
		require.Equal(t, []string{"g1"}, page("m.annotation", "m.reaction", 3, latest, earliest, true, 10))

		// Backwards pages include from and exclude to; a decremented position resumes after an event.
		require.Equal(t, []string{"gg", "g1"}, page("", "", 3, latest, earliest, true, 2))
		next := g1
		next.Decrement()
		require.Equal(t, []string{"c2", "late"}, page("", "", 3, next, earliest, true, 2))
		require.Equal(t, []string{"gg", "g1"}, page("", "", 3, latest, c2, true, 10))
		// Forward pages exclude from and include to.
		require.Equal(t, []string{"c1", "late"}, page("", "", 3, earliest, latest, false, 2))
		require.Equal(t, []string{"g1", "gg"}, page("", "", 3, c2, latest, false, 10))
		require.Equal(t, []string{"c1", "late", "c2"}, page("", "", 3, earliest, c2, false, 10))
	})
}
