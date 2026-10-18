package tables_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/postgres"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

// annotationTestTables are the tables the annotations table and its
// backfill migration depend on.
type annotationTestTables struct {
	db         *sql.DB
	events     tables.Events
	eventJSON  tables.EventJSON
	eventTypes tables.EventTypes
	redactions tables.Redactions
}

func mustCreateAnnotationDependencies(t *testing.T, dbType test.DBType) (*annotationTestTables, func()) {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	require.NoError(t, err)
	tabs := &annotationTestTables{db: db}
	switch dbType {
	case test.DBTypePostgres:
		require.NoError(t, postgres.CreateEventTypesTable(db))
		require.NoError(t, postgres.CreateEventsTable(db))
		require.NoError(t, postgres.CreateEventJSONTable(db))
		require.NoError(t, postgres.CreateRedactionsTable(db))
		tabs.eventTypes, err = postgres.PrepareEventTypesTable(db)
		require.NoError(t, err)
		tabs.events, err = postgres.PrepareEventsTable(db)
		require.NoError(t, err)
		tabs.eventJSON, err = postgres.PrepareEventJSONTable(db)
		require.NoError(t, err)
		tabs.redactions, err = postgres.PrepareRedactionsTable(db)
	case test.DBTypeSQLite:
		require.NoError(t, sqlite3.CreateEventTypesTable(db))
		require.NoError(t, sqlite3.CreateEventsTable(db))
		require.NoError(t, sqlite3.CreateEventJSONTable(db))
		require.NoError(t, sqlite3.CreateRedactionsTable(db))
		tabs.eventTypes, err = sqlite3.PrepareEventTypesTable(db)
		require.NoError(t, err)
		tabs.events, err = sqlite3.PrepareEventsTable(db)
		require.NoError(t, err)
		tabs.eventJSON, err = sqlite3.PrepareEventJSONTable(db)
		require.NoError(t, err)
		tabs.redactions, err = sqlite3.PrepareRedactionsTable(db)
	}
	require.NoError(t, err)
	return tabs, closeDB
}

// mustCreateAnnotationsTable creates the annotations table, which runs its
// backfill migration over the events stored so far.
func mustCreateAnnotationsTable(t *testing.T, dbType test.DBType, db *sql.DB) tables.Annotations {
	t.Helper()
	var tab tables.Annotations
	var err error
	switch dbType {
	case test.DBTypePostgres:
		require.NoError(t, postgres.CreateAnnotationsTable(db))
		tab, err = postgres.PrepareAnnotationsTable(db)
	case test.DBTypeSQLite:
		require.NoError(t, sqlite3.CreateAnnotationsTable(db))
		tab, err = sqlite3.PrepareAnnotationsTable(db)
	}
	require.NoError(t, err)
	return tab
}

func TestAnnotationsTable(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		deps, closeDB := mustCreateAnnotationDependencies(t, dbType) //nolint:contextcheck
		defer closeDB()
		tab := mustCreateAnnotationsTable(t, dbType, deps.db) //nolint:contextcheck
		const room = types.RoomNID(1)

		exists := func(sender, eventType, key, exclude string) bool {
			t.Helper()
			found, err := tab.SelectAnnotationExists(ctx, nil, room, "$target", sender, eventType, key, exclude)
			require.NoError(t, err)
			return found
		}

		require.NoError(t, tab.InsertAnnotation(ctx, nil, room, "$a1", "$target", "@alice:test", "m.reaction", "👍"))
		// Recording the same annotation again is not an error.
		require.NoError(t, tab.InsertAnnotation(ctx, nil, room, "$a1", "$target", "@alice:test", "m.reaction", "👍"))

		assert.True(t, exists("@alice:test", "m.reaction", "👍", ""))
		assert.False(t, exists("@alice:test", "m.reaction", "👎", ""), "different key")
		assert.False(t, exists("@bob:test", "m.reaction", "👍", ""), "different sender")
		assert.False(t, exists("@alice:test", "m.other", "👍", ""), "different event type")
		assert.False(t, exists("@alice:test", "m.reaction", "👍", "$a1"), "the excluded event")
		found, err := tab.SelectAnnotationExists(ctx, nil, room+1, "$target", "@alice:test", "m.reaction", "👍", "")
		require.NoError(t, err)
		assert.False(t, found, "different room")

		require.NoError(t, tab.DeleteAnnotation(ctx, nil, "$a1"))
		assert.False(t, exists("@alice:test", "m.reaction", "👍", ""), "deleted")

		// An annotation whose redaction is pending is recorded, but not one
		// whose redaction has been validated.
		require.NoError(t, deps.redactions.InsertRedaction(ctx, nil, tables.RedactionInfo{RedactionEventID: "$r2", RedactsEventID: "$a2"}))
		require.NoError(t, tab.InsertAnnotation(ctx, nil, room, "$a2", "$target", "@alice:test", "m.reaction", "pending"))
		assert.True(t, exists("@alice:test", "m.reaction", "pending", ""))
		require.NoError(t, deps.redactions.InsertRedaction(ctx, nil, tables.RedactionInfo{RedactionEventID: "$r3", RedactsEventID: "$a3", Validated: true}))
		require.NoError(t, tab.InsertAnnotation(ctx, nil, room, "$a3", "$target", "@alice:test", "m.reaction", "redacted"))
		assert.False(t, exists("@alice:test", "m.reaction", "redacted", ""))

		// A row left behind by a build that validated the redaction without
		// deleting the row, as after a rollback, does not count.
		require.NoError(t, deps.redactions.MarkRedactionValidated(ctx, nil, "$r2", true))
		assert.False(t, exists("@alice:test", "m.reaction", "pending", ""), "validated redaction without delete")
	})
}

func TestAnnotationsBackfill(t *testing.T) {
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		deps, closeDB := mustCreateAnnotationDependencies(t, dbType) //nolint:contextcheck
		defer closeDB()

		reactionNID, err := deps.eventTypes.InsertEventTypeNID(ctx, nil, "m.reaction")
		require.NoError(t, err)
		otherNID, err := deps.eventTypes.InsertEventTypeNID(ctx, nil, "m.room.message")
		require.NoError(t, err)

		store := func(eventID string, typeNID types.EventTypeNID, rejected bool, content string) {
			t.Helper()
			eventNID, _, err := deps.events.InsertEvent(ctx, nil, 1, typeNID, 1, eventID, nil, 1, rejected)
			require.NoError(t, err)
			eventJSON := fmt.Sprintf(`{"event_id":%q,"type":"m.reaction","sender":"@alice:test","content":%s}`, eventID, content)
			require.NoError(t, deps.eventJSON.InsertEventJSON(ctx, nil, eventNID, []byte(eventJSON)))
		}
		annotation := func(key string) string {
			return fmt.Sprintf(`{"m.relates_to":{"rel_type":"m.annotation","event_id":"$target","key":%q}}`, key)
		}
		store("$kept", reactionNID, false, annotation("kept"))
		store("$rejected", reactionNID, true, annotation("rejected"))
		// A redacted reaction has had its content removed.
		store("$redacted", reactionNID, false, `{}`)
		store("$reference", reactionNID, false, `{"m.relates_to":{"rel_type":"m.reference","event_id":"$target"}}`)
		store("$nokey", reactionNID, false, `{"m.relates_to":{"rel_type":"m.annotation","event_id":"$target","key":1}}`)
		store("$nul", reactionNID, false, `{"m.relates_to":{"rel_type":"m.annotation","event_id":"$target","key":"a\u0000b"}}`)
		store("$message", otherNID, false, annotation("message"))
		// More than one batch of reactions.
		for i := range 1100 {
			store(fmt.Sprintf("$many%d", i), reactionNID, false, annotation(fmt.Sprintf("many%d", i)))
		}

		tab := mustCreateAnnotationsTable(t, dbType, deps.db) //nolint:contextcheck
		exists := func(key string) bool {
			t.Helper()
			found, err := tab.SelectAnnotationExists(ctx, nil, 1, "$target", "@alice:test", "m.reaction", key, "")
			require.NoError(t, err)
			return found
		}
		assert.True(t, exists("kept"))
		assert.True(t, exists("many0"))
		assert.True(t, exists("many1099"))
		assert.False(t, exists("rejected"))
		assert.False(t, exists("message"), "only reactions are backfilled")

		var count int
		require.NoError(t, deps.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM roomserver_annotations").Scan(&count))
		assert.Equal(t, 1101, count)
	})
}
