package tables_test

import (
	"context"
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

func mustCreateEventJSONTable(t *testing.T, dbType test.DBType) (tables.EventJSON, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	assert.NoError(t, err)
	var tab tables.EventJSON
	switch dbType {
	case test.DBTypePostgres:
		err = postgres.CreateEventJSONTable(db)
		assert.NoError(t, err)
		tab, err = postgres.PrepareEventJSONTable(db)
	case test.DBTypeSQLite:
		err = sqlite3.CreateEventJSONTable(db)
		assert.NoError(t, err)
		tab, err = sqlite3.PrepareEventJSONTable(db)
	}
	assert.NoError(t, err)

	return tab, close
}

func Test_EventJSONTable(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, close := mustCreateEventJSONTable(t, dbType)
		defer close()

		// create some dummy data
		for i := 0; i < 10; i++ {
			err := tab.InsertEventJSON(
				context.Background(), nil, types.EventNID(i),
				[]byte(fmt.Sprintf(`{"value":%d"}`, i)), false,
			)
			assert.NoError(t, err)
		}

		tests := []struct {
			name      string
			args      []types.EventNID
			wantCount int
		}{
			{
				name:      "select subset of existing NIDs",
				args:      []types.EventNID{1, 2, 3, 4, 5},
				wantCount: 5,
			},
			{
				name:      "select subset of existing/non-existing NIDs",
				args:      []types.EventNID{1, 2, 12, 50},
				wantCount: 2,
			},
			{
				name:      "select single existing NID",
				args:      []types.EventNID{1},
				wantCount: 1,
			},
			{
				name:      "select single non-existing NID",
				args:      []types.EventNID{13},
				wantCount: 0,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				// select a subset of the data
				values, err := tab.BulkSelectEventJSON(context.Background(), nil, tc.args)
				assert.NoError(t, err)
				assert.Equal(t, tc.wantCount, len(values))
				for i, v := range values {
					assert.Equal(t, v.EventNID, types.EventNID(i+1))
					assert.Equal(t, []byte(fmt.Sprintf(`{"value":%d"}`, i+1)), v.EventJSON)
				}
			})
		}
	})
}

// A valid event can carry an escaped NUL in its content. Postgres cannot cast such JSON to
// JSONB, so the duplicate-insert guard must not depend on parsing the stored payload.
func Test_EventJSONTable_DuplicateInsertWithEscapedNUL(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, close := mustCreateEventJSONTable(t, dbType)
		defer close()
		ctx := context.Background()

		nul := []byte(`{"content":{"body":"a\u0000b"},"type":"m.room.message"}`)
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 1, nul, false))
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 1, nul, false), "duplicate insert must not fail")

		// A genuine redaction stays authoritative over a later complete copy.
		redacted := []byte(`{"content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction"}}}`)
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 2, redacted, false))
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 2, nul, false))
		// A pruned copy is replaced by a complete one.
		pruned := []byte(`{"content":{}}`)
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 3, pruned, false))
		require.NoError(t, tab.InsertEventJSON(ctx, nil, 3, nul, false))

		got, err := tab.BulkSelectEventJSON(ctx, nil, []types.EventNID{2, 3})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, redacted, got[0].EventJSON)
		require.Equal(t, nul, got[1].EventJSON)
	})
}
