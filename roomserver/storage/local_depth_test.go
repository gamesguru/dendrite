// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustOpen(t *testing.T, dbType test.DBType) (storage.Database, func()) {
	t.Helper()
	conStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.Open(context.Background(), cm, &config.DatabaseOptions{ConnectionString: config.DataSource(conStr)}, caches)
	require.NoError(t, err)
	return db, closeDB
}

// The local depth is a clock derived from prev_events, whatever depth the
// sending server claimed, and it is corrected when missing history arrives.
func TestLocalDepthFollowsPrevEventsNotWireDepth(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		db, closeDB := mustOpen(t, dbType)
		defer closeDB()

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		for i := 0; i < 6; i++ {
			room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "m"})
		}
		events := room.Events()

		store := func(i int) {
			ev := events[i]
			roomInfo, err := db.GetOrCreateRoomInfo(ctx, ev.PDU)
			require.NoError(t, err)
			typeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
			require.NoError(t, err)
			keyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
			require.NoError(t, err)
			_, _, err = db.StoreEvent(ctx, ev.PDU, roomInfo, typeNID, keyNID, []types.EventNID{}, false)
			require.NoError(t, err)
		}
		ids := func(from, to int) []string {
			var out []string
			for _, ev := range events[from:to] {
				out = append(out, ev.EventID())
			}
			return out
		}

		// The newest events arrive first, as after a federated join: their
		// prev events are not stored, so the depth is only provisional.
		for i := 4; i < len(events); i++ {
			store(i)
		}
		depths, err := db.LocalDepths(ctx, ids(4, len(events)))
		require.NoError(t, err)
		assert.Equal(t, int64(1), depths[events[4].EventID()], "provisional depth with no stored prev events")
		assert.Equal(t, int64(2), depths[events[5].EventID()])

		// The history arrives later, oldest last. Each stored event corrects
		// everything that descends from it.
		for i := 3; i >= 0; i-- {
			store(i)
		}
		depths, err = db.LocalDepths(ctx, ids(0, len(events)))
		require.NoError(t, err)
		for i, ev := range events {
			assert.Equal(t, int64(i), depths[ev.EventID()], "event %d", i)
		}
	})
}
