// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// terminalAfterBatchesAPI answers the first batches with events, then reports
// that every server lacks the remaining history.
type terminalAfterBatchesAPI struct {
	api.SyncRoomserverAPI
	batches [][]*rstypes.HeaderedEvent
	calls   int
}

func (f *terminalAfterBatchesAPI) PerformBackfill(_ context.Context, _ *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	f.calls++
	if f.calls <= len(f.batches) {
		res.Events = f.batches[f.calls-1]
		res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
		return nil
	}
	return &api.BackfillUnavailableError{Retryable: false, Err: errors.New("no server has these events")}
}

// After an earlier round left a gap open, a definitive "no server has the
// older events" answer means the reachable history ends there. Return the
// events fetched so far as a complete page instead of making the client retry
// the same cursor forever.
func TestBackwardBackfillTerminalAnswerAfterIncompleteIsComplete(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		anchor := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "stored anchor"}, test.WithDepth(1))
		missingParent := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "not fetched"}, test.WithDepth(10))
		backfilled := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "backfilled"},
			test.WithDepth(20), test.WithPrevEvents(missingParent.EventID()))
		child := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "stored child"},
			test.WithDepth(35), test.WithPrevEvents(backfilled.EventID()))

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = db.WriteEvent(ctx, anchor, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)
		_, err = db.WriteEvent(ctx, child, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		snapshot, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		require.Contains(t, extremities, child.EventID())

		federation := &terminalAfterBatchesAPI{batches: [][]*rstypes.HeaderedEvent{{backfilled}}}
		req := &messagesReq{
			ctx:              ctx,
			db:               db,
			snapshot:         snapshot,
			rsAPI:            federation,
			cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
			roomID:           room.ID,
			device:           &userapi.Device{UserID: "@alice:test"},
			backwardOrdering: true,
			from:             &types.TopologyToken{Depth: 35},
			filter:           &synctypes.RoomEventFilter{Limit: 10},
		}
		fetched, incomplete, err := req.backfillBackwardUntilComplete(extremities, nil, 10)
		require.NoError(t, err)
		require.Len(t, fetched, 1)
		assert.Equal(t, backfilled.EventID(), fetched[0].EventID())
		assert.False(t, incomplete, "a terminal answer ends the reachable history")
		assert.Equal(t, 2, federation.calls, "the terminal answer was requested once")
	})
}
