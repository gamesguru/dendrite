// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/notifier"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/storage/shared"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// cancellingWriteDB counts the per-room writes a sliding sync response makes
// to its new connection position and cancels the request on the cancelAt-th
// one, which then fails the way a write on a canceled context does. Any write
// attempted after that is counted in lateWrites. Other methods go to the
// embedded database, which is nil for tests that must not reach it.
type cancellingWriteDB struct {
	storage.Database

	cancel     context.CancelCauseFunc
	cancelAt   int
	writes     atomic.Int64
	lateWrites atomic.Int64
	failRoom   string
}

func (db *cancellingWriteDB) write(ctx context.Context, roomID string, real func() error) error {
	if ctx.Err() != nil {
		db.lateWrites.Add(1)
		return fmt.Errorf("write after cancellation: %w", ctx.Err())
	}
	if n := db.writes.Add(1); db.cancelAt > 0 && int(n) == db.cancelAt {
		db.cancel(nil)
		return fmt.Errorf("write: %w", ctx.Err())
	}
	if roomID == db.failRoom {
		return errBrokenDatabase
	}
	if real == nil {
		return nil
	}
	return real()
}

func (db *cancellingWriteDB) UpdateConnectionStream(ctx context.Context, connectionPosition int64, roomID, stream, roomStatus, lastToken string) error {
	return db.write(ctx, roomID, db.realWrite(func(inner storage.Database) error {
		return inner.UpdateConnectionStream(ctx, connectionPosition, roomID, stream, roomStatus, lastToken)
	}))
}

func (db *cancellingWriteDB) UpdateRoomConfig(ctx context.Context, connectionPosition int64, roomID string, timelineLimit int, requiredStateID int64) error {
	return db.write(ctx, roomID, db.realWrite(func(inner storage.Database) error {
		return inner.UpdateRoomConfig(ctx, connectionPosition, roomID, timelineLimit, requiredStateID)
	}))
}

func (db *cancellingWriteDB) realWrite(f func(storage.Database) error) func() error {
	if db.Database == nil {
		return nil
	}
	return func() error { return f(db.Database) }
}

// persistedConnState is a connection state for a response being built at
// position 2 from position 1, with rooms that all have to be carried forward.
func persistedConnState(rooms int) *V4ConnectionState {
	state := &V4ConnectionState{
		ConnectionKey:        1,
		ConnectionPosition:   2,
		PreviousStreamStates: make(map[string]map[string]*types.SlidingSyncStreamState, rooms),
		PreviousRoomConfigs:  make(map[string]*types.SlidingSyncRoomConfig, rooms),
	}
	for i := range rooms {
		roomID := fmt.Sprintf("!room%d:localhost", i)
		state.PreviousStreamStates[roomID] = map[string]*types.SlidingSyncStreamState{
			"events":          {RoomStatus: types.HaveSentRoomLive.String(), LastToken: "s1_1_1_1_1_1_1_1_1"},
			accountDataStream: {RoomStatus: "live", LastToken: "s1_1_1_1_1_1_1_1_1"},
		}
		state.PreviousRoomConfigs[roomID] = &types.SlidingSyncRoomConfig{TimelineLimit: 10, RequiredStateID: 1}
	}
	return state
}

// countLogs returns how many entries at warning level or above contain substr.
func countLogs(hook *logrustest.Hook, substr string) int {
	n := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.WarnLevel && strings.Contains(entry.Message, substr) {
			n++
		}
	}
	return n
}

// The copy-forward steps write one row per room the client was sent before.
// Once the client has gone, each remaining write used to fail with "context
// canceled" and be logged on its own, 80 at a time on production (#464).
func TestCopyForwardStopsAtCancellation(t *testing.T) {
	const rooms = 80
	steps := []struct {
		name string
		run  func(rp *RequestPool, ctx context.Context, state *V4ConnectionState) error
	}{
		{"events stream state", func(rp *RequestPool, ctx context.Context, state *V4ConnectionState) error {
			_, err := rp.copyForwardStreamStates(ctx, state, nil)
			return err
		}},
		{"room config", func(rp *RequestPool, ctx context.Context, state *V4ConnectionState) error {
			return rp.copyForwardRoomConfigs(ctx, state, nil)
		}},
		{"account_data stream state", func(rp *RequestPool, ctx context.Context, state *V4ConnectionState) error {
			return rp.copyForwardAccountDataState(ctx, state)
		}},
	}
	for _, step := range steps {
		for _, tc := range []struct {
			name       string
			cancelAt   int
			preCancel  bool
			wantWrites int64
		}{
			{name: "already canceled", preCancel: true, wantWrites: 0},
			{name: "canceled midway", cancelAt: rooms / 2, wantWrites: rooms / 2},
		} {
			t.Run(step.name+"/"+tc.name, func(t *testing.T) {
				hook := logrustest.NewGlobal()
				defer hook.Reset()

				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				if tc.preCancel {
					cancel(nil)
				}
				db := &cancellingWriteDB{cancel: cancel, cancelAt: tc.cancelAt}
				rp := &RequestPool{db: db}

				err := step.run(rp, ctx, persistedConnState(rooms))

				require.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, tc.wantWrites, db.writes.Load())
				assert.Zero(t, db.lateWrites.Load(), "no write may be attempted once the request is canceled")
				assert.Zero(t, countLogs(hook, "Failed"), "cancellation must not be logged per room")
			})
		}

		t.Run(step.name+"/genuine failure", func(t *testing.T) {
			hook := logrustest.NewGlobal()
			defer hook.Reset()

			db := &cancellingWriteDB{failRoom: "!room3:localhost"}
			rp := &RequestPool{db: db}

			require.NoError(t, step.run(rp, t.Context(), persistedConnState(rooms)))
			assert.EqualValues(t, rooms, db.writes.Load(), "a failed room must not stop the others")
			assert.Equal(t, 1, countLogs(hook, "Failed"), "a genuine failure must stay visible")
		})
	}
}

// panicTransactionDB fails the test if a write transaction is opened.
type panicTransactionDB struct {
	storage.Database
}

func (panicTransactionDB) NewDatabaseTransaction(context.Context) (*shared.DatabaseTransaction, error) {
	panic("a canceled request must not record receipts as delivered")
}

// Delivered receipts are recorded per connection, not per position, so a
// receipt recorded for a response the client never saw would be filtered out
// of the retry as well and never reach the client.
func TestRecordDeliveredReceiptsSkipsCanceledRequest(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	rp := &RequestPool{db: panicTransactionDB{}}

	assert.False(t, rp.recordDeliveredReceipts(ctx, 1, []types.OutputReceiptEvent{{RoomID: "!room:localhost", Type: "m.read"}}))
	assert.True(t, rp.recordDeliveredReceipts(t.Context(), 1, nil), "nothing to record is not a cancellation")
}

// Once the request is canceled, ProcessExtensions stops instead of failing
// every remaining extension against the rolled-back snapshot.
func TestProcessExtensionsStopsAtCancellation(t *testing.T) {
	hook := logrustest.NewGlobal()
	defer hook.Reset()

	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	rp := &RequestPool{db: panicTransactionDB{}}
	req := &types.SlidingSyncRequest{Extensions: &types.ExtensionRequest{
		ToDevice:    &types.ToDeviceRequest{Enabled: true},
		E2EE:        &types.E2EERequest{Enabled: true},
		AccountData: &types.AccountDataRequest{Enabled: true},
		Receipts:    &types.ReceiptsRequest{Enabled: true},
		Typing:      &types.TypingRequest{Enabled: true},
	}}

	_, _, delivered, err := rp.ProcessExtensions(ctx, nil, nil, req, "@alice:localhost", "DEVICE", 1, nil, types.StreamingToken{}, nil, nil, nil)

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, delivered)
	assert.Zero(t, countLogs(hook, "Failed"))
}

// noPartialStateRoomserver answers the one roomserver query a sliding sync
// request without lists or subscriptions makes.
type noPartialStateRoomserver struct {
	roomserverAPI.SyncRoomserverAPI
}

func (noPartialStateRoomserver) GetPartialStateRoomIDs(context.Context) ([]string, error) {
	return nil, nil
}

// QueryRoomsForUser fails, so these tests skip the wait for membership changes.
func (noPartialStateRoomserver) QueryRoomsForUser(context.Context, spec.UserID, string) ([]spec.RoomID, error) {
	return nil, errors.New("not implemented")
}

// A sliding sync request whose client goes away while the response is being
// persisted must not hand out the new position or disturb the one the client
// holds, and a retry from that position must carry every room's state forward
// as if the canceled request had never happened (#464).
func TestSlidingSyncRetryAfterCanceledPersistence(t *testing.T) {
	const rooms = 20
	// The response persists, in order, the events stream state, the room
	// config and the account_data stream state of each room it carries over.
	for _, tc := range []struct {
		name     string
		cancelAt int
	}{
		{name: "during events stream state", cancelAt: rooms / 2},
		{name: "during room config", cancelAt: rooms + rooms/2},
		{name: "during account_data stream state", cancelAt: 2*rooms + rooms/2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
				testSlidingSyncRetryAfterCanceledPersistence(t, dbType, rooms, tc.cancelAt)
			})
		})
	}
}

func testSlidingSyncRetryAfterCanceledPersistence(t *testing.T, dbType test.DBType, rooms, cancelAt int) {
	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	realDB, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
	require.NoError(t, err)

	ctx := t.Context()
	device := &userapi.Device{UserID: "@alice:localhost", ID: "DEVICE"}
	const (
		roomToken     = "s3_1_1_1_1_1_1_1_1"
		accountToken  = "s3_1_1_1_1_7_1_1_1"
		timelineLimit = 10
	)

	// The position the client acknowledged, with every room sent before.
	connectionKey, err := realDB.GetOrCreateConnection(ctx, device.UserID, device.ID, "default")
	require.NoError(t, err)
	acked, err := realDB.CreateConnectionPosition(ctx, connectionKey)
	require.NoError(t, err)
	requiredStateID, err := realDB.GetOrCreateRequiredStateID(ctx, connectionKey, "[]")
	require.NoError(t, err)
	for i := range rooms {
		roomID := fmt.Sprintf("!room%d:localhost", i)
		require.NoError(t, realDB.UpdateConnectionStream(ctx, acked, roomID, "events", types.HaveSentRoomLive.String(), roomToken))
		require.NoError(t, realDB.UpdateConnectionStream(ctx, acked, roomID, accountDataStream, "live", accountToken))
		require.NoError(t, realDB.UpdateRoomConfig(ctx, acked, roomID, timelineLimit, requiredStateID))
	}
	ackedStreams, err := realDB.GetConnectionStreamsByPosition(ctx, acked)
	require.NoError(t, err)
	ackedConfigs, err := realDB.GetRoomConfigsByPosition(ctx, acked)
	require.NoError(t, err)
	ackedPos := types.NewSlidingSyncStreamToken(acked, types.StreamingToken{}).String()

	db := &cancellingWriteDB{Database: realDB}
	rp := &RequestPool{
		db:       db,
		cfg:      &config.SyncAPI{Matrix: &config.Global{}},
		lastseen: &sync.Map{},
		presence: &sync.Map{},
		Notifier: notifier.NewNotifier(nil),
		rsAPI:    noPartialStateRoomserver{},
	}
	rp.lastseen.Store(device.UserID+device.ID, struct{}{})

	doSync := func(ctx context.Context) types.SlidingSyncResponse {
		body, marshalErr := json.Marshal(map[string]any{
			"pos":     ackedPos,
			"timeout": 0,
			"extensions": map[string]any{
				"account_data": map[string]any{"enabled": true},
				"receipts":     map[string]any{"enabled": true},
			},
		})
		require.NoError(t, marshalErr)
		req := httptest.NewRequest(http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", strings.NewReader(string(body))).WithContext(ctx)
		res := rp.OnIncomingSyncRequestV4(req, device)
		require.Equal(t, http.StatusOK, res.Code)
		resp, ok := res.JSON.(types.SlidingSyncResponse)
		require.True(t, ok, "unexpected response %T", res.JSON)
		return resp
	}

	// The client goes away while the response is being persisted.
	hook := logrustest.NewGlobal()
	defer hook.Reset()
	cancelCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	db.cancel, db.cancelAt = cancel, cancelAt
	abandoned := doSync(cancelCtx)

	assert.Equal(t, ackedPos, abandoned.Pos, "an abandoned response must not hand out its partly written position")
	assert.EqualValues(t, cancelAt, db.writes.Load())
	assert.Zero(t, db.lateWrites.Load(), "no write may be attempted once the request is canceled")
	assert.Zero(t, countLogs(hook, ""), "cancellation must be reported once, below warning level")
	streams, err := realDB.GetConnectionStreamsByPosition(ctx, acked)
	require.NoError(t, err)
	assert.Equal(t, ackedStreams, streams, "the acknowledged position must be untouched")
	configs, err := realDB.GetRoomConfigsByPosition(ctx, acked)
	require.NoError(t, err)
	assert.Equal(t, ackedConfigs, configs, "the acknowledged position must be untouched")

	// The client retries from the position it holds.
	db.cancelAt = 0
	retried := doSync(ctx)
	next, err := types.ParseSlidingSyncStreamToken(retried.Pos)
	require.NoError(t, err)
	require.NotEqual(t, acked, next.ConnectionPosition)
	partial := next.ConnectionPosition - 1
	require.Greater(t, partial, acked, "the abandoned request should have created a position")
	require.Error(t, realDB.ValidateConnectionPosition(ctx, partial, connectionKey), "the partly written position must be discarded")

	streams, err = realDB.GetConnectionStreamsByPosition(ctx, next.ConnectionPosition)
	require.NoError(t, err)
	configs, err = realDB.GetRoomConfigsByPosition(ctx, next.ConnectionPosition)
	require.NoError(t, err)
	require.Len(t, streams, rooms)
	require.Len(t, configs, rooms)
	for roomID, roomStreams := range streams {
		events := roomStreams["events"]
		require.NotNil(t, events, roomID)
		assert.Equal(t, types.HaveSentRoomPreviously.String(), events.RoomStatus, roomID)
		assert.Equal(t, roomToken, events.LastToken, "%s: the room must not be advanced past what was sent", roomID)
		accountData := roomStreams[accountDataStream]
		require.NotNil(t, accountData, roomID)
		assert.Equal(t, accountToken, accountData.LastToken, "%s: the account_data floor must not move", roomID)
		assert.Equal(t, timelineLimit, configs[roomID].TimelineLimit, roomID)
		assert.Equal(t, requiredStateID, configs[roomID].RequiredStateID, roomID)
	}
}
