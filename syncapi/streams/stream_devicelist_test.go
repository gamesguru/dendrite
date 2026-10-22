// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type partialJoinRSAPI struct {
	api.SyncRoomserverAPI
	roomID  string
	eventID string
	queried chan struct{}
}

func (r *partialJoinRSAPI) GetPartialStateRoomIDs(context.Context) ([]string, error) {
	return []string{r.roomID}, nil
}

func (r *partialJoinRSAPI) QueryMembershipForUser(
	_ context.Context, _ *api.QueryMembershipForUserRequest, res *api.QueryMembershipForUserResponse,
) error {
	res.IsInRoom = true
	res.EventID = r.eventID
	close(r.queried)
	return nil
}

type topologyDB struct {
	storage.Database
	present atomic.Bool
}

func (d *topologyDB) NewDatabaseSnapshot(context.Context) (storage.DatabaseTransaction, error) {
	return topologySnapshot{DatabaseTransaction: d.Database, present: &d.present}, nil
}

type topologySnapshot struct {
	storage.DatabaseTransaction
	present *atomic.Bool
}

func (s topologySnapshot) EventPositionInTopology(context.Context, string) (types.TopologyToken, error) {
	if !s.present.Load() {
		return types.TopologyToken{}, sql.ErrNoRows
	}
	return types.TopologyToken{}, nil
}

func (s topologySnapshot) Rollback() error {
	return nil
}

// A complete sync must not take its token until a partial-state join has
// reached the sync API, otherwise the next sync treats the room as newly joined.
func TestCompleteSyncWaitsForPartialStateJoin(t *testing.T) {
	db := &topologyDB{}
	rsAPI := &partialJoinRSAPI{
		roomID:  "!room:test",
		eventID: "$join",
		queried: make(chan struct{}),
	}
	provider := &DeviceListStreamProvider{
		DefaultStreamProvider: DefaultStreamProvider{DB: db},
		rsAPI:                 rsAPI,
	}
	req := &types.SyncRequest{
		Context: t.Context(),
		Device:  &userapi.Device{UserID: "@alice:test"},
		Log:     logrus.NewEntry(logrus.StandardLogger()),
	}

	done := make(chan struct{})
	go func() {
		provider.CompleteSync(t.Context(), nil, req)
		close(done)
	}()

	<-rsAPI.queried
	select {
	case <-done:
		t.Fatal("complete sync returned before the join reached the sync API")
	case <-time.After(20 * time.Millisecond):
	}

	db.present.Store(true)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("complete sync did not notice the join")
	}
	require.True(t, db.present.Load())
}
