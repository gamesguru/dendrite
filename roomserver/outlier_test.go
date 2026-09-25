// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// noServersFederationAPI knows no other servers in any room.
type noServersFederationAPI struct {
	fsAPI.RoomserverFederationAPI
}

func (noServersFederationAPI) QueryJoinedHostServerNamesInRoom(
	context.Context, *fsAPI.QueryJoinedHostServerNamesInRoomRequest, *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	return nil
}

// An outlier whose auth events cannot be obtained, e.g. because the only
// copy was badly signed, is rejected rather than failing the batch it came
// with, such as the room state of a join.
func TestOutlierWithUnobtainableAuthEventIsRejected(t *testing.T) {
	alice := test.NewUser(t)
	charlie := test.NewUser(t)
	room := test.NewRoom(t, alice)
	var authIDs []string
	for _, ev := range room.CurrentState() {
		switch ev.Type() {
		case spec.MRoomCreate, spec.MRoomPowerLevels, spec.MRoomJoinRules:
			authIDs = append(authIDs, ev.EventID())
		}
	}
	authIDs = append(authIDs, "$unobtainable")
	outlier := room.CreateEvent(t, charlie, spec.MRoomMember, map[string]any{"membership": spec.Leave},
		test.WithStateKey(charlie.ID), test.WithAuthIDs(authIDs))

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(noServersFederationAPI{}, nil)
	ctx := context.Background()
	require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

	res := api.InputRoomEventsResponse{}
	rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{{Kind: api.KindOutlier, Event: outlier}},
	}, &res)
	require.NoError(t, res.Err())
}
