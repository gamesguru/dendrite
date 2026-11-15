// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

type noReconciliationPeers struct{ fsAPI.RoomserverFederationAPI }

func (noReconciliationPeers) QueryJoinedHostServerNamesInRoom(
	context.Context, *fsAPI.QueryJoinedHostServerNamesInRoomRequest, *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	return nil
}

func (noReconciliationPeers) KeyRing() *gomatrixserverlib.KeyRing { return nil }

func TestQueryRoomReconciliationStateOmitsInvisibleExtremity(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		natsInstance := &jetstream.NATSInstance{}
		_, _ = natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(noReconciliationPeers{}, nil)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		if err := api.SendEvents(processCtx.Context(), rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
			t.Fatalf("send room events: %v", err)
		}

		res := &api.QueryRoomReconciliationStateResponse{}
		if err := rsAPI.QueryRoomReconciliationState(processCtx.Context(), &api.QueryRoomReconciliationStateRequest{
			RoomID:     room.ID,
			ServerName: spec.ServerName("remote.example"),
		}, res); err != nil {
			t.Fatalf("query reconciliation state: %v", err)
		}
		if len(res.ForwardExtremities) != 0 {
			t.Fatalf("invisible forward extremities were returned: %v", res.ForwardExtremities)
		}
	})
}
