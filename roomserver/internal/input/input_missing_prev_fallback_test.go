// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rsinternal "codefloe.com/pat-s/zendrite/roomserver/internal"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// hiddenPrevFederation answers /get_missing_events with nothing, but serves
// the hidden events via /event and the room state via /state_ids, like a
// server that only offers some history via /backfill.
type hiddenPrevFederation struct {
	remoteFederation
	hidden    map[string]*types.HeaderedEvent
	stateIDs  []string
	stateHits atomic.Int32
	eventHits atomic.Int32
}

func (f *hiddenPrevFederation) LookupStateIDs(
	_ context.Context, _, _ spec.ServerName, _, eventID string,
) (gomatrixserverlib.StateIDResponse, error) {
	if _, ok := f.hidden[eventID]; !ok {
		return nil, errors.New("remote.test: unknown event")
	}
	f.stateHits.Add(1)
	return &fclient.RespStateIDs{StateEventIDs: f.stateIDs, AuthEventIDs: f.stateIDs}, nil
}

func (f *hiddenPrevFederation) GetEvent(
	_ context.Context, _, _ spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	ev, ok := f.hidden[eventID]
	if !ok {
		return gomatrixserverlib.Transaction{}, errors.New("remote.test: 404 not found")
	}
	f.eventHits.Add(1)
	return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{json.RawMessage(ev.JSON())}}, nil
}

// A server may answer /get_missing_events with none of an event's prev_events,
// for instance for history it only offers via /backfill. The event must still
// be accepted, with the state before it looked up from its prev_events via
// /state_ids and /event, rather than dropped: it is a live event and has to
// reach clients.
func TestMissingPrevEventsFallBackToStateIDs(t *testing.T) {
	alice := test.NewUser(t, test.WithSigningServer("test", "ed25519:test", test.PrivateKeyB))
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		stateIDs := make([]string, 0, known)
		for _, ev := range room.Events()[:known] {
			if ev.StateKey() != nil {
				stateIDs = append(stateIDs, ev.EventID())
			}
		}
		// A message this server never receives, and a live event that refers
		// to it.
		hidden := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "hidden"})
		live := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "live"})
		require.Equal(t, []string{hidden.EventID()}, live.PrevEventIDs())

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: serverKeys{"test": test.PrivateKeyB, remoteServer: test.PrivateKeyA}}
		federation := &hiddenPrevFederation{
			remoteFederation: remoteFederation{room: room, keyRing: keyRing},
			hidden:           map[string]*types.HeaderedEvent{hidden.EventID(): hidden},
			stateIDs:         stateIDs,
		}
		rsAPI.SetFederationAPI(federation, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)

		err := api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{live}, "test", remoteServer, api.DoNotSendToOtherServers, nil, false)
		require.NoError(t, err, "the live event was not accepted")
		require.Positive(t, federation.stateHits.Load(), "the state at the missing prev event was not looked up via /state_ids")
		require.Positive(t, federation.eventHits.Load(), "the missing prev event was not fetched via /event")

		roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
		require.NoError(t, err)
		rejected, err := internalAPI.DB.IsEventRejected(ctx, roomInfo.RoomNID, live.EventID())
		if !errors.Is(err, sql.ErrNoRows) {
			require.NoError(t, err)
		}
		require.False(t, rejected, "the live event was stored as rejected")

		var res api.QueryLatestEventsAndStateResponse
		require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &res))
		require.True(t, slices.Contains(res.LatestEvents, live.EventID()), "latest events %v", res.LatestEvents)
	})
}
