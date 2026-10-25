// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
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

// linearChainFederationAPI answers /state_ids with the state at the last
// fetched event and serves the whole A-E chain from /event.
type linearChainFederationAPI struct {
	missingEventsFederationAPI
	stateIDs []string
	authIDs  []string
	events   map[string]*types.HeaderedEvent
}

func (f linearChainFederationAPI) LookupStateIDs(
	context.Context, spec.ServerName, spec.ServerName, string, string,
) (gomatrixserverlib.StateIDResponse, error) {
	return fclient.RespStateIDs{StateEventIDs: f.stateIDs, AuthEventIDs: f.authIDs}, nil
}

func (f linearChainFederationAPI) GetEvent(
	_ context.Context, _, _ spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	ev, ok := f.events[eventID]
	if !ok {
		return gomatrixserverlib.Transaction{}, fmt.Errorf("unknown event %s", eventID)
	}
	return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{ev.JSON()}}, nil
}

// A gap can be filled by a linear chain of state events that reaches a known
// event (Bob's join) and then the state_ids event. If those state events stay
// outliers, Bob's join remains a forward extremity, and current state is
// resolved between it and the newer branch even though the chain has a single
// ancestor. The fetched chain must be linked like any other backfilled event.
func TestLinearChainFilledFromStateIDsDoesNotLeaveAnOldExtremity(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
	require.True(t, ok)

	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat), test.RoomVersion(gomatrixserverlib.RoomVersionV10))
	join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	known := len(room.Events())

	var chain []*types.HeaderedEvent
	ts := time.Now()
	for _, name := range []string{"A", "B", "C", "D", "E"} {
		ev := room.CreateAndInsert(t, bob, spec.MRoomMember,
			map[string]any{"membership": spec.Join, "displayname": name},
			test.WithStateKey(bob.ID), test.WithTimestamp(ts))
		chain = append(chain, ev)
	}
	stateAtE := room.CurrentState()
	stateIDs := make([]string, 0, len(stateAtE))
	for _, ev := range stateAtE {
		stateIDs = append(stateIDs, ev.EventID())
	}
	authIDs := make([]string, 0, len(chain))
	for _, ev := range chain {
		authIDs = append(authIDs, ev.EventID())
	}

	stateIDsEvent := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "state ids"},
		test.WithPrevEvents(chain[len(chain)-1].EventID()))
	gmeEvent := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "missing"},
		test.WithPrevEvents(stateIDsEvent.EventID()))
	next := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "next"},
		test.WithPrevEvents(gmeEvent.EventID()))

	events := map[string]*types.HeaderedEvent{stateIDsEvent.EventID(): stateIDsEvent}
	for _, ev := range chain {
		events[ev.EventID()] = ev
	}
	rsAPI.SetFederationAPI(linearChainFederationAPI{
		missingEventsFederationAPI: missingEventsFederationAPI{missing: []*types.HeaderedEvent{gmeEvent}},
		stateIDs:                   stateIDs,
		authIDs:                    authIDs,
		events:                     events,
	}, &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}})

	require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
	require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, "test", nil, false))

	internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
	require.True(t, ok)
	roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
	require.NoError(t, err)
	latest, _, _, err := internalAPI.DB.LatestEventIDs(ctx, roomInfo.RoomNID)
	require.NoError(t, err)
	require.NotContains(t, latest, join.EventID(), "Bob's join is still a forward extremity")

	res := &api.QueryCurrentStateResponse{}
	require.NoError(t, rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
		RoomID:      room.ID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomMember, StateKey: bob.ID}},
	}, res))
	current := res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: bob.ID}]
	require.NotNil(t, current)
	require.Equal(t, chain[len(chain)-1].EventID(), current.EventID())
}
