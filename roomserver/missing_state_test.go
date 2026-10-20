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

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
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

const remoteServer = spec.ServerName("remote.test")

// missingEventsFederationAPI answers /get_missing_events with fixed events.
type missingEventsFederationAPI struct {
	fsAPI.RoomserverFederationAPI
	missing []*types.HeaderedEvent
}

func (missingEventsFederationAPI) QueryJoinedHostServerNamesInRoom(
	context.Context, *fsAPI.QueryJoinedHostServerNamesInRoomRequest, *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	return nil
}

func (missingEventsFederationAPI) IsServerBackingOff(spec.ServerName) bool { return false }

func (f missingEventsFederationAPI) LookupMissingEvents(
	context.Context, spec.ServerName, spec.ServerName, string, fclient.MissingEvents, gomatrixserverlib.RoomVersion,
) (fclient.RespMissingEvents, error) {
	var res fclient.RespMissingEvents
	for _, ev := range f.missing {
		res.Events = append(res.Events, ev.JSON())
	}
	return res, nil
}

// remoteKeys serves one server's public key, so its events pass signature checks.
type remoteKeys struct {
	public ed25519.PublicKey
}

func (k remoteKeys) FetchKeys(
	_ context.Context, requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	res := map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult{}
	for req := range requests {
		if req.ServerName != remoteServer {
			continue
		}
		res[req] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(k.public)},
			ExpiredTS:    gomatrixserverlib.PublicKeyNotExpired,
			ValidUntilTS: spec.AsTimestamp(time.Now().Add(time.Hour)),
		}
	}
	return res, nil
}

func (remoteKeys) FetcherName() string { return "remoteKeys" }

func (remoteKeys) StoreKeys(context.Context, map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult) error {
	return nil
}

// Filling a gap in a room can fail on this server's own dependencies, as when
// the broker or HA ownership refuses to publish during a PostgreSQL failover.
// That says nothing about the event: it must not be stored as rejected, and
// the room must not go on cooldown, or every following event from the remote
// server is rejected too and none of them is ever delivered.
func TestMissingStateNotRejectedWhenOwnInfrastructureFails(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		// The two messages the server never received, and the one that
		// arrives next and refers to them.
		gap := []*types.HeaderedEvent{
			room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "missed 1"}),
			room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "missed 2"}),
		}
		next := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "next"})

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
		require.True(t, ok)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}}
		rsAPI.SetFederationAPI(missingEventsFederationAPI{missing: gap}, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))

		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)
		producer := internalAPI.OutputProducer
		topic := producer.Topic
		// No stream binds this subject, so publishing the fetched events'
		// output fails the way it does while the broker is unavailable.
		producer.Topic = "nowhere"
		err := api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, "test", nil, false)
		producer.Topic = topic
		require.Error(t, err, "the fetched events' output could not be published")
		require.False(t, api.IsInputRejected(err), "a failure to publish rejected the event: %v", err)

		// Once publishing works again, the same event is accepted, and the
		// missing events with it.
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, "test", nil, false))
		roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
		require.NoError(t, err)
		for _, ev := range append(gap, next) {
			rejected, err := internalAPI.DB.IsEventRejected(ctx, roomInfo.RoomNID, ev.EventID())
			require.NoError(t, err)
			require.False(t, rejected, "event %s was stored as rejected", ev.EventID())
		}
	})
}

// stateIDsFederationAPI answers /get_missing_events with fixed events, and
// /state_ids and /event for the one prev event it leaves out.
type stateIDsFederationAPI struct {
	missingEventsFederationAPI
	stateIDs []string
	events   map[string]*types.HeaderedEvent
}

func (f stateIDsFederationAPI) LookupStateIDs(
	context.Context, spec.ServerName, spec.ServerName, string, string,
) (gomatrixserverlib.StateIDResponse, error) {
	return fclient.RespStateIDs{StateEventIDs: f.stateIDs, AuthEventIDs: f.stateIDs}, nil
}

func (f stateIDsFederationAPI) GetEvent(
	_ context.Context, _, _ spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	ev, ok := f.events[eventID]
	if !ok {
		return gomatrixserverlib.Transaction{}, fmt.Errorf("unknown event %s", eventID)
	}
	return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{ev.JSON()}}, nil
}

// An event whose gap /get_missing_events cannot close gets the state before
// it from /state_ids. That state describes the remote server's branch of the
// room only, so it must be merged with the room's other forward extremities,
// not replace the room's current state the way a federated join does.
// Treating it as a join dropped this server's own branch, so a state change
// made here was lost, and every such event re-sent the room's full state to
// the sync API.
func TestGapFilledStateKeepsOtherForwardExtremities(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		var stateAtJoin []string
		for _, ev := range room.CurrentState() {
			stateAtJoin = append(stateAtJoin, ev.EventID())
		}

		// This server changes the topic, which the remote server has not seen
		// yet. Meanwhile bob sends two messages on the remote branch, and then
		// one that refers to both. /get_missing_events returns only one of
		// them, so the state before the last one comes from /state_ids.
		topic := room.CreateAndInsert(t, alice, spec.MRoomTopic, map[string]any{"topic": "local"}, test.WithStateKey(""))
		returned := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "returned"}, test.WithPrevEvents(join.EventID()))
		withheld := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "withheld"}, test.WithPrevEvents(join.EventID()))
		next := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "next"},
			test.WithPrevEvents(returned.EventID(), withheld.EventID()))

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
		require.True(t, ok)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}}
		rsAPI.SetFederationAPI(stateIDsFederationAPI{
			missingEventsFederationAPI: missingEventsFederationAPI{missing: []*types.HeaderedEvent{returned}},
			stateIDs:                   stateAtJoin,
			events:                     map[string]*types.HeaderedEvent{withheld.EventID(): withheld},
		}, keyRing)
		local := append(append([]*types.HeaderedEvent{}, room.Events()[:known]...), topic)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, local, "test", "test", "test", nil, false))

		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, "test", nil, false))

		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)
		roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
		require.NoError(t, err)
		latest, _, _, err := internalAPI.DB.LatestEventIDs(ctx, roomInfo.RoomNID)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{topic.EventID(), next.EventID()}, latest,
			"the gap-filled event replaced the room's forward extremities")

		res := &api.QueryCurrentStateResponse{}
		require.NoError(t, rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
			RoomID:      room.ID,
			StateTuples: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomTopic, StateKey: ""}},
		}, res))
		current := res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomTopic, StateKey: ""}]
		require.NotNil(t, current, "the gap-filled event reset the room's topic")
		require.Equal(t, topic.EventID(), current.EventID())
	})
}
