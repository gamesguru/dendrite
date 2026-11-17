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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rsinternal "codefloe.com/pat-s/zendrite/roomserver/internal"
	rsinput "codefloe.com/pat-s/zendrite/roomserver/internal/input"
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
	hidden        map[string]*types.HeaderedEvent
	stateIDs      []string
	stateHits     atomic.Int32
	eventHits     atomic.Int32
	eventHitsByID map[string]int
	mu            sync.Mutex
	eventCopies   map[string][][]byte
	eventServers  map[string][]spec.ServerName
	joinedHosts   []spec.ServerName
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

func (f *hiddenPrevFederation) QueryJoinedHostServerNamesInRoom(
	_ context.Context, _ *fsAPI.QueryJoinedHostServerNamesInRoomRequest, res *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	res.ServerNames = append([]spec.ServerName(nil), f.joinedHosts...)
	return nil
}

func (f *hiddenPrevFederation) GetEvent(
	_ context.Context, _, serverName spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ev, ok := f.hidden[eventID]
	if !ok {
		return gomatrixserverlib.Transaction{}, errors.New("remote.test: 404 not found")
	}
	f.eventHits.Add(1)
	if f.eventServers != nil {
		f.eventServers[eventID] = append(f.eventServers[eventID], serverName)
	}
	if copies := f.eventCopies[eventID]; len(copies) > 0 {
		if f.eventHitsByID == nil {
			f.eventHitsByID = map[string]int{}
		}
		index := f.eventHitsByID[eventID]
		f.eventHitsByID[eventID] = index + 1
		if index >= len(copies) {
			index = len(copies) - 1
		}
		return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{json.RawMessage(copies[index])}}, nil
	}
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

// A hash-pruned copy from one resident should not beat a complete copy from
// another, but it remains a last-resort fallback so the DAG does not retain a
// missing prev event when every resident has only the pruned copy.
func TestMissingPrevEventPrefersFullCopyAndFallsBackToPruned(t *testing.T) {
	alice := test.NewUser(t, test.WithSigningServer("test", "ed25519:test", test.PrivateKeyB))
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	charlie := test.NewUser(t, test.WithSigningServer("other.test", "ed25519:other", test.PrivateKeyA))
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		allPruned bool
	}{
		{name: "prefers full copy"},
		{name: "stores pruned fallback", allPruned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
				room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
				bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
				room.CreateAndInsert(t, charlie, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(charlie.ID))
				known := len(room.Events())
				stateIDs := make([]string, 0, known+1)
				for _, ev := range room.Events()[:known] {
					if ev.StateKey() != nil {
						stateIDs = append(stateIDs, ev.EventID())
					}
				}
				hidden := room.CreateAndInsert(t, bob, spec.MRoomMember,
					map[string]any{"membership": spec.Join, "displayname": "hidden"}, test.WithStateKey(bob.ID))
				for i, stateEventID := range stateIDs {
					if stateEventID == bobJoin.EventID() {
						stateIDs[i] = hidden.EventID()
					}
				}
				live := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "live"})
				require.Equal(t, []string{hidden.EventID()}, live.PrevEventIDs())

				pruned, err := sjson.SetBytes(hidden.JSON(), "content.body", "tampered")
				require.NoError(t, err)
				copies := [][]byte{pruned, hidden.JSON()}
				if tc.allPruned {
					copies = [][]byte{pruned}
				}

				cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
				defer closeDB()
				cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
				caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
				natsInstance := jetstream.NATSInstance{}
				rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
				keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: serverKeys{"test": test.PrivateKeyB, remoteServer: test.PrivateKeyA, "other.test": test.PrivateKeyA}}
				federation := &hiddenPrevFederation{
					remoteFederation: remoteFederation{room: room, keyRing: keyRing},
					hidden:           map[string]*types.HeaderedEvent{hidden.EventID(): hidden},
					eventHitsByID:    make(map[string]int),
					stateIDs:         stateIDs,
					eventCopies:      map[string][][]byte{hidden.EventID(): copies},
					eventServers:     make(map[string][]spec.ServerName),
					joinedHosts:      []spec.ServerName{remoteServer, "other.test"},
				}
				rsAPI.SetFederationAPI(federation, keyRing)
				require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
				internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
				require.True(t, ok)
				require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{live}, "test", remoteServer, api.DoNotSendToOtherServers, nil, false))

				federation.mu.Lock()
				servers := append([]spec.ServerName(nil), federation.eventServers[hidden.EventID()]...)
				federation.mu.Unlock()
				require.GreaterOrEqual(t, len(servers), 2, "the second candidate server was not tried")
				require.NotEqual(t, servers[0], servers[1], "expected two distinct candidate servers")
				roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
				require.NoError(t, err)
				stored, err := internalAPI.DB.EventsFromIDs(ctx, roomInfo, []string{hidden.EventID()})
				require.NoError(t, err)
				require.Len(t, stored, 1, "the missing prev event must still be stored")
				if tc.allPruned {
					require.NotContains(t, string(stored[0].Content()), "displayname", "only the pruned fallback should be available")
				} else {
					require.Contains(t, string(stored[0].Content()), "displayname", "the full copy should win over the first pruned copy")
					require.Contains(t, string(stored[0].Content()), "hidden")
				}
			})
		})
	}
}

func TestLookupEventLocalHashPrunedFallbackSurvivesUnavailableFederation(t *testing.T) {
	alice := test.NewUser(t, test.WithSigningServer("test", "ed25519:test", test.PrivateKeyB))
	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		known := len(room.Events())
		hidden := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "full copy"})
		rawPruned, err := sjson.SetBytes(hidden.JSON(), "content.body", "tampered")
		require.NoError(t, err)
		verImpl := gomatrixserverlib.MustGetRoomVersion(room.Version)
		pruned, err := verImpl.NewEventFromUntrustedJSON(rawPruned)
		require.NoError(t, err)
		require.True(t, pruned.Redacted())

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: serverKeys{"test": test.PrivateKeyB}}
		federation := &hiddenPrevFederation{
			remoteFederation: remoteFederation{room: room, keyRing: keyRing},
			hidden:           map[string]*types.HeaderedEvent{},
		}
		rsAPI.SetFederationAPI(federation, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindOutlier, []*types.HeaderedEvent{{PDU: pruned}}, "test", "test", api.DoNotSendToOtherServers, nil, false))

		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)
		roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
		require.NoError(t, err)
		stored, err := internalAPI.DB.EventsFromIDs(ctx, roomInfo, []string{hidden.EventID()})
		require.NoError(t, err)
		require.Len(t, stored, 1)
		require.True(t, eventutil.StoredCopyLooksHashPruned(verImpl, stored[0].JSON()))

		got, err := rsinput.LookupEventLocalFirstForTest(ctx, internalAPI.Inputer, roomInfo, hidden.EventID(), []spec.ServerName{remoteServer})
		require.NoError(t, err, "an unavailable federation source must not turn a previously stored event into a gap")
		require.Equal(t, hidden.EventID(), got.EventID())
		require.NotContains(t, string(got.Content()), "full copy", "the local redacted copy is the fallback when no full copy is available")
	})
}
