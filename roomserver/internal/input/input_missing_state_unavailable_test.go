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

// unreachableStateFederation is a hiddenPrevFederation whose /event fails for
// some events, as when the remote server is backing off.
type unreachableStateFederation struct {
	hiddenPrevFederation
	unreachable map[string]bool
	failedHits  atomic.Int32
}

func (f *unreachableStateFederation) GetEvent(
	ctx context.Context, origin, s spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	if f.unreachable[eventID] {
		f.failedHits.Add(1)
		return gomatrixserverlib.Transaction{}, errors.New("server \"remote.test\" is backing off")
	}
	return f.hiddenPrevFederation.GetEvent(ctx, origin, s, eventID)
}

// When one state event at a missing prev event cannot be fetched, the state
// before the new event is unknown. The event must not be accepted on the
// partial state, which would authorise it against the wrong state and reset
// the room's state, and it must not be stored as rejected either, since the
// failure says nothing about the event.
func TestMissingStateEventLeavesEventUnaccepted(t *testing.T) {
	alice := test.NewUser(t, test.WithSigningServer("test", "ed25519:test", test.PrivateKeyB))
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		stateIDs := make([]string, 0, known+1)
		for _, ev := range room.Events()[:known] {
			if ev.StateKey() != nil {
				stateIDs = append(stateIDs, ev.EventID())
			}
		}
		// A state event this server never receives and cannot fetch, then a
		// message it never receives, then a live event referring to that.
		topic := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join, "displayname": "unreachable"}, test.WithStateKey(bob.ID))
		stateIDs = append(stateIDs, topic.EventID())
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
		federation := &unreachableStateFederation{
			hiddenPrevFederation: hiddenPrevFederation{
				remoteFederation: remoteFederation{room: room, keyRing: keyRing},
				hidden: map[string]*types.HeaderedEvent{
					hidden.EventID(): hidden,
					topic.EventID():  topic,
				},
				stateIDs: stateIDs,
			},
			unreachable: map[string]bool{topic.EventID(): true},
		}
		rsAPI.SetFederationAPI(federation, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)

		var before api.QueryLatestEventsAndStateResponse
		require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &before))

		// The input may fail; what matters is what was stored.
		_ = api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{live}, "test", remoteServer, api.DoNotSendToOtherServers, nil, false)
		require.Positive(t, federation.stateHits.Load(), "the state at the missing prev event was not looked up via /state_ids")
		require.Positive(t, federation.failedHits.Load(), "the unreachable state event was not requested via /event")

		roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
		require.NoError(t, err)
		rejected, err := internalAPI.DB.IsEventRejected(ctx, roomInfo.RoomNID, live.EventID())
		if !errors.Is(err, sql.ErrNoRows) {
			require.NoError(t, err)
		}
		require.False(t, rejected, "the live event was stored as rejected")

		var after api.QueryLatestEventsAndStateResponse
		require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &after))
		require.False(t, slices.Contains(after.LatestEvents, live.EventID()),
			"the live event was accepted on partial state; latest events %v", after.LatestEvents)
		require.Equal(t, stateKeys(before.StateEvents), stateKeys(after.StateEvents), "the room's current state changed")
	})
}

func stateKeys(events []*types.HeaderedEvent) map[string]string {
	keys := make(map[string]string, len(events))
	for _, ev := range events {
		raw, _ := json.Marshal([]string{ev.Type(), *ev.StateKey()})
		keys[string(raw)] = ev.EventID()
	}
	return keys
}
