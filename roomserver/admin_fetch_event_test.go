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

// fetchEventFederationAPI serves /event/{id} from a fixed map. When rewrite is
// set it returns that JSON for every request instead (to model a remote that
// answers with the wrong or a tampered event).
type fetchEventFederationAPI struct {
	missingEventsFederationAPI
	events  map[string]*types.HeaderedEvent
	rewrite json.RawMessage
}

func (f fetchEventFederationAPI) GetEvent(
	_ context.Context, _, _ spec.ServerName, eventID string,
) (gomatrixserverlib.Transaction, error) {
	if f.rewrite != nil {
		return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{f.rewrite}}, nil
	}
	ev, ok := f.events[eventID]
	if !ok {
		return gomatrixserverlib.Transaction{}, fmt.Errorf("unknown event %s", eventID)
	}
	return gomatrixserverlib.Transaction{PDUs: []json.RawMessage{ev.JSON()}}, nil
}

// GetEventAuth returns nothing, so an auth event the server lacks stays missing.
func (fetchEventFederationAPI) GetEventAuth(
	context.Context, spec.ServerName, spec.ServerName, gomatrixserverlib.RoomVersion, string, string,
) (fclient.RespEventAuth, error) {
	return fclient.RespEventAuth{}, nil
}

func TestPerformAdminFetchEvent(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		good := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "dropped"})
		// An event whose auth events omit the sender's membership fails auth
		// and is stored rejected.
		var authIDs []string
		for _, ev := range room.CurrentState() {
			switch ev.Type() {
			case spec.MRoomCreate, spec.MRoomPowerLevels, spec.MRoomJoinRules:
				authIDs = append(authIDs, ev.EventID())
			}
		}
		notMember := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "bad auth"},
			test.WithAuthIDs(authIDs))

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
		require.True(t, ok)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}}
		fed := fetchEventFederationAPI{events: map[string]*types.HeaderedEvent{
			good.EventID(): good, notMember.EventID(): notMember,
		}}
		rsAPI.SetFederationAPI(fed, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)

		t.Run("accepted", func(t *testing.T) {
			outcome, err := internalAPI.PerformAdminFetchEvent(ctx, room.ID, good.EventID(), remoteServer)
			require.NoError(t, err)
			require.Equal(t, "accepted", outcome)
		})

		t.Run("rejected", func(t *testing.T) {
			// Input reports the rejection, with its reason, as an error; the
			// "rejected" outcome is only the fallback readback.
			_, err := internalAPI.PerformAdminFetchEvent(ctx, room.ID, notMember.EventID(), remoteServer)
			require.Error(t, err)
			require.True(t, api.IsInputRejected(err), "unexpected error: %v", err)
			require.Contains(t, err.Error(), "not in room")
		})

		t.Run("unknown event is an error", func(t *testing.T) {
			_, err := internalAPI.PerformAdminFetchEvent(ctx, room.ID, "$doesnotexist", remoteServer)
			require.Error(t, err)
		})

		t.Run("remote returns a different event", func(t *testing.T) {
			other := fetchEventFederationAPI{rewrite: good.JSON()}
			rsAPI.SetFederationAPI(other, keyRing)
			_, err := internalAPI.PerformAdminFetchEvent(ctx, room.ID, notMember.EventID(), remoteServer)
			require.Error(t, err)
			require.Contains(t, err.Error(), "wanted")
		})

		t.Run("unknown room", func(t *testing.T) {
			rsAPI.SetFederationAPI(fed, keyRing)
			_, err := internalAPI.PerformAdminFetchEvent(ctx, "!nope:test", good.EventID(), remoteServer)
			require.Error(t, err)
		})
	})
}
