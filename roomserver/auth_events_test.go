// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"crypto/ed25519"
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

// eventAuthFederationAPI answers /event_auth with a fixed auth chain, and
// /get_missing_events with its last event.
type eventAuthFederationAPI struct {
	fsAPI.RoomserverFederationAPI
	chain   []*types.HeaderedEvent
	keyRing *gomatrixserverlib.KeyRing
}

func (f eventAuthFederationAPI) KeyRing() *gomatrixserverlib.KeyRing { return f.keyRing }

func (eventAuthFederationAPI) QueryJoinedHostServerNamesInRoom(
	context.Context, *fsAPI.QueryJoinedHostServerNamesInRoomRequest, *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	return nil
}

func (eventAuthFederationAPI) IsServerBackingOff(spec.ServerName) bool { return false }

// LookupMissingEvents serves the last event of the chain, which the event
// being processed also has as its prev event.
func (f eventAuthFederationAPI) LookupMissingEvents(
	context.Context, spec.ServerName, spec.ServerName, string, fclient.MissingEvents, gomatrixserverlib.RoomVersion,
) (fclient.RespMissingEvents, error) {
	return fclient.RespMissingEvents{Events: gomatrixserverlib.EventJSONs{f.chain[len(f.chain)-1].JSON()}}, nil
}

func (f eventAuthFederationAPI) GetEventAuth(
	context.Context, spec.ServerName, spec.ServerName, gomatrixserverlib.RoomVersion, string, string,
) (fclient.RespEventAuth, error) {
	var res fclient.RespEventAuth
	for _, ev := range f.chain {
		res.AuthEvents = append(res.AuthEvents, ev.JSON())
	}
	return res, nil
}

// An auth event fetched over federation must be stored under its own state
// key. It used to get the state key of the event it authorizes, so a
// membership fetched for a message, which has no state key, was stored as if
// it were not state at all.
func TestFetchedAuthEventKeepsItsStateKey(t *testing.T) {
	alice := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := room.Events()
		// Bob's display name change never reached this server, but his next
		// message cites it as an auth event.
		rename := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join, "displayname": "Bob"}, test.WithStateKey(bob.ID))
		message := room.CreateEvent(t, bob, "m.room.message", map[string]any{"body": "hello"})
		require.Contains(t, message.AuthEventIDs(), rename.EventID())

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
		require.True(t, ok)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}}
		rsAPI.SetFederationAPI(eventAuthFederationAPI{chain: append(known, rename), keyRing: keyRing}, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, known, "test", "test", "test", nil, false))

		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{message}, "test", remoteServer, "test", nil, false))

		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)
		entries, err := internalAPI.DB.StateEntriesForEventIDs(ctx, []string{rename.EventID()}, false)
		require.NoError(t, err)
		require.Len(t, entries, 1, "the fetched auth event was not stored")
		keys, err := internalAPI.DB.EventStateKeyNIDs(ctx, []string{bob.ID})
		require.NoError(t, err)
		require.NotZero(t, keys[bob.ID])
		require.Equal(t, keys[bob.ID], entries[0].EventStateKeyNID, "the fetched membership was stored under another state key")
	})
}
