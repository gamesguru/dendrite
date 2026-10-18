// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input_test

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
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

// remoteFederation answers /get_missing_events with fixed events, or fails
// the way a remote server that cannot provide them does, and /event_auth from
// the room's events.
type remoteFederation struct {
	fsAPI.RoomserverFederationAPI
	room    *test.Room
	keyRing *gomatrixserverlib.KeyRing
	missing []*types.HeaderedEvent
	failing atomic.Bool
}

func (f *remoteFederation) KeyRing() *gomatrixserverlib.KeyRing { return f.keyRing }

func (f *remoteFederation) GetEventAuth(
	_ context.Context, _, _ spec.ServerName, _ gomatrixserverlib.RoomVersion, _, eventID string,
) (fclient.RespEventAuth, error) {
	byID := map[string]*types.HeaderedEvent{}
	for _, ev := range f.room.Events() {
		byID[ev.EventID()] = ev
	}
	var res fclient.RespEventAuth
	seen := map[string]bool{}
	var walk func(id string)
	walk = func(id string) {
		ev, ok := byID[id]
		if !ok {
			return
		}
		for _, authID := range ev.AuthEventIDs() {
			if !seen[authID] {
				seen[authID] = true
				walk(authID)
				if authEvent, found := byID[authID]; found {
					res.AuthEvents = append(res.AuthEvents, authEvent.JSON())
				}
			}
		}
	}
	walk(eventID)
	return res, nil
}

func (*remoteFederation) QueryJoinedHostServerNamesInRoom(
	context.Context, *fsAPI.QueryJoinedHostServerNamesInRoomRequest, *fsAPI.QueryJoinedHostServerNamesInRoomResponse,
) error {
	return nil
}

func (*remoteFederation) IsServerBackingOff(spec.ServerName) bool { return false }

func (f *remoteFederation) LookupMissingEvents(
	context.Context, spec.ServerName, spec.ServerName, string, fclient.MissingEvents, gomatrixserverlib.RoomVersion,
) (fclient.RespMissingEvents, error) {
	var res fclient.RespMissingEvents
	if f.failing.Load() {
		return res, errors.New("remote.test: 500 internal server error")
	}
	for _, ev := range f.missing {
		res.Events = append(res.Events, ev.JSON())
	}
	return res, nil
}

// serverKeys serves the servers' public keys, so their events pass signature
// checks.
type serverKeys map[spec.ServerName]ed25519.PrivateKey

func (k serverKeys) FetchKeys(
	_ context.Context, requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	res := map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult{}
	for req := range requests {
		private, ok := k[req.ServerName]
		if !ok {
			continue
		}
		public, ok := private.Public().(ed25519.PublicKey)
		if !ok {
			continue
		}
		res[req] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(public)},
			ExpiredTS:    gomatrixserverlib.PublicKeyNotExpired,
			ValidUntilTS: spec.AsTimestamp(time.Now().Add(time.Hour)),
		}
	}
	return res, nil
}

func (serverKeys) FetcherName() string { return "serverKeys" }

func (serverKeys) StoreKeys(context.Context, map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult) error {
	return nil
}

// After a genuine rejection the room goes on the missing-state cooldown (#59),
// which skips the expensive missing-state work for the room's following
// events. It must not store them as rejected without looking at them. A
// membership stored that way is left out of the auth events of everything its
// sender sends later, so those events are rejected too, for good, even after
// the cooldown. Deferred instead, the membership is fetched with the first of
// them after the cooldown, and they are accepted.
func TestMissingStateCooldownDefersEvents(t *testing.T) {
	// Events from this server are fetched back as auth events too, so its
	// key must be known.
	alice := test.NewUser(t, test.WithSigningServer("test", "ed25519:test", test.PrivateKeyB))
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		known := len(room.Events())
		// A message this server never received, one that refers to it and is
		// rejected because the remote cannot provide it, a new display name
		// that arrives during the cooldown, and a message after it, whose auth
		// events include the membership with the new display name.
		lost := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "lost"})
		trigger := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "trigger"})
		rename := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join, "displayname": "Bob"}, test.WithStateKey(bob.ID))
		after := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "after the cooldown"})
		require.Contains(t, after.AuthEventIDs(), rename.EventID())

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: serverKeys{"test": test.PrivateKeyB, remoteServer: test.PrivateKeyA}}
		federation := &remoteFederation{room: room, keyRing: keyRing, missing: []*types.HeaderedEvent{lost, trigger, rename}}
		rsAPI.SetFederationAPI(federation, keyRing)
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
		internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
		require.True(t, ok)
		send := func(ev *types.HeaderedEvent) error {
			return api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{ev}, "test", remoteServer, api.DoNotSendToOtherServers, nil, false)
		}
		storedRejected := func(ev *types.HeaderedEvent) bool {
			roomInfo, err := internalAPI.DB.RoomInfo(ctx, room.ID)
			require.NoError(t, err)
			rejected, err := internalAPI.DB.IsEventRejected(ctx, roomInfo.RoomNID, ev.EventID())
			if errors.Is(err, sql.ErrNoRows) {
				return false
			}
			require.NoError(t, err)
			return rejected
		}

		// The remote cannot provide the missing event: a genuine rejection,
		// which starts the cooldown.
		federation.failing.Store(true)
		err := send(trigger)
		require.True(t, api.IsInputRejected(err), "the trigger was not rejected: %v", err)

		// The remote could now provide what is missing, but the cooldown skips
		// the lookup.
		federation.failing.Store(false)
		err = send(rename)
		require.Error(t, err, "the membership was processed during the cooldown")
		require.False(t, api.IsInputRejected(err), "the membership was rejected without being looked at: %v", err)
		require.False(t, storedRejected(rename), "the membership was stored as rejected without being looked at")

		// After the cooldown, the next message fetches the membership.
		internalAPI.ExpireMissingStateCooldowns()
		require.NoError(t, send(after), "a message whose auth events include the membership")
		require.False(t, storedRejected(rename), "the deferred membership was rejected when it was fetched")
		var res api.QueryLatestEventsAndStateResponse
		require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &res))
		require.True(t, slices.Contains(res.LatestEvents, after.EventID()), "latest events %v", res.LatestEvents)
	})
}
