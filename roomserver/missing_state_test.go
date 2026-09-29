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
