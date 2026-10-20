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
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// TestErasedUserFederation checks MSC4025 over federation: a server that
// joined after an erased user sent an event gets a redacted copy from /event
// and /get_missing_events, while events sent after it joined stay intact.
// Redacting must not change the cached event, and reactivation lifts it.
func TestErasedUserFederation(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	beforeJoin := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "before"})
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	afterJoin := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "after"})
	latest := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "latest"})

	aliceID, err := spec.NewUserID(alice.ID, true)
	require.NoError(t, err)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
		require.True(t, ok)
		rsAPI.SetFederationAPI(missingEventsFederationAPI{}, &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}})
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

		visibility := func(event gomatrixserverlib.PDU) (allowed, redact bool) {
			t.Helper()
			allowed, redact, err := rsAPI.QueryServerEventVisibility(ctx, remoteServer, event)
			require.NoError(t, err)
			return allowed, redact
		}
		missingEvents := func() map[string]gomatrixserverlib.PDU {
			t.Helper()
			var res api.QueryMissingEventsResponse
			require.NoError(t, rsAPI.QueryMissingEvents(ctx, &api.QueryMissingEventsRequest{
				LatestEvents: []string{latest.EventID()},
				Limit:        20,
				ServerName:   remoteServer,
			}, &res))
			byID := make(map[string]gomatrixserverlib.PDU, len(res.Events))
			for _, ev := range res.Events {
				byID[ev.EventID()] = ev.PDU
			}
			return byID
		}

		allowed, redact := visibility(beforeJoin)
		assert.True(t, allowed)
		assert.False(t, redact, "alice is not erased yet")

		require.NoError(t, rsAPI.PerformUserErasure(ctx, *aliceID, true))

		allowed, redact = visibility(beforeJoin)
		assert.True(t, allowed)
		assert.True(t, redact, "the remote server had no member when the event was sent")
		allowed, redact = visibility(afterJoin)
		assert.True(t, allowed)
		assert.False(t, redact, "the remote server was in the room when the event was sent")

		events := missingEvents()
		require.Contains(t, events, beforeJoin.EventID())
		require.Contains(t, events, afterJoin.EventID())
		assert.True(t, events[beforeJoin.EventID()].Redacted())
		assert.Equal(t, beforeJoin.EventID(), events[beforeJoin.EventID()].EventID())
		assert.False(t, events[afterJoin.EventID()].Redacted())

		// The event cache must still hold the unredacted event.
		var byID api.QueryEventsByIDResponse
		require.NoError(t, rsAPI.QueryEventsByID(ctx, &api.QueryEventsByIDRequest{RoomID: room.ID, EventIDs: []string{beforeJoin.EventID()}}, &byID))
		require.Len(t, byID.Events, 1)
		assert.False(t, byID.Events[0].Redacted())
		assert.JSONEq(t, `{"body":"before"}`, string(byID.Events[0].Content()))

		erased, err := rsAPI.QueryErasedUsers(ctx, []string{alice.ID, bob.ID})
		require.NoError(t, err)
		assert.Equal(t, map[string]struct{}{alice.ID: {}}, erased)

		require.NoError(t, rsAPI.PerformUserErasure(ctx, *aliceID, false))
		_, redact = visibility(beforeJoin)
		assert.False(t, redact, "reactivation lifts the erasure")
		assert.False(t, missingEvents()[beforeJoin.EventID()].Redacted())

		// Remote users cannot be erased.
		bobID, err := spec.NewUserID(bob.ID, true)
		require.NoError(t, err)
		assert.Error(t, rsAPI.PerformUserErasure(ctx, *bobID, true))
	})
}
