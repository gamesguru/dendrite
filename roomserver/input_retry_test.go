// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
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

// refusingJetStream refuses publications while refusing is set, the way the
// HA publication gate does while ownership cannot be reconfirmed during a
// PostgreSQL failover.
type refusingJetStream struct {
	nats.JetStreamContext
	refusing  atomic.Bool
	refused   atomic.Int32
	published atomic.Int32 // new room events published
}

func (r *refusingJetStream) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if r.refusing.Load() {
		r.refused.Add(1)
		return nil, fmt.Errorf("%w: HA ownership has not been confirmed within the freshness window", jetstream.ErrPublicationRefused)
	}
	ack, err := r.JetStreamContext.PublishMsg(msg, opts...)
	if err == nil && msg.Header.Get(jetstream.RoomEventType) == string(api.OutputTypeNewRoomEvent) {
		r.published.Add(1)
	}
	return ack, err
}

// retryTestRoom sets up a roomserver with a room the remote server has
// joined, and returns it with the next message from the remote server and the
// JetStream the roomserver publishes its output through.
func retryTestRoom(t *testing.T, dbType test.DBType) (api.RoomserverInternalAPI, *test.Room, *types.HeaderedEvent, *refusingJetStream) {
	t.Helper()
	alice := test.NewUser(t)
	bob := test.NewUser(t, test.WithSigningServer(remoteServer, "ed25519:remote", test.PrivateKeyA))
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	known := len(room.Events())
	next := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "next"})

	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	t.Cleanup(closeDB)
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
	require.True(t, ok)
	keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remoteKeys{public: public}}
	rsAPI.SetFederationAPI(missingEventsFederationAPI{}, keyRing)

	internalAPI, ok := rsAPI.(*rsinternal.RoomserverInternalAPI)
	require.True(t, ok)
	js := &refusingJetStream{JetStreamContext: internalAPI.OutputProducer.JetStream}
	internalAPI.OutputProducer.JetStream = js
	require.NoError(t, api.SendEvents(context.Background(), rsAPI, api.KindNew, room.Events()[:known], "test", "test", "test", nil, false))
	return rsAPI, room, next, js
}

func isLatestEvent(t *testing.T, rsAPI api.RoomserverInternalAPI, roomID, eventID string) bool {
	t.Helper()
	var res api.QueryLatestEventsAndStateResponse
	require.NoError(t, rsAPI.QueryLatestEventsAndState(context.Background(), &api.QueryLatestEventsAndStateRequest{RoomID: roomID}, &res))
	return slices.Contains(res.LatestEvents, eventID)
}

// An inbound federation event that fails on this server's own dependencies,
// here the HA publication gate during a PostgreSQL failover, must be retried
// once they recover. The remote's /send transaction has already succeeded, so
// it never resends the event: acknowledging the failed input loses it.
func TestAsyncInputRetriedWhenOwnInfrastructureFails(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		rsAPI, room, next, js := retryTestRoom(t, dbType)

		js.refusing.Store(true)
		require.NoError(t, api.SendEvents(context.Background(), rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, api.DoNotSendToOtherServers, nil, true))
		require.Eventually(t, func() bool { return js.refused.Load() > 0 }, 10*time.Second, 10*time.Millisecond,
			"the event's output was never published")
		published := js.published.Load()
		js.refusing.Store(false)

		// Its output must reach the rest of the server, not only the room's
		// latest events: on SQLite those are updated even though the
		// publication failed.
		require.Eventually(t, func() bool { return js.published.Load() > published }, 10*time.Second, 50*time.Millisecond,
			"the event was dropped instead of being retried once publication works again")
		// The output is published before the latest events are committed,
		// so the publication can be seen before the commit.
		require.Eventually(t, func() bool { return isLatestEvent(t, rsAPI, room.ID, next.EventID()) },
			10*time.Second, 50*time.Millisecond, "the retried event never became a latest event")
	})
}

// A synchronous caller, such as a client sending a message, is waiting for
// the reply. It is answered with the failure straight away, as before, and
// the input is not retried behind its back.
func TestSyncInputFailsWithoutRetryWhenOwnInfrastructureFails(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		rsAPI, _, next, js := retryTestRoom(t, dbType)

		js.refusing.Store(true)
		started := time.Now()
		err := api.SendEvents(context.Background(), rsAPI, api.KindNew, []*types.HeaderedEvent{next}, "test", remoteServer, api.DoNotSendToOtherServers, nil, false)
		require.Error(t, err, "publication was refused")
		require.False(t, api.IsInputRejected(err), "a refused publication rejected the event: %v", err)
		require.Less(t, time.Since(started), 5*time.Second, "the synchronous caller waited for retries")
		time.Sleep(time.Second)
		require.EqualValues(t, 1, js.refused.Load(), "a synchronous input was retried after its caller was answered")
	})
}
