// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package federationapi_test

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi"
	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// knockFedClient plays the remote server hosting a room that allows knocking.
type knockFedClient struct {
	*fedClient
	room *test.Room
	// extraState is appended to the knock_room_state the remote returns.
	extraState []gomatrixserverlib.InviteStrippedState
	knockVers  []gomatrixserverlib.RoomVersion
	knocked    gomatrixserverlib.PDU
}

func (f *knockFedClient) MakeKnock(
	_ context.Context, _, _ spec.ServerName, roomID, userID string, roomVersions []gomatrixserverlib.RoomVersion,
) (res fclient.RespMakeKnock, err error) {
	f.knockVers = roomVersions
	if roomID != f.room.ID {
		return res, fmt.Errorf("unknown room %s", roomID)
	}
	res.RoomVersion = f.room.Version
	res.KnockEvent = gomatrixserverlib.ProtoEvent{
		SenderID:   userID,
		RoomID:     roomID,
		Type:       spec.MRoomMember,
		StateKey:   &userID,
		Content:    spec.RawJSON(`{"membership":"knock"}`),
		PrevEvents: f.room.ForwardExtremities(),
	}
	needed, err := gomatrixserverlib.StateNeededForProtoEvent(&res.KnockEvent)
	if err != nil {
		return res, err
	}
	res.KnockEvent.AuthEvents = f.room.MustGetAuthEventRefsForEvent(f.t, needed)
	return res, nil
}

func (f *knockFedClient) SendKnock(_ context.Context, _, _ spec.ServerName, event gomatrixserverlib.PDU) (res fclient.RespSendKnock, err error) {
	f.knocked = event
	for _, ev := range f.room.CurrentState() {
		switch ev.Type() {
		case spec.MRoomCreate, spec.MRoomJoinRules, spec.MRoomName:
			res.KnockRoomState = append(res.KnockRoomState, gomatrixserverlib.NewInviteStrippedState(ev.PDU))
		}
	}
	res.KnockRoomState = append(res.KnockRoomState, f.extraState...)
	return res, nil
}

func TestFederationAPIPerformKnock(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		cfg.FederationAPI.PreferDirectFetch = true
		cfg.FederationAPI.KeyPerspectives = nil
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

		serverA := spec.ServerName("server.a")
		serverAKeyID := gomatrixserverlib.KeyID("ed25519:servera")
		creator := test.NewUser(t, test.WithSigningServer(serverA, serverAKeyID, test.PrivateKeyA))
		knocker := test.NewUser(t, test.WithSigningServer(cfg.Global.ServerName, cfg.Global.KeyID, cfg.Global.PrivateKey))

		room := test.NewRoom(t, creator, test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))
		room.CreateAndInsert(t, creator, spec.MRoomJoinRules, map[string]any{"join_rule": spec.Knock}, test.WithStateKey(""))
		room.CreateAndInsert(t, creator, spec.MRoomName, map[string]any{"name": "Kaer Morhen"}, test.WithStateKey(""))
		// An otherwise valid entry from another room, which MSC4311 says to drop.
		otherCreator := test.NewUser(t, test.WithSigningServer(serverA, serverAKeyID, test.PrivateKeyA))
		otherRoom := test.NewRoom(t, otherCreator, test.RoomVersion(gomatrixserverlib.RoomVersionV12))
		foreign := otherRoom.CreateAndInsert(t, otherCreator, spec.MRoomTopic, map[string]any{"topic": "spoofed"}, test.WithStateKey(""))

		fc := &knockFedClient{
			fedClient: &fedClient{
				t: t,
				keys: map[spec.ServerName]struct {
					key   ed25519.PrivateKey
					keyID gomatrixserverlib.KeyID
				}{
					serverA: {key: test.PrivateKeyA, keyID: serverAKeyID},
				},
			},
			room:       room,
			extraState: []gomatrixserverlib.InviteStrippedState{gomatrixserverlib.NewInviteStrippedState(foreign.PDU)},
		}
		fsapi := federationapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, fc, &fedRoomserverAPI{}, caches, nil, false)

		var res api.PerformKnockResponse
		err := fsapi.PerformKnock(context.Background(), &api.PerformKnockRequest{
			RoomID:      room.ID,
			UserID:      knocker.ID,
			ServerNames: []spec.ServerName{serverA},
			Content:     map[string]any{"reason": "let me in"},
		}, &res)
		require.NoError(t, err)
		require.Nil(t, res.LastError)
		assert.Equal(t, serverA, res.KnockedVia)
		assert.Contains(t, fc.knockVers, gomatrixserverlib.RoomVersionV12)
		assert.NotContains(t, fc.knockVers, gomatrixserverlib.RoomVersionV6, "v6 does not support knocking")

		// The knock we sent is ours, for our user, with the requested content.
		require.NotNil(t, fc.knocked)
		assert.Equal(t, res.Event.EventID(), fc.knocked.EventID())
		membership, err := res.Event.Membership()
		require.NoError(t, err)
		assert.Equal(t, spec.Knock, membership)
		assert.Equal(t, knocker.ID, string(res.Event.SenderID()))
		assert.JSONEq(t, `{"membership":"knock","reason":"let me in"}`, string(res.Event.Content()))
		assert.Contains(t, string(res.Event.JSON()), string(cfg.Global.KeyID), "knock must be signed by our server")

		// Only entries which pass MSC4311 validation are kept.
		var kept []string
		for _, entry := range res.KnockRoomState {
			kept = append(kept, entry.Type())
		}
		assert.ElementsMatch(t, []string{spec.MRoomCreate, spec.MRoomJoinRules, spec.MRoomName}, kept)
	})
}

func TestFederationAPIPerformKnockReportsRemoteRefusal(t *testing.T) {
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	creator := test.NewUser(t)
	room := test.NewRoom(t, creator, test.RoomVersion(gomatrixserverlib.RoomVersionV10))
	// The fake client only knows this room, so knocking elsewhere fails.
	fc := &knockFedClient{fedClient: &fedClient{t: t}, room: room}
	fsapi := federationapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, fc, &fedRoomserverAPI{}, caches, nil, false)

	var res api.PerformKnockResponse
	err := fsapi.PerformKnock(context.Background(), &api.PerformKnockRequest{
		RoomID:      "!unknown:server.a",
		UserID:      "@bob:" + string(cfg.Global.ServerName),
		ServerNames: []spec.ServerName{"server.a"},
	}, &res)
	require.NoError(t, err)
	require.NotNil(t, res.LastError)
	assert.Nil(t, res.Event)
}
