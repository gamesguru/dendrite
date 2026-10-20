// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// knockFederationAPI stands in for the federation API when knocking on a
// room hosted by another server.
type knockFederationAPI struct {
	fsAPI.RoomserverFederationAPI
	knock     gomatrixserverlib.PDU
	state     []gomatrixserverlib.InviteStrippedState
	knockReqs []fsAPI.PerformKnockRequest
	leaveReqs []fsAPI.PerformLeaveRequest
}

func (f *knockFederationAPI) PerformKnock(_ context.Context, req *fsAPI.PerformKnockRequest, res *fsAPI.PerformKnockResponse) error {
	f.knockReqs = append(f.knockReqs, *req)
	res.Event = f.knock
	res.KnockRoomState = f.state
	res.KnockedVia = req.ServerNames[0]
	return nil
}

func (f *knockFederationAPI) PerformLeave(_ context.Context, req *fsAPI.PerformLeaveRequest, _ *fsAPI.PerformLeaveResponse) error {
	f.leaveReqs = append(f.leaveReqs, *req)
	return nil
}

func TestPerformFederatedKnock(t *testing.T) {
	remote := spec.ServerName("remote.test")
	creator := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyB))
	bob := test.NewUser(t)
	room := test.NewRoom(t, creator, test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))
	room.CreateAndInsert(t, creator, spec.MRoomJoinRules, map[string]any{"join_rule": spec.Knock}, test.WithStateKey(""))
	knock := room.CreateEvent(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Knock}, test.WithStateKey(bob.ID))
	var state []gomatrixserverlib.InviteStrippedState
	for _, ev := range room.CurrentState() {
		state = append(state, gomatrixserverlib.NewInviteStrippedState(ev.PDU))
	}
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, nc := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
		outputs, err := nc.SubscribeSync(cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent))
		require.NoError(t, err)
		db, err := storage.Open(processCtx.Context(), cm, &cfg.RoomServer.Database, caches) //nolint:contextcheck
		require.NoError(t, err)

		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		// The knock flow sets unsigned fields on the event federation
		// returns, so each parallel subtest needs its own copy, as a real
		// federation response would be.
		ownKnock, err := gomatrixserverlib.MustGetRoomVersion(knock.Version()).NewEventFromTrustedJSON(knock.JSON(), false)
		require.NoError(t, err)
		fedAPI := &knockFederationAPI{knock: ownKnock, state: state}
		rsAPI.SetFederationAPI(fedAPI, nil)
		bobID, err := spec.NewUserID(bob.ID, true)
		require.NoError(t, err)

		roomID, err := rsAPI.PerformKnock(ctx, &api.PerformKnockRequest{
			RoomIDOrAlias: room.ID,
			UserID:        *bobID,
			Content:       map[string]any{"reason": "let me in"},
			ServerNames:   []spec.ServerName{remote},
		})
		require.NoError(t, err)
		assert.Equal(t, room.ID, roomID)
		require.Len(t, fedAPI.knockReqs, 1)
		assert.Equal(t, "let me in", fedAPI.knockReqs[0].Content["reason"])
		assert.Equal(t, spec.Knock, fedAPI.knockReqs[0].Content["membership"])

		// The knock is announced to the sync API with stripped room state.
		var announced api.OutputEvent
		for announced.Type != api.OutputTypeNewKnockEvent {
			msg, nextErr := outputs.NextMsg(5 * time.Second)
			require.NoError(t, nextErr)
			require.NoError(t, json.Unmarshal(msg.Data, &announced))
		}
		knockState := gjson.GetBytes(announced.NewKnockEvent.Event.Unsigned(), "knock_room_state").Array()
		require.Len(t, knockState, len(state))
		for _, entry := range knockState {
			assert.False(t, entry.Get("signatures").Exists(), "knock_room_state must be stripped: %s", entry.Raw)
		}

		// The pending knock is recorded even though this server is not in the room.
		pending, eventID, _, err := helpers.IsKnockPending(ctx, db, room.ID, spec.SenderID(bob.ID))
		require.NoError(t, err)
		assert.True(t, pending)
		assert.Equal(t, knock.EventID(), eventID)
		invited, _, _, _, err := helpers.IsInvitePending(ctx, db, room.ID, spec.SenderID(bob.ID))
		require.NoError(t, err)
		assert.False(t, invited, "a knock is not an invite")

		// Leaving rescinds the knock through a server in the room, taken from
		// the knock_room_state as v12 room IDs name no server.
		require.NoError(t, rsAPI.PerformLeave(ctx, &api.PerformLeaveRequest{RoomID: room.ID, Leaver: *bobID}, &api.PerformLeaveResponse{}))
		require.Len(t, fedAPI.leaveReqs, 1)
		assert.Equal(t, []spec.ServerName{remote}, []spec.ServerName(fedAPI.leaveReqs[0].ServerNames))
		pending, _, _, err = helpers.IsKnockPending(ctx, db, room.ID, spec.SenderID(bob.ID))
		require.NoError(t, err)
		assert.False(t, pending)

		var retired api.OutputEvent
		for retired.Type != api.OutputTypeRetireInviteEvent {
			msg, nextErr := outputs.NextMsg(5 * time.Second)
			require.NoError(t, nextErr)
			require.NoError(t, json.Unmarshal(msg.Data, &retired))
		}
		assert.Equal(t, knock.EventID(), retired.RetireInviteEvent.EventID)
		assert.Equal(t, spec.Leave, retired.RetireInviteEvent.Membership)
	})
}

func TestRetractPendingKnock(t *testing.T) {
	remote := spec.ServerName("remote.test")
	creator := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyB))
	stranger := test.NewUser(t, test.WithSigningServer("stranger.test", "ed25519:stranger", test.PrivateKeyA))
	bob := test.NewUser(t)
	room := test.NewRoom(t, creator, test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))
	room.CreateAndInsert(t, creator, spec.MRoomJoinRules, map[string]any{"join_rule": spec.Knock}, test.WithStateKey(""))
	knock := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Knock}, test.WithStateKey(bob.ID))
	var state []gomatrixserverlib.InviteStrippedState
	for _, ev := range room.CurrentState() {
		if ev.Type() != spec.MRoomMember {
			state = append(state, gomatrixserverlib.NewInviteStrippedState(ev.PDU))
		}
	}
	kick := room.CreateEvent(t, creator, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))
	// A server with no part in the room must not be able to clear the knock.
	bobStateKey := bob.ID
	strangerKickPDU, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV12).NewEventBuilderFromProtoEvent(&gomatrixserverlib.ProtoEvent{
		SenderID:   stranger.ID,
		RoomID:     room.ID,
		Type:       spec.MRoomMember,
		StateKey:   &bobStateKey,
		Content:    spec.RawJSON(`{"membership":"leave"}`),
		PrevEvents: room.ForwardExtremities(),
		AuthEvents: []string{},
	}).Build(time.Now(), "stranger.test", "ed25519:stranger", test.PrivateKeyA)
	require.NoError(t, err)
	strangerKick := &types.HeaderedEvent{PDU: strangerKickPDU}
	ctx := context.Background()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, nc := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	outputs, err := nc.SubscribeSync(cfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent))
	require.NoError(t, err)
	db, err := storage.Open(processCtx.Context(), cm, &cfg.RoomServer.Database, caches)
	require.NoError(t, err)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(&knockFederationAPI{knock: knock.PDU, state: state}, nil)
	bobID, err := spec.NewUserID(bob.ID, true)
	require.NoError(t, err)
	_, err = rsAPI.PerformKnock(ctx, &api.PerformKnockRequest{RoomIDOrAlias: room.ID, UserID: *bobID, ServerNames: []spec.ServerName{remote}})
	require.NoError(t, err)

	// A kick sent by a server we have no reason to trust is left alone.
	applied, err := rsAPI.RetractPendingMembership(ctx, strangerKick)
	require.NoError(t, err)
	assert.False(t, applied)
	pending, _, _, err := helpers.IsKnockPending(ctx, db, room.ID, spec.SenderID(bob.ID))
	require.NoError(t, err)
	assert.True(t, pending)

	// The room creator's server, named by the knock state, rejects the knock.
	applied, err = rsAPI.RetractPendingMembership(ctx, kick)
	require.NoError(t, err)
	assert.True(t, applied)
	pending, _, _, err = helpers.IsKnockPending(ctx, db, room.ID, spec.SenderID(bob.ID))
	require.NoError(t, err)
	assert.False(t, pending)

	var retired api.OutputEvent
	for retired.Type != api.OutputTypeRetireInviteEvent {
		msg, nextErr := outputs.NextMsg(5 * time.Second)
		require.NoError(t, nextErr)
		require.NoError(t, json.Unmarshal(msg.Data, &retired))
	}
	assert.Equal(t, knock.EventID(), retired.RetireInviteEvent.EventID)
	assert.Equal(t, kick.EventID(), retired.RetireInviteEvent.RetiredByEventID)
	assert.Equal(t, spec.Leave, retired.RetireInviteEvent.Membership)

	// Events about other memberships, or for rooms we are in, go the usual way.
	applied, err = rsAPI.RetractPendingMembership(ctx, knock)
	require.NoError(t, err)
	assert.False(t, applied)
}

func TestRetractPendingInvite(t *testing.T) {
	remote := spec.ServerName("remote.test")
	inviter := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyB))
	moderator := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyB))
	bob := test.NewUser(t)
	room := test.NewRoom(t, inviter, test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))
	room.CreateAndInsert(t, inviter, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(moderator.ID))
	room.CreateAndInsert(t, moderator, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(moderator.ID))
	invite := room.CreateAndInsert(t, inviter, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(bob.ID))
	inviterKick := room.CreateEvent(t, inviter, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))
	bobStateKey := bob.ID
	moderatorKickPDU, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV12).NewEventBuilderFromProtoEvent(&gomatrixserverlib.ProtoEvent{
		SenderID:   moderator.ID,
		RoomID:     room.ID,
		Type:       spec.MRoomMember,
		StateKey:   &bobStateKey,
		Content:    spec.RawJSON(`{"membership":"leave"}`),
		PrevEvents: room.ForwardExtremities(),
		AuthEvents: []string{},
	}).Build(time.Now(), remote, "ed25519:remote", test.PrivateKeyB)
	require.NoError(t, err)
	moderatorKick := &types.HeaderedEvent{PDU: moderatorKickPDU}
	ctx := context.Background()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	db, err := storage.Open(processCtx.Context(), cm, &cfg.RoomServer.Database, caches)
	require.NoError(t, err)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	require.NoError(t, rsAPI.HandleInvite(ctx, invite))

	isInvited := func() bool {
		t.Helper()
		invited, _, eventID, _, pendingErr := helpers.IsInvitePending(ctx, db, room.ID, spec.SenderID(bob.ID))
		require.NoError(t, pendingErr)
		return invited && eventID == invite.EventID()
	}

	// A moderator's kick is not a rescission, as in Synapse.
	applied, err := rsAPI.RetractPendingMembership(ctx, moderatorKick)
	require.NoError(t, err)
	assert.False(t, applied)
	assert.True(t, isInvited())

	// The inviter rescinds it.
	applied, err = rsAPI.RetractPendingMembership(ctx, inviterKick)
	require.NoError(t, err)
	assert.True(t, applied)
	assert.False(t, isInvited())
}
