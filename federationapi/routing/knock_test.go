// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/federationapi/routing"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// TestMakeAndSendKnock drives the make_knock/send_knock handlers against a
// real roomserver hosting the room, as a remote knocking server would.
func TestMakeAndSendKnock(t *testing.T) {
	remote := spec.ServerName("remote.test")
	remoteKeyID := gomatrixserverlib.KeyID("ed25519:remote")
	knocker := test.NewUser(t, test.WithSigningServer(remote, remoteKeyID, test.PrivateKeyB))
	alice := test.NewUser(t)

	knockRoom := test.NewRoom(t, alice, test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))
	knockRoom.CreateAndInsert(t, alice, spec.MRoomJoinRules, map[string]any{"join_rule": spec.Knock}, test.WithStateKey(""))
	// A different creator, as identical v12 create events share a room ID.
	inviteOnlyRoom := test.NewRoom(t, test.NewUser(t), test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPrivateChat))

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	ctx := context.Background()
	for _, room := range []*test.Room{knockRoom, inviteOnlyRoom} {
		require.NoError(t, rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
	}

	makeKnock := func(room *test.Room, user *test.User, vers ...gomatrixserverlib.RoomVersion) (int, gjson.Result) {
		path := fmt.Sprintf("/_matrix/federation/v1/make_knock/%s/%s", room.ID, user.ID)
		req := fclient.NewFederationRequest(http.MethodGet, remote, cfg.Global.ServerName, path)
		httpReq, err := req.HTTPRequest()
		require.NoError(t, err)
		roomID, err := spec.NewRoomID(room.ID)
		require.NoError(t, err)
		userID, err := spec.NewUserID(user.ID, true)
		require.NoError(t, err)
		res := routing.MakeKnock(httpReq, &req, &cfg.FederationAPI, rsAPI, *roomID, *userID, vers)
		body, err := json.Marshal(res.JSON)
		require.NoError(t, err)
		return res.Code, gjson.ParseBytes(body)
	}

	t.Run("unsupported ver", func(t *testing.T) {
		code, body := makeKnock(knockRoom, knocker, gomatrixserverlib.RoomVersionV10)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, string(spec.ErrorIncompatibleRoomVersion), body.Get("errcode").Str)
	})
	t.Run("user not from the requesting server", func(t *testing.T) {
		code, _ := makeKnock(knockRoom, alice, gomatrixserverlib.RoomVersionV12)
		assert.Equal(t, http.StatusForbidden, code)
	})
	t.Run("room does not allow knocking", func(t *testing.T) {
		code, _ := makeKnock(inviteOnlyRoom, knocker, gomatrixserverlib.RoomVersionV12)
		assert.Equal(t, http.StatusForbidden, code)
	})

	t.Run("knock", func(t *testing.T) {
		code, body := makeKnock(knockRoom, knocker, gomatrixserverlib.RoomVersionV10, gomatrixserverlib.RoomVersionV12)
		require.Equal(t, http.StatusOK, code, body.Raw)
		assert.Equal(t, string(gomatrixserverlib.RoomVersionV12), body.Get("room_version").Str)

		// Sign the template as the remote server would.
		var proto gomatrixserverlib.ProtoEvent
		require.NoError(t, json.Unmarshal([]byte(body.Get("event").Raw), &proto))
		assert.Equal(t, spec.Knock, gjson.GetBytes(proto.Content, "membership").Str)
		knock, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV12).
			NewEventBuilderFromProtoEvent(&proto).Build(time.Now(), remote, remoteKeyID, test.PrivateKeyB)
		require.NoError(t, err)

		req := fclient.NewFederationRequest(http.MethodPut, remote, cfg.Global.ServerName,
			fmt.Sprintf("/_matrix/federation/v1/send_knock/%s/%s", knockRoom.ID, knock.EventID()))
		require.NoError(t, req.SetContent(knock))
		httpReq, err := req.HTTPRequest()
		require.NoError(t, err)
		roomID, err := spec.NewRoomID(knockRoom.ID)
		require.NoError(t, err)
		res := routing.SendKnock(httpReq, &req, &cfg.FederationAPI, rsAPI, &test.NopJSONVerifier{}, *roomID, knock.EventID())
		resBody, err := json.Marshal(res.JSON)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.Code, string(resBody))

		// MSC4311: knock_room_state holds full PDUs, including m.room.create.
		var sawCreate bool
		for _, entry := range gjson.GetBytes(resBody, "knock_room_state").Array() {
			assert.True(t, entry.Get("signatures").Exists(), "entry is not a PDU: %s", entry.Raw)
			if entry.Get("type").Str == spec.MRoomCreate {
				sawCreate = true
			}
		}
		assert.True(t, sawCreate, "knock_room_state lacks m.room.create: %s", resBody)

		// The knock is now part of the room.
		stateRes := rsapi.QueryCurrentStateResponse{}
		require.NoError(t, rsAPI.QueryCurrentState(ctx, &rsapi.QueryCurrentStateRequest{
			RoomID:      knockRoom.ID,
			StateTuples: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomMember, StateKey: knocker.ID}},
		}, &stateRes))
		member := stateRes.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: knocker.ID}]
		require.NotNil(t, member)
		assert.Equal(t, knock.EventID(), member.EventID())
	})
}
