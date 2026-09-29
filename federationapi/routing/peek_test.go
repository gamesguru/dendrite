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
	"strings"
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

func TestPeek(t *testing.T) {
	remote := spec.ServerName("remote.test")
	alice := test.NewUser(t)
	worldReadable := func(creator *test.User, version gomatrixserverlib.RoomVersion) *test.Room {
		return test.NewRoom(t, creator, test.RoomVersion(version), test.RoomPreset(test.PresetPublicChat),
			test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityWorldReadable))
	}
	peekableV11 := worldReadable(alice, gomatrixserverlib.RoomVersionV11)
	peekableV12 := worldReadable(alice, gomatrixserverlib.RoomVersionV12)
	// A different creator, as identical v12 create events share a room ID.
	sharedV12 := test.NewRoom(t, test.NewUser(t), test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPublicChat))
	bob := test.NewUser(t)
	encrypted := worldReadable(bob, gomatrixserverlib.RoomVersionV12)
	encrypted.CreateAndInsert(t, bob, spec.MRoomEncryption, map[string]any{"algorithm": "m.megolm.v1.aes-sha2"}, test.WithStateKey(""))

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
	for _, room := range []*test.Room{peekableV11, peekableV12, sharedV12, encrypted} {
		require.NoError(t, rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
	}

	peek := func(roomID string, vers ...gomatrixserverlib.RoomVersion) (int, []byte) {
		path := fmt.Sprintf("/_matrix/federation/v1/peek/%s/%s", roomID, roomID)
		req := fclient.NewFederationRequest(http.MethodPut, remote, cfg.Global.ServerName, path)
		httpReq, err := req.HTTPRequest()
		require.NoError(t, err)
		res := routing.Peek(httpReq, &req, &cfg.FederationAPI, rsAPI, roomID, roomID, vers)
		body, err := json.Marshal(res.JSON)
		require.NoError(t, err, "the response body must encode as JSON")
		return res.Code, body
	}
	all := []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV11, gomatrixserverlib.RoomVersionV12}

	// The peeking server must be able to decode the response, whatever the
	// room ID form of the room version.
	for _, room := range []*test.Room{peekableV11, peekableV12} {
		t.Run("peek into room version "+string(room.Version), func(t *testing.T) {
			code, body := peek(room.ID, all...)
			require.Equal(t, http.StatusOK, code, string(body))
			var res fclient.RespPeek
			require.NoError(t, json.Unmarshal(body, &res))
			assert.Equal(t, room.Version, res.RoomVersion)
			assert.Equal(t, time.Hour.Milliseconds(), res.RenewalInterval)
			require.NotNil(t, res.LatestEvent)
			assert.Equal(t, room.Events()[len(room.Events())-1].EventID(), res.LatestEvent.EventID())
			assert.NotEmpty(t, res.StateEvents)
			assert.NotEmpty(t, res.AuthEvents)
		})
	}

	for _, tc := range []struct {
		name     string
		roomID   string
		vers     []gomatrixserverlib.RoomVersion
		wantCode int
		wantErr  spec.MatrixErrorCode
	}{
		{"unsupported room version", peekableV12.ID, []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV11}, http.StatusBadRequest, spec.ErrorIncompatibleRoomVersion},
		{"not world-readable", sharedV12.ID, all, http.StatusForbidden, spec.ErrorForbidden},
		{"encrypted", encrypted.ID, all, http.StatusForbidden, spec.ErrorForbidden},
		{"unknown room", "!" + strings.Repeat("B", 43), all, http.StatusNotFound, spec.ErrorNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := peek(tc.roomID, tc.vers...)
			assert.Equal(t, tc.wantCode, code, string(body))
			assert.Equal(t, string(tc.wantErr), gjson.GetBytes(body, "errcode").Str, string(body))
		})
	}
}
