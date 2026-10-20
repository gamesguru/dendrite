// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package roomserver_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// peekFederationAPI records outbound peeks and fails them like an unreachable
// remote server would.
type peekFederationAPI struct {
	fsAPI.RoomserverFederationAPI
	peeks []fsAPI.PerformOutboundPeekRequest
}

func (f *peekFederationAPI) PerformOutboundPeek(_ context.Context, req *fsAPI.PerformOutboundPeekRequest, res *fsAPI.PerformOutboundPeekResponse) error {
	f.peeks = append(f.peeks, *req)
	res.LastError = &gomatrix.HTTPError{Code: http.StatusNotFound, Message: `{"errcode":"M_NOT_FOUND","error":"Room not found"}`}
	return res.LastError
}

func TestPerformPeek(t *testing.T) {
	alice := test.NewUser(t)
	remote := spec.ServerName("remote.test")
	worldReadable := func(version gomatrixserverlib.RoomVersion) *test.Room {
		return test.NewRoom(t, alice, test.RoomVersion(version), test.RoomPreset(test.PresetPublicChat),
			test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityWorldReadable))
	}
	localV11 := worldReadable(gomatrixserverlib.RoomVersionV11)
	localV12 := worldReadable(gomatrixserverlib.RoomVersionV12)
	// Another creator, as identical create events would name the same room.
	sharedV12 := test.NewRoom(t, test.NewUser(t), test.RoomVersion(gomatrixserverlib.RoomVersionV12), test.RoomPreset(test.PresetPublicChat))
	// A version 12 room this server has never seen.
	unknownV12 := "!" + strings.Repeat("A", 43)
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		fedAPI := &peekFederationAPI{}
		rsAPI.SetFederationAPI(fedAPI, nil)
		for _, room := range []*test.Room{localV11, localV12, sharedV12} {
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
		}
		peek := func(roomID string, servers ...spec.ServerName) (string, error) {
			fedAPI.peeks = nil
			return rsAPI.PerformPeek(ctx, &api.PerformPeekRequest{
				RoomIDOrAlias: roomID, UserID: alice.ID, DeviceID: "PEEKER", ServerNames: servers,
			})
		}

		t.Run("local rooms are peeked without federation", func(t *testing.T) {
			for _, room := range []*test.Room{localV11, localV12} {
				roomID, err := peek(room.ID, remote)
				require.NoError(t, err, "room version %s", room.Version)
				assert.Equal(t, room.ID, roomID)
				assert.Empty(t, fedAPI.peeks, "room version %s", room.Version)
			}
		})

		t.Run("a room that is not world-readable is refused", func(t *testing.T) {
			_, err := peek(sharedV12.ID)
			assert.ErrorAs(t, err, &api.ErrNotAllowed{})
		})

		t.Run("an unknown version 12 room needs server names", func(t *testing.T) {
			_, err := peek(unknownV12, "test")
			assert.ErrorAs(t, err, &api.ErrInvalidID{})
			assert.Empty(t, fedAPI.peeks, "must not peek into a room via this server")
		})

		t.Run("an unknown version 12 room is peeked via the given servers", func(t *testing.T) {
			_, err := peek(unknownV12, "test", remote)
			var httpErr *gomatrix.HTTPError
			require.ErrorAs(t, err, &httpErr)
			assert.Equal(t, http.StatusNotFound, httpErr.Code)
			require.Len(t, fedAPI.peeks, 1)
			assert.Equal(t, unknownV12, fedAPI.peeks[0].RoomID)
			assert.Equal(t, []spec.ServerName{remote}, []spec.ServerName(fedAPI.peeks[0].ServerNames))
		})

		t.Run("a remote room with a server name is peeked via that server", func(t *testing.T) {
			remoteRoom := "!room:" + string(remote)
			_, err := peek(remoteRoom, "other.test")
			require.Error(t, err)
			require.Len(t, fedAPI.peeks, 1)
			assert.Equal(t, []spec.ServerName{"other.test", remote}, []spec.ServerName(fedAPI.peeks[0].ServerNames))
		})

		t.Run("a remote server may only peek into a world-readable room", func(t *testing.T) {
			inbound := func(roomID string) (api.PerformInboundPeekResponse, error) {
				var res api.PerformInboundPeekResponse
				err := rsAPI.PerformInboundPeek(ctx, &api.PerformInboundPeekRequest{
					RoomID: roomID, PeekID: roomID, ServerName: remote, RenewalInterval: time.Hour.Milliseconds(),
				}, &res)
				return res, err
			}
			res, err := inbound(localV12.ID)
			require.NoError(t, err)
			assert.True(t, res.RoomExists)
			assert.Equal(t, gomatrixserverlib.RoomVersionV12, res.RoomVersion)
			require.NotNil(t, res.LatestEvent)
			assert.NotEmpty(t, res.StateEvents)

			_, err = inbound(sharedV12.ID)
			assert.ErrorAs(t, err, &api.ErrNotAllowed{})

			res, err = inbound(unknownV12)
			require.NoError(t, err)
			assert.False(t, res.RoomExists)
		})

		t.Run("an invalid room ID is refused", func(t *testing.T) {
			for _, roomID := range []string{"!", "!short", "room:test"} {
				_, err := peek(roomID)
				assert.ErrorAs(t, err, &api.ErrInvalidID{}, roomID)
			}
			assert.Empty(t, fedAPI.peeks)
		})
	})
}
