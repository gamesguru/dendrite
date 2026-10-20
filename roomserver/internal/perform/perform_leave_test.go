// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/internal/perform"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// leaveFederationAPI records the federated leaves asked of the federation API.
type leaveFederationAPI struct {
	fsAPI.RoomserverFederationAPI
	leaveReqs []fsAPI.PerformLeaveRequest
}

func (f *leaveFederationAPI) PerformLeave(_ context.Context, req *fsAPI.PerformLeaveRequest, _ *fsAPI.PerformLeaveResponse) error {
	f.leaveReqs = append(f.leaveReqs, *req)
	return nil
}

// unknownSenderAPI resolves no user ID for sender, as for a federated invite
// whose inviter's mxid_mapping this server has never seen.
type unknownSenderAPI struct {
	rsAPI.RoomserverInternalAPI
	sender spec.SenderID
}

func (a unknownSenderAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	if senderID == a.sender {
		return nil, nil
	}
	return a.RoomserverInternalAPI.QueryUserIDForSender(ctx, roomID, senderID)
}

// TestRejectInviteFromUnknownSender rejects an invite from an inviter this
// server cannot resolve to a user ID, which is rejected through the room ID's
// server, if it names one, and the servers of the invite's stripped state.
func TestRejectInviteFromUnknownSender(t *testing.T) {
	remote := spec.ServerName("remote.test")
	for _, version := range []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV11, gomatrixserverlib.RoomVersionV12} {
		t.Run(string(version), func(t *testing.T) {
			creator := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyB))
			bob := test.NewUser(t)
			room := test.NewRoom(t, creator, test.RoomVersion(version), test.RoomPreset(test.PresetPrivateChat))
			var state []gomatrixserverlib.InviteStrippedState
			for _, ev := range room.CurrentState() {
				state = append(state, gomatrixserverlib.NewInviteStrippedState(ev.PDU))
			}
			invite := room.CreateEvent(t, creator, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(bob.ID))
			require.NoError(t, invite.SetUnsignedField("invite_room_state", state))
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
			internalAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
			internalAPI.SetFederationAPI(nil, nil)
			require.NoError(t, internalAPI.HandleInvite(ctx, invite))

			fedAPI := &leaveFederationAPI{}
			leaver := &perform.Leaver{
				Cfg:   &cfg.RoomServer,
				DB:    db,
				FSAPI: fedAPI,
				RSAPI: unknownSenderAPI{RoomserverInternalAPI: internalAPI, sender: invite.SenderID()},
			}
			bobID, err := spec.NewUserID(bob.ID, true)
			require.NoError(t, err)
			var outputs []rsAPI.OutputEvent
			require.NotPanics(t, func() {
				outputs, err = leaver.PerformLeave(ctx, &rsAPI.PerformLeaveRequest{RoomID: room.ID, Leaver: *bobID}, &rsAPI.PerformLeaveResponse{})
			})
			require.NoError(t, err)
			require.Len(t, fedAPI.leaveReqs, 1)
			assert.Equal(t, []spec.ServerName{remote}, []spec.ServerName(fedAPI.leaveReqs[0].ServerNames))

			// The invite is retired.
			require.Len(t, outputs, 1)
			assert.Equal(t, rsAPI.OutputTypeRetireInviteEvent, outputs[0].Type)
			assert.Equal(t, invite.EventID(), outputs[0].RetireInviteEvent.EventID)
			invited, _, _, _, err := helpers.IsInvitePending(ctx, db, room.ID, spec.SenderID(bob.ID))
			require.NoError(t, err)
			assert.False(t, invited)
		})
	}
}
