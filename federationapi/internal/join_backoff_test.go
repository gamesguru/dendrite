// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// noInviteRoomserverAPI reports no pending invite for any user.
type noInviteRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
}

func (noInviteRoomserverAPI) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, user spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(user.String())
	return &senderID, nil
}

func (noInviteRoomserverAPI) QueryRoomInfo(context.Context, spec.RoomID) (*types.RoomInfo, error) {
	return nil, nil
}

func (noInviteRoomserverAPI) InvitePending(context.Context, spec.RoomID, spec.SenderID) (bool, error) {
	return false, nil
}

type refusingJoinClient struct {
	fclient.FederationClient
}

func (refusingJoinClient) MakeJoin(context.Context, spec.ServerName, spec.ServerName, string, string) (fclient.RespMakeJoin, error) {
	return fclient.RespMakeJoin{}, spec.HTTPError{
		Code:     http.StatusForbidden,
		Contents: []byte(`{"errcode":"M_FORBIDDEN","error":"You are not joined to any matching rooms."}`),
	}
}

// A server that answers make_join with a refusal is reachable. Backing off
// from it would also delay every event we send it, such as the leave of a
// user who then joins through another server.
func TestRefusedJoinDoesNotBackOff(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cfg := config.FederationAPI{Matrix: &config.Global{
		SigningIdentity: fclient.SigningIdentity{ServerName: "local", KeyID: "ed25519:1", PrivateKey: key},
	}}
	fedClient := refusingJoinClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(testDB, process.NewProcessContext(), false, cfg.Matrix.ServerName, fedClient, &stats, nil)
	fedAPI := NewFederationInternalAPI(testDB, &cfg, noInviteRoomserverAPI{}, fedClient, &stats, nil, queues, nil)

	res := api.PerformJoinResponse{}
	fedAPI.PerformJoin(context.Background(), &api.PerformJoinRequest{
		RoomID:      "!room:remote",
		UserID:      "@alice:local",
		ServerNames: []spec.ServerName{"remote"},
	}, &res)
	require.NotNil(t, res.LastError)
	assert.Nil(t, stats.ForServer("remote").BackoffInfo(), "a refused join must not back off the server")
}

type invitedRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
	senderErr, inviteErr, roomErr error
	pending, partial              bool
	roomInfo                      *types.RoomInfo
}

func (a invitedRoomserverAPI) QueryRoomInfo(context.Context, spec.RoomID) (*types.RoomInfo, error) {
	return a.roomInfo, a.roomErr
}

func (a invitedRoomserverAPI) IsRoomPartialState(context.Context, types.RoomNID) (bool, error) {
	return a.partial, nil
}

func stubRoomInfo() *types.RoomInfo {
	roomInfo := &types.RoomInfo{RoomNID: 1}
	roomInfo.SetIsStub(true)
	return roomInfo
}

func (a invitedRoomserverAPI) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, user spec.UserID) (*spec.SenderID, error) {
	if a.senderErr != nil {
		return nil, a.senderErr
	}
	senderID := spec.SenderID(user.String())
	return &senderID, nil
}

func (a invitedRoomserverAPI) InvitePending(context.Context, spec.RoomID, spec.SenderID) (bool, error) {
	return a.pending, a.inviteErr
}

// An unknown invite state must be reported as an error, so the join asks for
// full state instead of risking a rejected partial-state join and a second one.
func TestInvitePending(t *testing.T) {
	room, err := spec.NewRoomID("!room:remote")
	require.NoError(t, err)
	user, err := spec.NewUserID("@alice:local", true)
	require.NoError(t, err)
	lookupErr := errors.New("db down")

	for name, tc := range map[string]struct {
		rs          invitedRoomserverAPI
		wantPending bool
		wantErr     error
	}{
		"invited":              {rs: invitedRoomserverAPI{pending: true}, wantPending: true},
		"not invited":          {rs: invitedRoomserverAPI{}},
		"sender lookup failed": {rs: invitedRoomserverAPI{senderErr: lookupErr}, wantErr: lookupErr},
		"invite lookup failed": {rs: invitedRoomserverAPI{inviteErr: lookupErr}, wantErr: lookupErr},
	} {
		t.Run(name, func(t *testing.T) {
			r := &FederationInternalAPI{rsAPI: tc.rs}
			pending, err := r.invitePending(context.Background(), room, user)
			assert.Equal(t, tc.wantPending, pending)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// joinRecordingClient answers make_join and records which kind of send_join the
// join asks for, then fails it so that no room is joined.
type joinRecordingClient struct {
	fclient.FederationClient
	mu    sync.Mutex
	sends []string
}

func (c *joinRecordingClient) MakeJoin(_ context.Context, _, _ spec.ServerName, roomID, userID string) (fclient.RespMakeJoin, error) {
	stateKey := userID
	return fclient.RespMakeJoin{
		RoomVersion: gomatrixserverlib.RoomVersionV10,
		JoinEvent: gomatrixserverlib.ProtoEvent{
			RoomID:     roomID,
			SenderID:   userID,
			Type:       spec.MRoomMember,
			StateKey:   &stateKey,
			Content:    []byte(`{"membership":"join"}`),
			PrevEvents: []string{"$prev:remote"},
			Depth:      1,
		},
	}, nil
}

func (c *joinRecordingClient) record(kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends = append(c.sends, kind)
}

func (c *joinRecordingClient) SendJoin(context.Context, spec.ServerName, spec.ServerName, gomatrixserverlib.PDU) (fclient.RespSendJoin, error) {
	c.record("full")
	return fclient.RespSendJoin{}, errors.New("send_join stopped by the test")
}

func (c *joinRecordingClient) SendJoinPartialState(context.Context, spec.ServerName, spec.ServerName, gomatrixserverlib.PDU) (fclient.RespSendJoin, error) {
	c.record("partial")
	return fclient.RespSendJoin{}, errors.New("send_join stopped by the test")
}

// A user with a pending invite must ask for full state at once, since partial
// state omits the invite membership the join needs, and must send exactly one
// join. Retrying after a rejected partial join would send a second one.
func TestJoinSendJoinRequestMatchesInviteState(t *testing.T) {
	lookupErr := errors.New("db down")
	for name, tc := range map[string]struct {
		rs   invitedRoomserverAPI
		want []string
	}{
		"invited asks for full state once":               {rs: invitedRoomserverAPI{pending: true}, want: []string{"full"}},
		"unknown invite state asks for full state once":  {rs: invitedRoomserverAPI{inviteErr: lookupErr}, want: []string{"full"}},
		"unknown room uses partial state":                {rs: invitedRoomserverAPI{}, want: []string{"partial"}},
		"stub room uses partial state":                   {rs: invitedRoomserverAPI{roomInfo: stubRoomInfo()}, want: []string{"partial"}},
		"partial room uses partial state":                {rs: invitedRoomserverAPI{roomInfo: &types.RoomInfo{RoomNID: 1}, partial: true}, want: []string{"partial"}},
		"resynced room asks for full state once":         {rs: invitedRoomserverAPI{roomInfo: &types.RoomInfo{RoomNID: 1}}, want: []string{"full"}},
		"room lookup failure uses partial state":         {rs: invitedRoomserverAPI{roomErr: errors.New("db down")}, want: []string{"partial"}},
		"sender lookup failure asks for full state once": {rs: invitedRoomserverAPI{senderErr: lookupErr}, want: []string{"full"}},
	} {
		t.Run(name, func(t *testing.T) {
			testDB := test.NewInMemoryFederationDatabase()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			cfg := config.FederationAPI{Matrix: &config.Global{
				SigningIdentity: fclient.SigningIdentity{ServerName: "local", KeyID: "ed25519:1", PrivateKey: key},
			}}
			processCtx := process.NewProcessContext()
			defer processCtx.ShutdownZendrite()
			client := &joinRecordingClient{}
			stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
			queues := queue.NewOutgoingQueues(testDB, processCtx, false, cfg.Matrix.ServerName, client, &stats, nil)
			fedAPI := NewFederationInternalAPI(testDB, &cfg, tc.rs, client, &stats, nil, queues, nil)

			res := api.PerformJoinResponse{}
			fedAPI.PerformJoin(context.Background(), &api.PerformJoinRequest{
				RoomID:      "!room:remote",
				UserID:      "@alice:local",
				ServerNames: []spec.ServerName{"remote"},
			}, &res)

			require.NotNil(t, res.LastError, "the stubbed send_join fails the join")
			assert.Equal(t, tc.want, client.sends)
		})
	}
}
