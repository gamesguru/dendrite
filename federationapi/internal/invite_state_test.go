// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

type inviteFedClient struct {
	fclient.FederationClient
	sent    []byte
	respond error
}

func (c *inviteFedClient) SendInviteV2(_ context.Context, _, _ spec.ServerName, request fclient.InviteV2Request) (fclient.RespInviteV2, error) {
	sent, err := json.Marshal(request)
	if err != nil {
		return fclient.RespInviteV2{}, err
	}
	c.sent = sent
	if c.respond != nil {
		return fclient.RespInviteV2{}, c.respond
	}
	event := request.Event()
	return fclient.RespInviteV2{Event: event.JSON()}, nil
}

type inviteSenderRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
}

func (inviteSenderRoomserverAPI) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func newInviteSender(t *testing.T, fedClient *inviteFedClient) *FederationInternalAPI {
	t.Helper()
	testDB := test.NewInMemoryFederationDatabase()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cfg := config.FederationAPI{Matrix: &config.Global{
		SigningIdentity: fclient.SigningIdentity{ServerName: "test", KeyID: "ed25519:1", PrivateKey: key},
	}}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(testDB, process.NewProcessContext(), false, cfg.Matrix.ServerName, fedClient, &stats, nil)
	return NewFederationInternalAPI(testDB, &cfg, inviteSenderRoomserverAPI{}, fedClient, &stats, nil, queues, nil)
}

func TestSendInviteSendsRoomStateAsPDUs(t *testing.T) {
	for _, version := range []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV10, gomatrixserverlib.RoomVersionV12} {
		t.Run(string(version), func(t *testing.T) {
			alice := test.NewUser(t)
			room := test.NewRoom(t, alice, test.RoomVersion(version))
			invite := room.CreateEvent(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey("@bob:remote"))

			var state []gomatrixserverlib.InviteStrippedState
			byType := map[string]*rstypes.HeaderedEvent{}
			for _, ev := range room.CurrentState() {
				state = append(state, gomatrixserverlib.NewInviteStrippedState(ev.PDU))
				byType[ev.Type()] = ev
			}
			require.Contains(t, byType, spec.MRoomCreate)

			fedClient := &inviteFedClient{}
			_, err := newInviteSender(t, fedClient).SendInvite(context.Background(), invite.PDU, state)
			require.NoError(t, err)

			entries := gjson.GetBytes(fedClient.sent, "invite_room_state").Array()
			require.Len(t, entries, len(state))
			var sawCreate bool
			for _, entry := range entries {
				want, ok := byType[entry.Get("type").Str]
				require.True(t, ok, "unexpected entry %s", entry.Raw)
				assert.JSONEq(t, string(want.JSON()), entry.Raw, "entry must be the full PDU")
				sawCreate = sawCreate || want.Type() == spec.MRoomCreate
			}
			assert.True(t, sawCreate, "invite_room_state lacks m.room.create")
		})
	}
}

// gomatrixserverlib.PerformInvite maps a remote M_MISSING_PARAM to a 5xx for
// the client, which needs the HTTP error to survive SendInvite's wrapping.
func TestSendInviteKeepsRemoteMissingParamError(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomVersion(gomatrixserverlib.RoomVersionV12))
	invite := room.CreateEvent(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey("@bob:remote"))

	remoteErr := spec.HTTPError{
		Code:         http.StatusBadRequest,
		WrappedError: spec.RespError{ErrCode: string(spec.ErrorMissingParam), Err: "Create event not among invite state entries."},
	}
	_, err := newInviteSender(t, &inviteFedClient{respond: remoteErr}).SendInvite(context.Background(), invite.PDU, nil)
	var httpErr spec.HTTPError
	require.True(t, errors.As(err, &httpErr), "got %v", err)
	assert.Equal(t, remoteErr.WrappedError, httpErr.WrappedError)
}
