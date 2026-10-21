// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
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
