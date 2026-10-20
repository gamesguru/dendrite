// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

type leaveFedClient struct {
	fclient.FederationClient
	template []byte
	sent     gomatrixserverlib.PDU
}

func (c *leaveFedClient) MakeLeave(_ context.Context, _, _ spec.ServerName, roomID, userID string) (fclient.RespMakeLeave, error) {
	return fclient.RespMakeLeave{
		RoomVersion: gomatrixserverlib.RoomVersionV10,
		LeaveEvent: gomatrixserverlib.ProtoEvent{
			Type:       spec.MRoomMember,
			SenderID:   userID,
			StateKey:   &userID,
			RoomID:     roomID,
			Content:    c.template,
			PrevEvents: []string{"$prev"},
			AuthEvents: []string{"$create"},
			Depth:      2,
		},
	}, nil
}

func (c *leaveFedClient) SendLeave(_ context.Context, _, _ spec.ServerName, event gomatrixserverlib.PDU) error {
	c.sent = event
	return nil
}

type leaveSenderRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
}

func (leaveSenderRoomserverAPI) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(userID.String())
	return &senderID, nil
}

// TestPerformLeaveReason covers MSC2367 for leaving through another server:
// the reason is added to the make_leave template, which otherwise is kept.
func TestPerformLeaveReason(t *testing.T) {
	for name, tc := range map[string]struct {
		template   string
		reason     string
		wantReason string
		wantExtra  bool
	}{
		"no template":              {},
		"no template with reason":  {reason: "farewell", wantReason: "farewell"},
		"template":                 {template: `{"membership":"leave","x":1}`, wantExtra: true},
		"template with reason":     {template: `{"membership":"leave","x":1}`, reason: "farewell", wantReason: "farewell", wantExtra: true},
		"reason replaces template": {template: `{"membership":"leave","reason":"theirs"}`, reason: "mine", wantReason: "mine"},
	} {
		t.Run(name, func(t *testing.T) {
			testDB := test.NewInMemoryFederationDatabase()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			cfg := config.FederationAPI{Matrix: &config.Global{
				SigningIdentity: fclient.SigningIdentity{ServerName: "test", KeyID: "ed25519:1", PrivateKey: key},
			}}
			fedClient := &leaveFedClient{}
			if tc.template != "" {
				fedClient.template = []byte(tc.template)
			}
			stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
			queues := queue.NewOutgoingQueues(testDB, process.NewProcessContext(), false, cfg.Matrix.ServerName, fedClient, &stats, nil)
			fedAPI := NewFederationInternalAPI(testDB, &cfg, leaveSenderRoomserverAPI{}, fedClient, &stats, nil, queues, nil)

			err = fedAPI.PerformLeave(context.Background(), &api.PerformLeaveRequest{
				RoomID:      "!room:remote",
				UserID:      "@alice:test",
				ServerNames: []spec.ServerName{"remote"},
				Reason:      tc.reason,
			}, &api.PerformLeaveResponse{})
			require.NoError(t, err)
			require.NotNil(t, fedClient.sent, "no send_leave")

			content := gjson.ParseBytes(fedClient.sent.Content())
			assert.Equal(t, spec.Leave, content.Get("membership").Str)
			if tc.wantReason == "" {
				assert.False(t, content.Get("reason").Exists(), content.Raw)
			} else {
				assert.Equal(t, tc.wantReason, content.Get("reason").Str)
			}
			assert.Equal(t, tc.wantExtra, content.Get("x").Exists(), content.Raw)
		})
	}
}
