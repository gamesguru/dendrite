// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// exchangeFederationClient records the servers asked to exchange a 3PID
// invite, failing for those in fail.
type exchangeFederationClient struct {
	fclient.FederationClient
	fail    map[spec.ServerName]bool
	servers []spec.ServerName
}

func (f *exchangeFederationClient) ExchangeThirdPartyInvite(_ context.Context, _, s spec.ServerName, _ gomatrixserverlib.ProtoEvent) error {
	f.servers = append(f.servers, s)
	if f.fail[s] {
		return errors.New("unreachable")
	}
	return nil
}

func TestSendToRemoteServer(t *testing.T) {
	cfg := &config.FederationAPI{Matrix: &config.Global{SigningIdentity: fclient.SigningIdentity{ServerName: "local.test"}}}
	const v12RoomID = "!BFgpzfjEm2ai5uC3G4Esft1Yf2Lb7W9DGKfWUDJAXwU"
	for _, tc := range []struct {
		name    string
		sender  string
		roomID  string
		fail    map[spec.ServerName]bool
		servers []spec.ServerName
		wantErr bool
	}{
		{
			name:    "v11 room falls back to the room ID's server",
			sender:  "@alice:remote.test",
			roomID:  "!room:origin.test",
			fail:    map[spec.ServerName]bool{"remote.test": true},
			servers: []spec.ServerName{"remote.test", "origin.test"},
		},
		{
			name:    "v11 room skips the local sender's server",
			sender:  "@alice:local.test",
			roomID:  "!room:origin.test",
			servers: []spec.ServerName{"origin.test"},
		},
		{
			name:    "v12 room is sent to the sender's server",
			sender:  "@alice:remote.test",
			roomID:  v12RoomID,
			servers: []spec.ServerName{"remote.test"},
		},
		{
			name:    "v12 room with a failing sender's server has no fallback",
			sender:  "@alice:remote.test",
			roomID:  v12RoomID,
			fail:    map[spec.ServerName]bool{"remote.test": true},
			servers: []spec.ServerName{"remote.test"},
			wantErr: true,
		},
		{
			name:    "v12 room with a local sender names no server",
			sender:  "@alice:local.test",
			roomID:  v12RoomID,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			federation := &exchangeFederationClient{fail: tc.fail}
			inv := invite{Sender: tc.sender, RoomID: tc.roomID}
			var err error
			require.NotPanics(t, func() {
				err = sendToRemoteServer(context.Background(), inv, federation, cfg, gomatrixserverlib.ProtoEvent{})
			})
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.servers, federation.servers)
		})
	}
}
