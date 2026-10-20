// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package threepid

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type inviteUserAPI struct {
	userapi.ClientUserAPI
}

func (inviteUserAPI) QueryProfile(_ context.Context, _ string) (*authtypes.Profile, error) {
	return &authtypes.Profile{DisplayName: "Alice", AvatarURL: "mxc://hs.example/alice"}, nil
}

type inviteRoomserverAPI struct {
	api.ClientRoomserverAPI
	state []*types.HeaderedEvent
}

func (a inviteRoomserverAPI) QueryCurrentState(_ context.Context, _ *api.QueryCurrentStateRequest, res *api.QueryCurrentStateResponse) error {
	res.StateEvents = map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{}
	for _, ev := range a.state {
		res.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}] = ev
	}
	return nil
}

// TestLookup3PID covers the v2 lookup, which sends a hash of the 3PID when the
// identity server supports sha256, and the plain 3PID otherwise.
func TestLookup3PID(t *testing.T) {
	for name, tc := range map[string]struct {
		algorithms []string
		wantLookup string
		wantErr    bool
	}{
		// The example of the identity service API's hashing section.
		"sha256":      {algorithms: []string{"none", "sha256"}, wantLookup: "4kenr7N9drpCJ4AfalmlGQVsOn3o2RHjkADUpXJWZUc"},
		"none":        {algorithms: []string{"none"}, wantLookup: "alice@example.com email"},
		"unsupported": {algorithms: []string{"md5"}, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			var lookups []map[string]any
			client := fclient.NewClient(fclient.WithTransport(identityTransport(func(req *http.Request) (*http.Response, error) {
				assert.Equal(t, "Bearer client-token", req.Header.Get("Authorization"))
				switch req.URL.Path {
				case "/_matrix/identity/v2/hash_details":
					details, err := json.Marshal(map[string]any{"algorithms": tc.algorithms, "lookup_pepper": "matrixrocks"})
					require.NoError(t, err)
					return jsonResponse(200, string(details)), nil
				case "/_matrix/identity/v2/lookup":
					var body map[string]any
					require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
					lookups = append(lookups, body)
					mappings, err := json.Marshal(map[string]any{"mappings": map[string]string{tc.wantLookup: "@alice:hs.example"}})
					require.NoError(t, err)
					return jsonResponse(200, string(mappings)), nil
				}
				t.Fatalf("unexpected request %s", req.URL)
				return nil, nil
			})))
			auth := IdentityAuth{Token: "client-token", Config: identityAuthConfig(), Client: client}
			mxid, err := lookup3PID(t.Context(), auth, &MembershipRequest{IDServer: "id.example", Medium: "email", Address: "alice@example.com"})
			if tc.wantErr {
				require.Error(t, err)
				assert.Empty(t, lookups)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "@alice:hs.example", mxid)
			require.Len(t, lookups, 1)
			assert.Equal(t, []any{tc.wantLookup}, lookups[0]["addresses"])
			assert.Equal(t, "matrixrocks", lookups[0]["pepper"])
			assert.Equal(t, name, lookups[0]["algorithm"])
		})
	}
}

// TestStoreInvite covers the v2 store-invite request, which carries the room
// type (MSC3288) and what an invitation email can show about the room.
func TestStoreInvite(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomType("m.space"), test.RoomPreset(test.PresetPrivateChat))
	room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "Kaer Morhen"}, test.WithStateKey(""))
	var state []*types.HeaderedEvent
	for _, ev := range room.CurrentState() {
		if ev.Type() != spec.MRoomMember && ev.Type() != spec.MRoomPowerLevels && ev.Type() != spec.MRoomHistoryVisibility {
			state = append(state, ev)
		}
	}

	var stored map[string]any
	client := fclient.NewClient(fclient.WithTransport(identityTransport(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "/_matrix/identity/v2/store-invite", req.URL.Path)
		assert.Equal(t, "Bearer client-token", req.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(req.Body).Decode(&stored))
		return jsonResponse(200, `{"token":"invite-token","display_name":"a...@e...","public_keys":[]}`), nil
	})))
	cfg := identityAuthConfig()
	_, aliceServer, err := gomatrixserverlib.SplitID('@', alice.ID)
	require.NoError(t, err)
	cfg.Matrix.ServerName = aliceServer
	auth := IdentityAuth{Token: "client-token", UserAPI: inviteUserAPI{}, Config: cfg, Client: client}
	var res *idServerStoreInviteResponse
	res, err = storeInvite(t.Context(), auth, inviteRoomserverAPI{state: state}, &userapi.Device{UserID: alice.ID},
		&MembershipRequest{IDServer: "id.example", Medium: "email", Address: "bob@example.com"}, room.ID)
	require.NoError(t, err)
	assert.Equal(t, "invite-token", res.Token)
	assert.Equal(t, map[string]any{
		"medium":              "email",
		"address":             "bob@example.com",
		"room_id":             room.ID,
		"sender":              alice.ID,
		"sender_display_name": "Alice",
		"sender_avatar_url":   "mxc://hs.example/alice",
		"room_type":           "m.space",
		"room_name":           "Kaer Morhen",
		"room_join_rules":     "invite",
	}, stored)
}

// TestCheckAndProcessInviteNeedsAccessToken covers the id_access_token that
// the spec requires for 3PID invites, without which no identity server is
// contacted.
func TestCheckAndProcessInviteNeedsAccessToken(t *testing.T) {
	body := &MembershipRequest{IDServer: "id.example", Medium: "email", Address: "bob@example.com"}
	_, err := CheckAndProcessInvite(t.Context(), &userapi.Device{UserID: "@alice:hs.example"}, body, identityAuthConfig(), nil, nil, nil, "!room:hs.example", time.Now())
	assert.ErrorAs(t, err, &ErrMissingParameter{})
}
