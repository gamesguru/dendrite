// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/federationapi/routing"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

const inviteLocalServer = spec.ServerName("localhost")

// inviteRoomserverAPI covers the roomserver calls made while handling an
// invite for a room this server has not joined.
type inviteRoomserverAPI struct {
	rsapi.FederationRoomserverAPI
	handled []*rstypes.HeaderedEvent
}

func (r *inviteRoomserverAPI) IsKnownRoom(context.Context, spec.RoomID) (bool, error) {
	return false, nil
}

func (r *inviteRoomserverAPI) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (r *inviteRoomserverAPI) StateQuerier() gomatrixserverlib.StateQuerier {
	return noStateQuerier{}
}

func (r *inviteRoomserverAPI) HandleInvite(_ context.Context, event *rstypes.HeaderedEvent) error {
	r.handled = append(r.handled, event)
	return nil
}

type noStateQuerier struct{}

func (noStateQuerier) GetAuthEvents(context.Context, gomatrixserverlib.PDU) (gomatrixserverlib.AuthEventProvider, error) {
	return nil, fmt.Errorf("room is unknown")
}

func (noStateQuerier) GetState(context.Context, spec.RoomID, []gomatrixserverlib.StateKeyTuple) ([]gomatrixserverlib.PDU, error) {
	return nil, nil
}

// inviteState builds invite_room_state entries as full PDUs, or in the
// pre-MSC4311 stripped form when stripped is set.
func inviteState(t *testing.T, events []*rstypes.HeaderedEvent, stripped bool) []json.RawMessage {
	t.Helper()
	entries := make([]json.RawMessage, 0, len(events))
	for _, ev := range events {
		if !stripped {
			entries = append(entries, json.RawMessage(ev.JSON()))
			continue
		}
		raw, err := json.Marshal(gomatrixserverlib.NewInviteStrippedState(ev.PDU))
		require.NoError(t, err)
		entries = append(entries, raw)
	}
	return entries
}

func sendInviteV2(t *testing.T, room *test.Room, invite *rstypes.HeaderedEvent, state []json.RawMessage) (rsAPI *inviteRoomserverAPI, code int, body []byte) {
	t.Helper()
	_, sk, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cfg := &config.FederationAPI{Matrix: &config.Global{
		SigningIdentity: fclient.SigningIdentity{
			ServerName: inviteLocalServer,
			KeyID:      "ed25519:local",
			PrivateKey: sk,
		},
	}}

	reqBody, err := json.Marshal(map[string]any{
		"room_version":      room.Version,
		"event":             json.RawMessage(invite.JSON()),
		"invite_room_state": state,
	})
	require.NoError(t, err)

	origin := spec.ServerName(gjson.GetBytes(invite.JSON(), "origin").Str)
	path := fmt.Sprintf("/_matrix/federation/v2/invite/%s/%s", room.ID, invite.EventID())
	req := fclient.NewFederationRequest(http.MethodPut, origin, inviteLocalServer, path)
	require.NoError(t, req.SetContent(json.RawMessage(reqBody)))
	httpReq, err := req.HTTPRequest()
	require.NoError(t, err)

	roomID, err := spec.NewRoomID(room.ID)
	require.NoError(t, err)
	rsAPI = &inviteRoomserverAPI{}
	res := routing.InviteV2(httpReq, &req, *roomID, invite.EventID(), cfg, rsAPI, &test.NopJSONVerifier{})
	body, err = json.Marshal(res.JSON)
	require.NoError(t, err)
	return rsAPI, res.Code, body
}

func TestInviteV2ValidatesInviteRoomState(t *testing.T) {
	alice := test.NewUser(t)
	invitee := "@bob:" + string(inviteLocalServer)

	newRoom := func(creator *test.User, version gomatrixserverlib.RoomVersion) (*test.Room, *rstypes.HeaderedEvent, []*rstypes.HeaderedEvent) {
		room := test.NewRoom(t, creator, test.RoomVersion(version), test.RoomPreset(test.PresetPrivateChat))
		name := room.CreateAndInsert(t, creator, spec.MRoomName, map[string]any{"name": "Kaer Morhen"}, test.WithStateKey(""))
		invite := room.CreateEvent(t, creator, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(invitee))
		var create *rstypes.HeaderedEvent
		for _, ev := range room.CurrentState() {
			if ev.Type() == spec.MRoomCreate {
				create = ev
			}
		}
		require.NotNil(t, create)
		return room, invite, []*rstypes.HeaderedEvent{create, name}
	}

	v10Room, v10Invite, v10State := newRoom(alice, gomatrixserverlib.RoomVersionV10)
	v12Room, v12Invite, v12State := newRoom(alice, gomatrixserverlib.RoomVersionV12)
	// A different creator, as identical create events hash to the same room ID.
	otherV12Room, _, otherV12State := newRoom(test.NewUser(t), gomatrixserverlib.RoomVersionV12)
	require.NotEqual(t, v12Room.ID, otherV12Room.ID)

	tests := map[string]struct {
		room        *test.Room
		invite      *rstypes.HeaderedEvent
		state       []json.RawMessage
		wantCode    int
		wantErrCode spec.MatrixErrorCode
	}{
		"v12 with PDU state": {
			room: v12Room, invite: v12Invite,
			state:    inviteState(t, v12State, false),
			wantCode: http.StatusOK,
		},
		// MSC4311's migration section: state which is missing or still in
		// stripped form only warns, so servers predating MSC4311 can invite.
		"v12 with stripped state": {
			room: v12Room, invite: v12Invite,
			state:    inviteState(t, v12State, true),
			wantCode: http.StatusOK,
		},
		"v12 without create event": {
			room: v12Room, invite: v12Invite,
			state:    inviteState(t, v12State[1:], false),
			wantCode: http.StatusOK,
		},
		"v12 with another room's create event": {
			room: v12Room, invite: v12Invite,
			state:    inviteState(t, append(otherV12State[:1:1], v12State[1]), false),
			wantCode: http.StatusBadRequest, wantErrCode: spec.ErrorMissingParam,
		},
		"v12 without state": {
			room: v12Room, invite: v12Invite,
			wantCode: http.StatusOK,
		},
		"v10 with PDU state": {
			room: v10Room, invite: v10Invite,
			state:    inviteState(t, v10State, false),
			wantCode: http.StatusOK,
		},
		// Room versions 1 through 11 accept invites from servers which predate MSC4311.
		"v10 with stripped state": {
			room: v10Room, invite: v10Invite,
			state:    inviteState(t, v10State, true),
			wantCode: http.StatusOK,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rsAPI, code, body := sendInviteV2(t, tc.room, tc.invite, tc.state)
			require.Equal(t, tc.wantCode, code, string(body))
			if tc.wantErrCode != "" {
				assert.Equal(t, string(tc.wantErrCode), gjson.GetBytes(body, "errcode").Str)
				assert.Empty(t, rsAPI.handled, "rejected invite must not reach the roomserver")
				return
			}

			require.Len(t, rsAPI.handled, 1)
			// Clients only ever see the stripped form, including the create
			// event whenever the inviting server sent one.
			var sentCreate, sawCreate bool
			for _, entry := range tc.state {
				sentCreate = sentCreate || gjson.GetBytes(entry, "type").Str == spec.MRoomCreate
			}
			for _, entry := range gjson.GetBytes(rsAPI.handled[0].Unsigned(), "invite_room_state").Array() {
				for _, field := range []string{"event_id", "signatures", "hashes", "origin_server_ts"} {
					assert.False(t, entry.Get(field).Exists(), "invite_room_state leaked %s: %s", field, entry.Raw)
				}
				if entry.Get("type").Str == spec.MRoomCreate {
					sawCreate = true
				}
			}
			assert.Equal(t, sentCreate, sawCreate, "m.room.create in invite_room_state")
		})
	}
}
