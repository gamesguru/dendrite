// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package api

import (
	"context"
	"errors"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type restrictedJoinRooms struct {
	RestrictedJoinAPI
	state                     map[string]gomatrixserverlib.PDU
	allowedKnown, userAllowed bool
}

func (r *restrictedJoinRooms) CurrentStateEvent(_ context.Context, _ spec.RoomID, eventType, _ string) (gomatrixserverlib.PDU, error) {
	return r.state[eventType], nil
}

func (r *restrictedJoinRooms) InvitePending(context.Context, spec.RoomID, spec.SenderID) (bool, error) {
	return false, nil
}

func (r *restrictedJoinRooms) QueryRoomInfo(_ context.Context, roomID spec.RoomID) (*types.RoomInfo, error) {
	nid := types.RoomNID(1)
	if roomID.String() == "!allowed:local.test" {
		if !r.allowedKnown {
			return nil, nil
		}
		nid = 2
	}
	return &types.RoomInfo{RoomNID: nid, RoomVersion: gomatrixserverlib.RoomVersionV10}, nil
}

func (r *restrictedJoinRooms) QueryServerJoinedToRoom(_ context.Context, _ *QueryServerJoinedToRoomRequest, res *QueryServerJoinedToRoomResponse) error {
	res.RoomExists = true
	res.IsInRoom = true
	return nil
}

func (r *restrictedJoinRooms) UserJoinedToRoom(_ context.Context, nid types.RoomNID, _ spec.SenderID) (bool, error) {
	return nid == 2 && r.userAllowed, nil
}

func (r *restrictedJoinRooms) LocallyJoinedUsers(context.Context, gomatrixserverlib.RoomVersion, types.RoomNID) ([]gomatrixserverlib.PDU, error) {
	return nil, nil
}

func TestRestrictedJoinErrorCodes(t *testing.T) {
	version := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	state := map[string]gomatrixserverlib.PDU{}
	for eventType, content := range map[string]string{
		spec.MRoomJoinRules:   `{"join_rule":"restricted","allow":[{"type":"m.room_membership","room_id":"!allowed:local.test"}]}`,
		spec.MRoomPowerLevels: `{"invite":50}`,
	} {
		event, err := version.NewEventFromTrustedJSON([]byte(`{"room_id":"!target:local.test","sender":"@alice:local.test","state_key":"","type":"`+eventType+`","content":`+content+`}`), false)
		require.NoError(t, err)
		state[eventType] = event
	}
	roomID, err := spec.NewRoomID("!target:local.test")
	require.NoError(t, err)
	for _, tc := range []struct {
		name           string
		known, allowed bool
		code           spec.MatrixErrorCode
	}{
		{name: "uncheckable condition", code: ErrorUnableToAuthoriseJoin},
		{name: "no local authorizing user", known: true, allowed: true, code: ErrorUnableToGrantJoin},
		{name: "not in allowed room", known: true, code: spec.ErrorForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			querier := &JoinRoomQuerier{Roomserver: &restrictedJoinRooms{state: state, allowedKnown: tc.known, userAllowed: tc.allowed}, TargetRoomID: roomID.String()}
			_, err := version.CheckRestrictedJoin(t.Context(), "local.test", querier, *roomID, "@bob:remote.test")
			err = querier.RestrictedJoinError(err)
			var matrixErr spec.MatrixError
			require.True(t, errors.As(err, &matrixErr))
			require.Equal(t, tc.code, matrixErr.ErrCode)
		})
	}
}
