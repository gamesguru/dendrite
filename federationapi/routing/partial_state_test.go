// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type partialStateRoomserverAPI struct {
	api.FederationRoomserverAPI
	info       *types.RoomInfo
	infoErr    error
	partial    bool
	partialErr error
}

func (a partialStateRoomserverAPI) QueryRoomInfo(context.Context, spec.RoomID) (*types.RoomInfo, error) {
	return a.info, a.infoErr
}

func (a partialStateRoomserverAPI) IsRoomPartialState(context.Context, types.RoomNID) (bool, error) {
	return a.partial, a.partialErr
}

func TestRejectPartialStateRoom(t *testing.T) {
	known := &types.RoomInfo{RoomNID: 1}
	for name, tc := range map[string]struct {
		rs         partialStateRoomserverAPI
		roomID     string
		wantReject bool
	}{
		"partial state room":  {rs: partialStateRoomserverAPI{info: known, partial: true}, roomID: "!r:test", wantReject: true},
		"fully stated room":   {rs: partialStateRoomserverAPI{info: known}, roomID: "!r:test"},
		"unknown room":        {rs: partialStateRoomserverAPI{}, roomID: "!r:test"},
		"room lookup fails":   {rs: partialStateRoomserverAPI{infoErr: errors.New("db down")}, roomID: "!r:test"},
		"partial check fails": {rs: partialStateRoomserverAPI{info: known, partialErr: errors.New("db down")}, roomID: "!r:test"},
		"invalid room id":     {rs: partialStateRoomserverAPI{info: known, partial: true}, roomID: "not a room"},
	} {
		t.Run(name, func(t *testing.T) {
			resp := rejectPartialStateRoom(context.Background(), tc.rs, tc.roomID)
			if !tc.wantReject {
				assert.Nil(t, resp)
				return
			}
			require.NotNil(t, resp)
			assert.Equal(t, http.StatusNotFound, resp.Code)
			matrixErr, ok := resp.JSON.(spec.MatrixError)
			require.True(t, ok, "the body is a Matrix error")
			assert.Equal(t, spec.ErrorNotFound, matrixErr.ErrCode)
		})
	}
}
