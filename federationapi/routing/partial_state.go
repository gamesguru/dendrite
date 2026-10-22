// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
)

// rejectPartialStateRoom returns a 404 when this server has joined the room only
// with partial state (MSC3706). It does not know the room's members yet, so it
// cannot authorize a join or knock, and tells the other server to ask elsewhere.
// It returns nil for any other room, including one it cannot look up, and leaves
// the handler to report that.
func rejectPartialStateRoom(ctx context.Context, rsAPI api.FederationRoomserverAPI, roomID string) *util.JSONResponse {
	room, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil
	}
	info, err := rsAPI.QueryRoomInfo(ctx, *room)
	if err != nil || info == nil {
		return nil
	}
	partial, err := rsAPI.IsRoomPartialState(ctx, info.RoomNID)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Warn("Failed to check whether the room has partial state")
		return nil
	}
	if !partial {
		return nil
	}
	return &util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("The room's state is still being resynced, so this server cannot take part in joins yet"),
	}
}
