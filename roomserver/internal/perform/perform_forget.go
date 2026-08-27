// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
)

type Forgetter struct {
	DB    storage.Database
	RSAPI api.RoomserverInternalAPI
}

// PerformForget implements api.RoomServerQueryAPI.
func (f *Forgetter) PerformForget(
	ctx context.Context,
	request *api.PerformForgetRequest,
	response *api.PerformForgetResponse,
) error {
	// Membership rows are keyed by the room-specific sender ID, which differs
	// from the user ID in pseudo-ID rooms (room version 11+). Resolve it so the
	// correct row is marked forgotten; otherwise /forget is a silent no-op for
	// those rooms (and never triggers auto-purge).
	target := request.UserID
	if f.RSAPI != nil {
		if roomID, err := spec.NewRoomID(request.RoomID); err == nil {
			if userID, err := spec.NewUserID(request.UserID, true); err == nil {
				if senderID, err := f.RSAPI.QuerySenderIDForUser(ctx, *roomID, *userID); err == nil && senderID != nil {
					target = string(*senderID)
				}
			}
		}
	}

	if err := f.DB.ForgetRoom(ctx, target, request.RoomID, true); err != nil {
		return err
	}
	// Under AutoPurgeOnAllForgotten this /forget may have been the last
	// non-forgotten membership row, so re-evaluate auto-purge. The schedule
	// helper is a no-op under modes where forget is not a trigger
	// (never, on_empty).
	if f.RSAPI != nil {
		f.RSAPI.ScheduleAutoPurgeIfEligible(ctx, request.RoomID)
	}
	return nil
}
