// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"math"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type getMembershipResponse struct {
	Chunk []synctypes.ClientEvent `json:"chunk"`
}

// GetMemberships implements
//
//	GET /rooms/{roomId}/members
func GetMemberships(
	req *http.Request, device *userapi.Device, roomID string,
	syncDB storage.Database, rsAPI api.SyncRoomserverAPI,
	membership, notMembership *string, at string,
) util.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Device UserID is invalid"),
		}
	}
	queryReq := api.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *userID,
	}

	var queryRes api.QueryMembershipForUserResponse
	if queryErr := rsAPI.QueryMembershipForUser(req.Context(), &queryReq, &queryRes); queryErr != nil {
		util.GetLogger(req.Context()).WithError(queryErr).Error("rsAPI.QueryMembershipsForRoom failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !queryRes.HasBeenInRoom {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("You aren't a member of the room and weren't previously a member of the room."),
		}
	}

	// A room joined with partial state does not know all its members yet, so
	// wait for the resync to finish before answering (MSC3706).
	if err = rsAPI.AwaitRoomFullState(req.Context(), roomID); err != nil {
		util.GetLogger(req.Context()).WithError(err).Warn("Gave up waiting for the room's state to be resynced")
		return util.JSONResponse{
			Code: http.StatusGatewayTimeout,
			JSON: spec.Unknown("Timed out waiting for the room's state to be resynced"),
		}
	}

	db, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	defer db.Rollback() //nolint:errcheck

	atToken, err := parseMembershipAtToken(at)
	if err != nil {
		atToken = types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}
		if queryRes.HasBeenInRoom && !queryRes.IsInRoom {
			// If you have left the room then this will be the members of the room when you left.
			atToken, err = db.EventPositionInTopology(req.Context(), queryRes.EventID)
			if err != nil {
				util.GetLogger(req.Context()).WithError(err).Error("unable to get 'atToken'")
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
	}

	eventIDs, err := db.SelectMemberships(req.Context(), roomID, atToken, membership, notMembership)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("db.SelectMemberships failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	qryRes := &api.QueryEventsByIDResponse{}
	if err := rsAPI.QueryEventsByID(req.Context(), &api.QueryEventsByIDRequest{EventIDs: eventIDs, RoomID: roomID}, qryRes); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("rsAPI.QueryEventsByID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	result := qryRes.Events

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: getMembershipResponse{synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(result), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(req.Context(), roomID, senderID)
		})},
	}
}

// parseMembershipAtToken accepts both topology tokens and /sync stream tokens.
// MaxInt64 depth marks a stream-only token for SelectMemberships.
func parseMembershipAtToken(at string) (types.TopologyToken, error) {
	atToken, err := types.NewTopologyTokenFromString(at)
	if err == nil {
		return atToken, nil
	}
	streamToken, streamErr := types.NewStreamTokenFromString(at)
	if streamErr != nil {
		return types.TopologyToken{}, err
	}
	return types.TopologyToken{Depth: math.MaxInt64, PDUPosition: streamToken.PDUPosition}, nil
}
