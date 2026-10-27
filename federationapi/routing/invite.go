// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/msc4155"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// InviteV3 implements /_matrix/federation/v2/invite/{roomID}/{userID}.
func InviteV3(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	invitedUser spec.UserID,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	userAPI userapi.FederationUserAPI,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	inviteReq := fclient.InviteV3Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	}
	if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}
	if hasInvalidV12InviteState(string(inviteReq.RoomVersion()), inviteReq.InviteRoomState()) {
		// Accepted leniently until the ecosystem has adapted to MSC4311.
		util.GetLogger(httpReq.Context()).Warn("invite_room_state has no m.room.create event for room version 12 or later")
	}
	ignoredInvite, ruleResp := inviteDisposition(httpReq.Context(), rsAPI, userAPI, roomID, invitedUser, spec.SenderID(inviteReq.Event().SenderID), request.Origin())
	if ruleResp != nil {
		return *ruleResp
	}

	input := gomatrixserverlib.HandleInviteV3Input{
		HandleInviteInput: gomatrixserverlib.HandleInviteInput{
			RoomVersion:       inviteReq.RoomVersion(),
			RoomID:            roomID,
			InvitedUser:       invitedUser,
			KeyID:             cfg.Matrix.KeyID,
			PrivateKey:        cfg.Matrix.PrivateKey,
			Verifier:          keys,
			RoomQuerier:       rsAPI,
			MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
			StateQuerier:      rsAPI.StateQuerier(),
			InviteEvent:       nil,
			StrippedState:     inviteReq.InviteRoomState(),
			UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
				return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
			},
		},
		InviteProtoEvent: inviteReq.Event(),
		GetOrCreateSenderID: func(ctx context.Context, userID spec.UserID, roomID spec.RoomID, roomVersion string) (spec.SenderID, ed25519.PrivateKey, error) {
			// assign a roomNID, otherwise we can't create a private key for the user
			_, nidErr := rsAPI.AssignRoomNID(ctx, roomID, gomatrixserverlib.RoomVersion(roomVersion))
			if nidErr != nil {
				return "", nil, nidErr
			}
			key, keyErr := rsAPI.GetOrCreateUserRoomPrivateKey(ctx, userID, roomID)
			if keyErr != nil {
				return "", nil, keyErr
			}

			return spec.SenderIDFromPseudoIDKey(key), key, nil
		},
	}
	event, jsonErr := handleInviteV3(httpReq.Context(), input, rsAPI, ignoredInvite)
	if jsonErr != nil {
		return *jsonErr
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInviteV2{Event: event.JSON()},
	}
}

// InviteV2 implements /_matrix/federation/v2/invite/{roomID}/{eventID}.
func InviteV2(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	userAPI userapi.FederationUserAPI,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	inviteReq := fclient.InviteV2Request{}
	err := json.Unmarshal(request.Content(), &inviteReq)
	var unsupportedVersionErr gomatrixserverlib.UnsupportedRoomVersionError
	var badJSONErr gomatrixserverlib.BadJSONError
	switch {
	case errors.As(err, &unsupportedVersionErr):
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(
				fmt.Sprintf("Room version %q is not supported by this server.", unsupportedVersionErr.Version),
			),
		}
	case errors.As(err, &badJSONErr):
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	case err != nil:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into an invite request. " + err.Error()),
		}
	}

	if inviteReq.Event().StateKey() == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The invite event has no state key"),
		}
	}

	invitedUser, userErr := spec.NewUserID(*inviteReq.Event().StateKey(), true)
	if userErr != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID is invalid"),
		}
	}
	if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	if inviteReq.Event().EventID() != eventID {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
		}
	}
	if hasInvalidV12InviteState(string(inviteReq.RoomVersion()), inviteReq.InviteRoomState()) {
		// Accepted leniently until the ecosystem has adapted to MSC4311.
		util.GetLogger(httpReq.Context()).Warn("invite_room_state has no m.room.create event for room version 12 or later")
	}
	ignoredInvite, ruleResp := inviteDisposition(httpReq.Context(), rsAPI, userAPI, roomID, *invitedUser, inviteReq.Event().SenderID(), request.Origin())
	if ruleResp != nil {
		return *ruleResp
	}

	input := gomatrixserverlib.HandleInviteInput{
		RoomVersion:       inviteReq.RoomVersion(),
		RoomID:            roomID,
		InvitedUser:       *invitedUser,
		KeyID:             cfg.Matrix.KeyID,
		PrivateKey:        cfg.Matrix.PrivateKey,
		Verifier:          keys,
		RoomQuerier:       rsAPI,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
		StateQuerier:      rsAPI.StateQuerier(),
		InviteEvent:       inviteReq.Event(),
		StrippedState:     inviteReq.InviteRoomState(),
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}
	event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI, ignoredInvite)
	if jsonErr != nil {
		return *jsonErr
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInviteV2{Event: event.JSON()},
	}
}

func hasInvalidV12InviteState(roomVersion string, strippedState []gomatrixserverlib.InviteStrippedState) bool {
	version, err := strconv.Atoi(roomVersion)
	if err != nil || version < 12 {
		return false
	}
	for _, event := range strippedState {
		if event.Type() == spec.MRoomCreate {
			return false
		}
	}
	return true
}

// InviteV1 implements /_matrix/federation/v1/invite/{roomID}/{eventID}.
func InviteV1(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	roomID spec.RoomID,
	eventID string,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	userAPI msc4155.AccountDataQuerier,
	keys gomatrixserverlib.JSONVerifier,
) util.JSONResponse {
	roomVer := gomatrixserverlib.RoomVersionV1
	body := request.Content()
	// roomVer is hardcoded to v1 so we know we won't panic on Must
	event, err := gomatrixserverlib.MustGetRoomVersion(roomVer).NewEventFromTrustedJSON(body, false)
	var badJSONErr gomatrixserverlib.BadJSONError
	if errors.As(err, &badJSONErr) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	} else if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into an invite v1 request. " + err.Error()),
		}
	}
	var strippedState []gomatrixserverlib.InviteStrippedState
	if inviteRoomState := gjson.GetBytes(event.Unsigned(), "invite_room_state"); inviteRoomState.IsArray() {
		if jsonErr := json.Unmarshal([]byte(inviteRoomState.Raw), &strippedState); jsonErr != nil {
			// just warn, they may not have added any.
			util.GetLogger(httpReq.Context()).Warnf("failed to extract stripped state from invite event")
		}
	}

	if event.StateKey() == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The invite event has no state key"),
		}
	}

	invitedUser, err := spec.NewUserID(*event.StateKey(), true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID is invalid"),
		}
	}
	if !cfg.Matrix.IsLocalServerName(invitedUser.Domain()) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The invited user domain does not belong to this server"),
		}
	}

	if event.EventID() != eventID {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the invite event JSON"),
		}
	}

	ignoredInvite, ruleResp := inviteDisposition(httpReq.Context(), rsAPI, userAPI, roomID, *invitedUser, event.SenderID(), request.Origin())
	if ruleResp != nil {
		return *ruleResp
	}

	input := gomatrixserverlib.HandleInviteInput{
		RoomVersion:       roomVer,
		RoomID:            roomID,
		InvitedUser:       *invitedUser,
		KeyID:             cfg.Matrix.KeyID,
		PrivateKey:        cfg.Matrix.PrivateKey,
		Verifier:          keys,
		RoomQuerier:       rsAPI,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
		StateQuerier:      rsAPI.StateQuerier(),
		InviteEvent:       event,
		StrippedState:     strippedState,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
	}
	event, jsonErr := handleInvite(httpReq.Context(), input, rsAPI, ignoredInvite)
	if jsonErr != nil {
		return *jsonErr
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespInvite{Event: event.JSON()},
	}
}

func handleInvite(ctx context.Context, input gomatrixserverlib.HandleInviteInput, rsAPI api.FederationRoomserverAPI, ignored bool) (gomatrixserverlib.PDU, *util.JSONResponse) {
	inviteEvent, err := gomatrixserverlib.HandleInvite(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI, ignored)
}

func handleInviteV3(ctx context.Context, input gomatrixserverlib.HandleInviteV3Input, rsAPI api.FederationRoomserverAPI, ignored bool) (gomatrixserverlib.PDU, *util.JSONResponse) {
	inviteEvent, err := gomatrixserverlib.HandleInviteV3(ctx, input)
	return handleInviteResult(ctx, inviteEvent, err, rsAPI, ignored)
}

func handleInviteResult(ctx context.Context, inviteEvent gomatrixserverlib.PDU, err error, rsAPI api.FederationRoomserverAPI, ignored bool) (gomatrixserverlib.PDU, *util.JSONResponse) {
	var internalErr spec.InternalServerError
	var matrixErr spec.MatrixError
	switch {
	case err == nil:
		// no error, fall through to processing below
	case errors.As(err, &internalErr):
		util.GetLogger(ctx).WithError(err)
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case errors.As(err, &matrixErr):
		util.GetLogger(ctx).WithError(err)
		code := http.StatusInternalServerError
		switch matrixErr.ErrCode {
		case spec.ErrorForbidden:
			code = http.StatusForbidden
		case spec.ErrorUnsupportedRoomVersion, spec.ErrorBadJSON, spec.ErrorMissingParam:
			code = http.StatusBadRequest
		}

		return nil, &util.JSONResponse{
			Code: code,
			JSON: matrixErr,
		}
	default:
		util.GetLogger(ctx).WithError(err)
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("unknown error"),
		}
	}

	if ignored {
		return inviteEvent, nil
	}
	headeredInvite := &types.HeaderedEvent{PDU: inviteEvent}
	if err = rsAPI.HandleInvite(ctx, headeredInvite); err != nil {
		util.GetLogger(ctx).WithError(err).Error("HandleInvite failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return inviteEvent, nil
}

// inviteDisposition applies the invitee's MSC4155 rules before an invite is
// handled. A sender that cannot be resolved yet, such as a pseudo-ID in a room
// unknown locally, is judged by the origin server's rules instead. Ignored
// invites are still signed normally, so the sender cannot tell, but are not
// stored or delivered to the invitee's sync.
func inviteDisposition(
	ctx context.Context, rsAPI api.FederationRoomserverAPI, userAPI msc4155.AccountDataQuerier,
	roomID spec.RoomID, invitedUser spec.UserID, sender spec.SenderID, origin spec.ServerName,
) (bool, *util.JSONResponse) {
	if sender == "" || userAPI == nil {
		return false, nil
	}
	var blocked, ignored bool
	var err error
	senderUser, senderErr := inviteSenderUserID(ctx, rsAPI, roomID, sender)
	switch {
	case senderErr != nil:
		resp := util.ErrorResponse(senderErr)
		return false, &resp
	case senderUser != nil:
		blocked, ignored, err = msc4155.Disposition(ctx, userAPI, invitedUser, senderUser.String())
	default:
		blocked, ignored, err = msc4155.ServerDisposition(ctx, userAPI, invitedUser, origin)
	}
	if err != nil {
		resp := util.ErrorResponse(err)
		return false, &resp
	}
	if blocked {
		return false, &util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("Invites from this user or server are blocked")}
	}
	return ignored, nil
}

// inviteSenderUserID resolves the sender of an invite for the invite rules. A
// room that is not known locally yet has no sender mapping, but a sender ID
// that is a plain user ID can still be used as is.
func inviteSenderUserID(ctx context.Context, rsAPI api.FederationRoomserverAPI, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	user, err := rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	if err == nil && user != nil {
		return user, nil
	}
	if parsed, parseErr := spec.NewUserID(string(senderID), true); parseErr == nil {
		return parsed, nil
	}
	return user, err
}
