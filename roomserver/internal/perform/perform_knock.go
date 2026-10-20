// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

type Knocker struct {
	Cfg     *config.RoomServer
	FSAPI   fsAPI.RoomserverFederationAPI
	RSAPI   rsAPI.RoomserverInternalAPI
	DB      storage.Database
	Inputer *input.Inputer
}

// PerformKnock knocks on a room on behalf of a local user, locally if this
// server is in the room and over federation otherwise. It returns the room ID
// and the output events announcing the knock.
func (r *Knocker) PerformKnock(
	ctx context.Context,
	req *rsAPI.PerformKnockRequest,
) (string, []rsAPI.OutputEvent, error) {
	if !r.Cfg.Matrix.IsLocalServerName(req.UserID.Domain()) {
		return "", nil, rsAPI.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", req.UserID.String())}
	}

	servers := req.ServerNames
	roomIDStr := req.RoomIDOrAlias
	if strings.HasPrefix(roomIDStr, "#") {
		resolved, aliasServers, err := resolveRoomAlias(ctx, r.Cfg, r.FSAPI, r.RSAPI, roomIDStr)
		if err != nil {
			return "", nil, err
		}
		roomIDStr = resolved
		servers = append(servers, aliasServers...)
	}
	roomID, err := spec.NewRoomID(roomIDStr)
	if err != nil {
		return "", nil, rsAPI.ErrInvalidID{Err: fmt.Errorf("room ID %q is invalid: %w", roomIDStr, err)}
	}
	// Room versions before 12 name a server which was in the room.
	if domain := eventutil.RoomIDDomain(*roomID); domain != "" {
		servers = append(servers, domain)
	}

	inRoomRes := rsAPI.QueryServerJoinedToRoomResponse{}
	if err = r.RSAPI.QueryServerJoinedToRoom(ctx, &rsAPI.QueryServerJoinedToRoomRequest{RoomID: roomID.String()}, &inRoomRes); err != nil {
		return "", nil, fmt.Errorf("QueryServerJoinedToRoom: %w", err)
	}

	content := map[string]any{}
	for k, v := range req.Content {
		content[k] = v
	}
	content["membership"] = spec.Knock

	var outputs []rsAPI.OutputEvent
	if inRoomRes.RoomExists && inRoomRes.IsInRoom {
		outputs, err = r.performLocalKnock(ctx, *roomID, req.UserID, content)
	} else {
		outputs, err = r.performFederatedKnock(ctx, *roomID, req.UserID, content, servers)
	}
	if err != nil {
		return "", nil, err
	}
	return roomID.String(), outputs, nil
}

func (r *Knocker) performLocalKnock(
	ctx context.Context, roomID spec.RoomID, userID spec.UserID, content map[string]any,
) ([]rsAPI.OutputEvent, error) {
	senderID := userID.String()
	proto := gomatrixserverlib.ProtoEvent{
		Type:     spec.MRoomMember,
		SenderID: senderID,
		StateKey: &senderID,
		RoomID:   roomID.String(),
	}
	if err := proto.SetContent(content); err != nil {
		return nil, fmt.Errorf("proto.SetContent: %w", err)
	}
	identity, err := r.RSAPI.SigningIdentityFor(ctx, roomID, userID)
	if err != nil {
		return nil, fmt.Errorf("SigningIdentityFor: %w", err)
	}
	var buildRes rsAPI.QueryLatestEventsAndStateResponse
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, time.Now(), r.RSAPI, &buildRes)
	if err != nil {
		var errNoExists eventutil.ErrRoomNoExists
		if errors.As(err, &errNoExists) {
			return nil, spec.NotFound(fmt.Sprintf("room %q does not exist", roomID.String()))
		}
		return nil, fmt.Errorf("eventutil.QueryAndBuildEvent: %w", err)
	}
	if event.Version() == gomatrixserverlib.RoomVersionPseudoIDs {
		return nil, rsAPI.ErrNotAllowed{Err: fmt.Errorf("knocking is not supported in room version %s", event.Version())}
	}

	inputRes := rsAPI.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, &rsAPI.InputRoomEventsRequest{
		InputRoomEvents: []rsAPI.InputRoomEvent{{
			Kind:         rsAPI.KindNew,
			Event:        event,
			Origin:       userID.Domain(),
			SendAsServer: string(userID.Domain()),
		}},
	}, &inputRes)
	if err = inputRes.Err(); err != nil {
		return nil, rsAPI.ErrNotAllowed{Err: errors.New(rsAPI.InputRejectionReason(err))}
	}

	// Input already recorded the knock membership; announce it to the sync
	// API with the room state that clients show for knocked rooms.
	state, err := gomatrixserverlib.GenerateStrippedState(ctx, roomID, &QueryState{r.DB, r.RSAPI})
	if err != nil {
		return nil, fmt.Errorf("GenerateStrippedState: %w", err)
	}
	return newKnockOutput(event.PDU, state)
}

func (r *Knocker) performFederatedKnock(
	ctx context.Context, roomID spec.RoomID, userID spec.UserID, content map[string]any, servers []spec.ServerName,
) ([]rsAPI.OutputEvent, error) {
	fedReq := fsAPI.PerformKnockRequest{
		RoomID:      roomID.String(),
		UserID:      userID.String(),
		ServerNames: servers,
		Content:     content,
	}
	fedRes := fsAPI.PerformKnockResponse{}
	if err := r.FSAPI.PerformKnock(ctx, &fedReq, &fedRes); err != nil {
		return nil, err
	}
	if fedRes.LastError != nil {
		return nil, fedRes.LastError
	}

	// Record the knock like a received invite: the room is unknown or
	// stale here, so the event is kept in the invites table rather than
	// the room's state.
	event := fedRes.Event
	updater, err := r.DB.MembershipUpdater(ctx, roomID.String(), string(event.SenderID()), true, event.Version())
	if err != nil {
		return nil, fmt.Errorf("r.DB.MembershipUpdater: %w", err)
	}
	knockOutputs, err := newKnockOutput(event, fedRes.KnockRoomState)
	if err != nil {
		_ = updater.Rollback()
		return nil, err
	}
	inserted, retired, err := updater.Update(tables.MembershipStateKnock, &types.Event{PDU: event}, false)
	if err != nil {
		_ = updater.Rollback()
		return nil, fmt.Errorf("updater.Update: %w", err)
	}
	if err = updater.Commit(); err != nil {
		return nil, fmt.Errorf("updater.Commit: %w", err)
	}
	// Retirements go first, as the sync API shows the latest entry per room.
	var outputs []rsAPI.OutputEvent
	for _, eventID := range retired {
		outputs = append(outputs, rsAPI.OutputEvent{
			Type: rsAPI.OutputTypeRetireInviteEvent,
			RetireInviteEvent: &rsAPI.OutputRetireInviteEvent{
				EventID:          eventID,
				RoomID:           roomID.String(),
				Membership:       spec.Knock,
				RetiredByEventID: event.EventID(),
				TargetSenderID:   event.SenderID(),
			},
		})
	}
	if inserted {
		outputs = append(outputs, knockOutputs...)
	}
	return outputs, nil
}

// newKnockOutput announces a knock, carrying the room state for clients in
// unsigned.knock_room_state. The state is marshaled in its stripped form.
func newKnockOutput(event gomatrixserverlib.PDU, state []gomatrixserverlib.InviteStrippedState) ([]rsAPI.OutputEvent, error) {
	if state == nil {
		state = []gomatrixserverlib.InviteStrippedState{}
	}
	if err := event.SetUnsignedField("knock_room_state", state); err != nil {
		return nil, fmt.Errorf("event.SetUnsignedField: %w", err)
	}
	return []rsAPI.OutputEvent{{
		Type: rsAPI.OutputTypeNewKnockEvent,
		NewKnockEvent: &rsAPI.OutputNewKnockEvent{
			Event:       &types.HeaderedEvent{PDU: event},
			RoomVersion: event.Version(),
		},
	}}, nil
}
