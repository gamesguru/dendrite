// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type Leaver struct {
	Cfg     *config.RoomServer
	DB      storage.Database
	FSAPI   fsAPI.RoomserverFederationAPI
	RSAPI   rsAPI.RoomserverInternalAPI
	UserAPI userapi.RoomserverUserAPI
	Inputer *input.Inputer
}

// WriteOutputEvents implements OutputRoomEventWriter.
func (r *Leaver) PerformLeave(
	ctx context.Context,
	req *rsAPI.PerformLeaveRequest,
	res *rsAPI.PerformLeaveResponse,
) ([]rsAPI.OutputEvent, error) {
	if !r.Cfg.Matrix.IsLocalServerName(req.Leaver.Domain()) {
		return nil, fmt.Errorf("user %q does not belong to this homeserver", req.Leaver.String())
	}
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"room_id": req.RoomID,
		"user_id": req.Leaver.String(),
	})
	logger.Info("User requested to leave room")
	if strings.HasPrefix(req.RoomID, "!") {
		output, err := r.performLeaveRoomByID(context.Background(), req, res) //nolint:contextcheck
		if err != nil {
			logger.WithError(err).Error("Failed to leave room")
		} else {
			logger.Info("User left room successfully")
		}
		return output, err
	}
	return nil, spec.InvalidParam(fmt.Sprintf("room ID %q is invalid", req.RoomID))
}

//nolint:gocyclo
func (r *Leaver) performLeaveRoomByID(
	ctx context.Context,
	req *rsAPI.PerformLeaveRequest,
	res *rsAPI.PerformLeaveResponse,
) ([]rsAPI.OutputEvent, error) {
	roomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return nil, err
	}
	leaver, err := r.RSAPI.QuerySenderIDForUser(ctx, *roomID, req.Leaver)
	if err != nil || leaver == nil {
		return nil, fmt.Errorf("leaver %s has no matching senderID in this room", req.Leaver.String())
	}

	// If there's an invite outstanding for the room then respond to
	// that.
	isInvitePending, senderUser, eventID, _, err := helpers.IsInvitePending(ctx, r.DB, req.RoomID, *leaver)
	if err == nil && isInvitePending {
		sender, serr := r.RSAPI.QueryUserIDForSender(ctx, *roomID, senderUser)
		if serr != nil {
			return nil, fmt.Errorf("failed looking up userID for sender %q: %w", senderUser, serr)
		}

		var domain spec.ServerName
		if sender == nil {
			// TODO: Currently a federated invite has no way of knowing the mxid_mapping of the inviter.
			// Should we add the inviter's m.room.member event (with mxid_mapping) to invite_room_state to allow
			// the invited user to leave via the inviter's server?
			domain = roomID.Domain()
		} else {
			domain = sender.Domain()
		}
		if !r.Cfg.Matrix.IsLocalServerName(domain) {
			return r.performFederatedLeaveFromPending(ctx, req, res, []spec.ServerName{domain}, eventID, *leaver)
		}
		// check that this is not a "server notice room"
		accData := &userapi.QueryAccountDataResponse{}
		if err = r.UserAPI.QueryAccountData(ctx, &userapi.QueryAccountDataRequest{
			UserID:   req.Leaver.String(),
			RoomID:   req.RoomID,
			DataType: "m.tag",
		}, accData); err != nil {
			return nil, fmt.Errorf("unable to query account data: %w", err)
		}

		if roomData, ok := accData.RoomAccountData[req.RoomID]; ok {
			tagData, ok := roomData["m.tag"]
			if ok {
				tags := gomatrix.TagContent{}
				if err = json.Unmarshal(tagData, &tags); err != nil {
					return nil, fmt.Errorf("unable to unmarshal tag content")
				}
				if _, ok = tags.Tags["m.server_notice"]; ok {
					// mimic the returned values from Synapse
					res.Message = "You cannot reject this invite"
					res.Code = 403
					return nil, spec.LeaveServerNoticeError()
				}
			}
		}
	}

	// A knock on a room this server is not in has to be rescinded through a
	// server which is. Knocks on rooms we are in take the local path below.
	isKnockPending, knockEventID, knockEvent, err := helpers.IsKnockPending(ctx, r.DB, req.RoomID, *leaver)
	if err == nil && isKnockPending {
		inRoomRes := rsAPI.QueryServerJoinedToRoomResponse{}
		if err = r.RSAPI.QueryServerJoinedToRoom(ctx, &rsAPI.QueryServerJoinedToRoomRequest{RoomID: req.RoomID}, &inRoomRes); err != nil {
			return nil, fmt.Errorf("QueryServerJoinedToRoom: %w", err)
		}
		if !inRoomRes.IsInRoom {
			return r.performFederatedLeaveFromPending(ctx, req, res, r.knockServers(*roomID, knockEvent), knockEventID, *leaver)
		}
	}

	// There's no invite pending, so first of all we want to find out
	// if the room exists and if the user is actually in it.
	latestReq := rsAPI.QueryLatestEventsAndStateRequest{
		RoomID: req.RoomID,
		StateToFetch: []gomatrixserverlib.StateKeyTuple{
			{
				EventType: spec.MRoomMember,
				StateKey:  string(*leaver),
			},
		},
	}
	latestRes := rsAPI.QueryLatestEventsAndStateResponse{}
	if err = helpers.QueryLatestEventsAndState(ctx, r.DB, r.RSAPI, &latestReq, &latestRes); err != nil {
		return nil, err
	}
	if !latestRes.RoomExists {
		return nil, spec.NotFound(fmt.Sprintf("room %q does not exist", req.RoomID))
	}

	// Now let's see if the user is in the room.
	if len(latestRes.StateEvents) == 0 {
		return nil, spec.Forbidden(fmt.Sprintf("user %q is not a member of room %q", req.Leaver.String(), req.RoomID))
	}
	membership, err := latestRes.StateEvents[0].Membership()
	if err != nil {
		return nil, fmt.Errorf("error getting membership: %w", err)
	}
	if membership != spec.Join && membership != spec.Invite && membership != spec.Knock {
		return nil, spec.Forbidden(fmt.Sprintf("user %q is not joined to the room (membership is %q)", req.Leaver.String(), membership))
	}

	// Prepare the template for the leave event.
	senderIDString := string(*leaver)
	proto := gomatrixserverlib.ProtoEvent{
		Type:     spec.MRoomMember,
		SenderID: senderIDString,
		StateKey: &senderIDString,
		RoomID:   req.RoomID,
		Redacts:  "",
	}
	if err = proto.SetContent(map[string]any{"membership": "leave"}); err != nil {
		return nil, fmt.Errorf("eb.SetContent: %w", err)
	}
	if err = proto.SetUnsigned(struct{}{}); err != nil {
		return nil, fmt.Errorf("eb.SetUnsigned: %w", err)
	}

	// We know that the user is in the room at this point so let's build
	// a leave event.
	// TODO: Check what happens if the room exists on the server
	// but everyone has since left. I suspect it does the wrong thing.

	validRoomID, err := spec.NewRoomID(req.RoomID)
	if err != nil {
		return nil, err
	}

	var buildRes rsAPI.QueryLatestEventsAndStateResponse
	identity, err := r.RSAPI.SigningIdentityFor(ctx, *validRoomID, req.Leaver)
	if err != nil {
		return nil, fmt.Errorf("SigningIdentityFor: %w", err)
	}
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, time.Now(), r.RSAPI, &buildRes)
	if err != nil {
		return nil, fmt.Errorf("eventutil.QueryAndBuildEvent: %w", err)
	}

	// Give our leave event to the roomserver input stream. The
	// roomserver will process the membership change and notify
	// downstream automatically.
	// We set SkipMissingEvents to true because we don't want to block
	// the leave request waiting for federation to fetch missing events.
	// The user wants to leave now, not after we've caught up with history.
	inputReq := rsAPI.InputRoomEventsRequest{
		InputRoomEvents: []rsAPI.InputRoomEvent{
			{
				Kind:              rsAPI.KindNew,
				Event:             event,
				Origin:            req.Leaver.Domain(),
				SendAsServer:      string(req.Leaver.Domain()),
				SkipMissingEvents: true,
			},
		},
	}
	inputRes := rsAPI.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, &inputReq, &inputRes)
	if err = inputRes.Err(); err != nil {
		return nil, fmt.Errorf("r.InputRoomEvents: %w", err)
	}

	return nil, nil
}

// RetractPendingMembership retires a local user's invite or knock for a room
// this server is not in, when the inviter rescinds the invite or a server in
// the room rejects the knock. It returns the output events and whether it
// applied.
func (r *Leaver) RetractPendingMembership(
	ctx context.Context, event *types.HeaderedEvent,
) ([]rsAPI.OutputEvent, bool, error) {
	membership, err := event.Membership()
	if err != nil || event.Type() != spec.MRoomMember || event.StateKey() == nil ||
		(membership != spec.Leave && membership != spec.Ban) {
		return nil, false, nil
	}
	roomID := event.RoomID()
	target, err := r.RSAPI.QueryUserIDForSender(ctx, roomID, spec.SenderID(*event.StateKey()))
	if err != nil || target == nil || !r.Cfg.Matrix.IsLocalServerName(target.Domain()) {
		return nil, false, nil
	}
	sender, err := r.RSAPI.QueryUserIDForSender(ctx, roomID, event.SenderID())
	if err != nil || sender == nil {
		return nil, false, nil
	}
	inRoomRes := rsAPI.QueryServerJoinedToRoomResponse{}
	if err = r.RSAPI.QueryServerJoinedToRoom(ctx, &rsAPI.QueryServerJoinedToRoomRequest{RoomID: roomID.String()}, &inRoomRes); err != nil {
		return nil, false, fmt.Errorf("QueryServerJoinedToRoom: %w", err)
	}
	if inRoomRes.IsInRoom {
		// The event can be authorized against the room state as usual.
		return nil, false, nil
	}

	targetID := spec.SenderID(*event.StateKey())
	var pendingEventID string
	var trusted []spec.ServerName
	if invited, inviter, eventID, _, inviteErr := helpers.IsInvitePending(ctx, r.DB, roomID.String(), targetID); inviteErr == nil && invited {
		// Only the inviter can rescind an invite, as in Synapse.
		if event.SenderID() != inviter {
			return nil, false, nil
		}
		pendingEventID = eventID
		trusted = []spec.ServerName{sender.Domain()}
	} else if knocked, eventID, knock, knockErr := helpers.IsKnockPending(ctx, r.DB, roomID.String(), targetID); knockErr == nil && knocked {
		pendingEventID = eventID
		trusted = r.knockServers(roomID, knock)
	}
	if pendingEventID == "" || !slices.Contains(trusted, sender.Domain()) {
		return nil, false, nil
	}

	info, err := r.DB.RoomInfo(ctx, roomID.String())
	if err != nil || info == nil {
		return nil, false, fmt.Errorf("failed to get RoomInfo: %w", err)
	}
	updater, err := r.DB.MembershipUpdater(ctx, roomID.String(), string(targetID), true, info.RoomVersion)
	if err != nil {
		return nil, false, fmt.Errorf("r.DB.MembershipUpdater: %w", err)
	}
	if err = updater.Delete(); err != nil {
		_ = updater.Rollback()
		return nil, false, fmt.Errorf("updater.Delete: %w", err)
	}
	if err = updater.Commit(); err != nil {
		return nil, false, fmt.Errorf("updater.Commit: %w", err)
	}
	return []rsAPI.OutputEvent{{
		Type: rsAPI.OutputTypeRetireInviteEvent,
		RetireInviteEvent: &rsAPI.OutputRetireInviteEvent{
			EventID:          pendingEventID,
			RoomID:           roomID.String(),
			Membership:       membership,
			RetiredByEventID: event.EventID(),
			TargetSenderID:   targetID,
		},
	}}, true, nil
}

// knockServers lists the servers to rescind a knock through: the room ID's
// domain, if it has one, and the senders of the room state returned with the
// knock, starting with the creator's.
func (r *Leaver) knockServers(roomID spec.RoomID, knockEvent gomatrixserverlib.PDU) []spec.ServerName {
	return r.pendingStateServers(roomID, knockEvent, "knock_room_state")
}

// pendingStateServers lists the servers known to be in a room this server is
// not in, from the room ID's domain and the senders of the stripped state
// stored under unsignedKey on an invite or knock, the creator's first.
func (r *Leaver) pendingStateServers(roomID spec.RoomID, event gomatrixserverlib.PDU, unsignedKey string) []spec.ServerName {
	var servers []spec.ServerName
	seen := map[spec.ServerName]bool{}
	add := func(server spec.ServerName) {
		if server == "" || seen[server] || r.Cfg.Matrix.IsLocalServerName(server) {
			return
		}
		seen[server] = true
		servers = append(servers, server)
	}
	add(roomIDDomain(roomID))
	state := gjson.GetBytes(event.Unsigned(), unsignedKey).Array()
	sort.SliceStable(state, func(i, j int) bool {
		return state[i].Get("type").Str == spec.MRoomCreate && state[j].Get("type").Str != spec.MRoomCreate
	})
	for _, entry := range state {
		if sender, err := spec.NewUserID(entry.Get("sender").Str, true); err == nil {
			add(sender.Domain())
		}
	}
	return servers
}

// performFederatedLeaveFromPending rejects an invite or rescinds a knock for a
// room this server is not in, then retires it locally even if no remote server
// could be reached.
func (r *Leaver) performFederatedLeaveFromPending(
	ctx context.Context,
	req *rsAPI.PerformLeaveRequest,
	res *rsAPI.PerformLeaveResponse, //nolint:unparam
	servers []spec.ServerName, eventID string,
	leaver spec.SenderID,
) ([]rsAPI.OutputEvent, error) {
	// Ask the federation sender to perform a federated leave for us.
	leaveReq := fsAPI.PerformLeaveRequest{
		RoomID:      req.RoomID,
		UserID:      req.Leaver.String(),
		ServerNames: servers,
	}
	leaveRes := fsAPI.PerformLeaveResponse{}
	if err := r.FSAPI.PerformLeave(ctx, &leaveReq, &leaveRes); err != nil {
		// failures in PerformLeave should NEVER stop us from telling other components like the
		// sync API that the invite or knock was withdrawn. Otherwise we can end up with stuck invites.
		util.GetLogger(ctx).WithError(err).Errorf("failed to PerformLeave, still retiring the invite or knock")
	}

	info, err := r.DB.RoomInfo(ctx, req.RoomID)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Errorf("failed to get RoomInfo, still retiring the invite or knock")
	}

	updater, err := r.DB.MembershipUpdater(ctx, req.RoomID, string(leaver), true, info.RoomVersion)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Errorf("failed to get MembershipUpdater, still retiring the invite or knock")
	}
	if updater != nil {
		if err = updater.Delete(); err != nil {
			util.GetLogger(ctx).WithError(err).Errorf("failed to delete membership, still retiring the invite or knock")
			if err = updater.Rollback(); err != nil {
				util.GetLogger(ctx).WithError(err).Errorf("failed to rollback deleting membership, still retiring the invite or knock")
			}
		} else {
			if err = updater.Commit(); err != nil {
				util.GetLogger(ctx).WithError(err).Errorf("failed to commit deleting membership, still retiring the invite or knock")
			}
		}
	}

	// Withdraw the invite or knock, so that the sync API etc are
	// notified that we left.
	return []rsAPI.OutputEvent{
		{
			Type: rsAPI.OutputTypeRetireInviteEvent,
			RetireInviteEvent: &rsAPI.OutputRetireInviteEvent{
				EventID:        eventID,
				RoomID:         req.RoomID,
				Membership:     "leave",
				TargetSenderID: leaver,
			},
		},
	}, nil
}
