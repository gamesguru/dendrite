// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package auth

import (
	"context"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
)

// TODO: This logic should live in gomatrixserverlib

// IsServerAllowed returns true if the server is allowed to see events in the room
// at this particular state. This function implements https://matrix.org/docs/spec/client_server/r0.6.0#id87
func IsServerAllowed(
	ctx context.Context, querier api.QuerySenderIDAPI,
	serverName spec.ServerName,
	serverCurrentlyInRoom bool,
	authEvents []gomatrixserverlib.PDU,
) bool {
	// Skip the membership scan when it cannot change the answer.
	if HistoryVisibilityForRoom(authEvents) == gomatrixserverlib.HistoryVisibilityWorldReadable {
		return true
	}
	allowed, _ := ServerEventAccess(ctx, querier, serverName, serverCurrentlyInRoom, authEvents)
	return allowed
}

// ServerEventAccess reports whether the server is allowed to see events in
// the room at this particular state, and whether that is because one of its
// users was joined, or invited under invited visibility. A server allowed only
// through shared or world readable visibility is served redacted copies of
// the events of erased users (MSC4025).
func ServerEventAccess(
	ctx context.Context, querier api.QuerySenderIDAPI,
	serverName spec.ServerName,
	serverCurrentlyInRoom bool,
	authEvents []gomatrixserverlib.PDU,
) (allowed, member bool) {
	historyVisibility := HistoryVisibilityForRoom(authEvents)

	// 2. If the user's membership was join, allow.
	joinedUserExists := IsAnyUserOnServerWithMembership(ctx, querier, serverName, authEvents, spec.Join)
	if joinedUserExists {
		return true, true
	}
	// 4. If the user's membership was invite, and the history_visibility was set to invited, allow.
	if historyVisibility == gomatrixserverlib.HistoryVisibilityInvited &&
		IsAnyUserOnServerWithMembership(ctx, querier, serverName, authEvents, spec.Invite) {
		return true, true
	}
	// 1. If the history_visibility was set to world_readable, allow.
	if historyVisibility == gomatrixserverlib.HistoryVisibilityWorldReadable {
		return true, false
	}
	// 3. If history_visibility was set to shared, and the user joined the room at any point after the event was sent, allow.
	if historyVisibility == gomatrixserverlib.HistoryVisibilityShared && serverCurrentlyInRoom {
		return true, false
	}

	// 5. Otherwise, deny.
	return false, false
}

func HistoryVisibilityForRoom(authEvents []gomatrixserverlib.PDU) gomatrixserverlib.HistoryVisibility {
	// https://matrix.org/docs/spec/client_server/r0.6.0#id87
	// By default if no history_visibility is set, or if the value is not understood, the visibility is assumed to be shared.
	visibility := gomatrixserverlib.HistoryVisibilityShared
	for _, ev := range authEvents {
		if ev.Type() != spec.MRoomHistoryVisibility {
			continue
		}
		if vis, err := ev.HistoryVisibility(); err == nil {
			visibility = vis
		}
	}
	return visibility
}

func IsAnyUserOnServerWithMembership(ctx context.Context, querier api.QuerySenderIDAPI, serverName spec.ServerName, authEvents []gomatrixserverlib.PDU, wantMembership string) bool {
	for _, ev := range authEvents {
		if ev.Type() != spec.MRoomMember {
			continue
		}
		membership, err := ev.Membership()
		if err != nil || membership != wantMembership {
			continue
		}

		stateKey := ev.StateKey()
		if stateKey == nil {
			continue
		}

		userID, err := querier.QueryUserIDForSender(ctx, ev.RoomID(), spec.SenderID(*stateKey))
		if err != nil {
			continue
		}

		if userID.Domain() == serverName {
			return true
		}
	}
	return false
}
