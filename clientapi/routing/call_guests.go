// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"crypto/rand"
	"math/big"
	"net/http"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// CallGuestLocalpartPrefix starts the localpart of every call guest, which is
// generated rather than chosen, so that call guests are recognizable. No other
// account may take a localpart with this prefix.
const CallGuestLocalpartPrefix = "call-guest-"

const (
	// The number of random characters that follow the prefix: 36^12, about
	// 4.7e18 localparts, make a collision negligible.
	callGuestLocalpartRandomLength = 12
	callGuestLocalpartAlphabet     = "abcdefghijklmnopqrstuvwxyz0123456789"
	// The number of localparts to try before giving up on collisions.
	callGuestLocalpartAttempts = 5
)

// newCallGuestLocalpart returns the prefix followed by random lowercase
// alphanumerics from crypto/rand.
func newCallGuestLocalpart() (string, error) {
	var b strings.Builder
	b.WriteString(CallGuestLocalpartPrefix)
	limit := big.NewInt(int64(len(callGuestLocalpartAlphabet)))
	for range callGuestLocalpartRandomLength {
		n, err := rand.Int(rand.Reader, limit)
		if err != nil {
			return "", err
		}
		b.WriteByte(callGuestLocalpartAlphabet[n.Int64()])
	}
	return b.String(), nil
}

// callGuestLocalpartReserved refuses a localpart with the call guest prefix
// for any account other than a call guest, so that no member can look like a
// call guest and no call guest like a member.
func callGuestLocalpartReserved(localpart string) *util.JSONResponse {
	if strings.HasPrefix(localpart, CallGuestLocalpartPrefix) {
		return &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidUsername("Usernames starting with " + CallGuestLocalpartPrefix + " are reserved for call guests"),
		}
	}
	return nil
}

// callGuestForbiddenEventTypes are the event types a call guest may not send,
// because they change who may join a room, who may do what in it, or what
// the room is. Element Call rooms set state_default to 0, so power levels
// alone would let a guest send any of them.
var callGuestForbiddenEventTypes = map[string]bool{
	spec.MRoomCreate:            true,
	spec.MRoomPowerLevels:       true,
	spec.MRoomJoinRules:         true,
	spec.MRoomHistoryVisibility: true,
	spec.MRoomGuestAccess:       true,
	spec.MRoomEncryption:        true,
	spec.MRoomCanonicalAlias:    true,
	spec.MRoomAliases:           true,
	spec.MRoomThirdPartyInvite:  true,
	spec.MSpaceChild:            true,
	spec.MSpaceParent:           true,
	"m.room.server_acl":         true,
	"m.room.tombstone":          true,
}

// callGuestEventForbidden refuses an event a call guest may not send: a
// structural event, or a membership event about another user, which would
// invite, kick or ban them. Other events, such as MatrixRTC call memberships,
// are left to the room's power levels. It returns nil for other accounts.
func callGuestEventForbidden(device *userapi.Device, eventType string, stateKey *string) *util.JSONResponse {
	if device == nil || device.AccountType != userapi.AccountTypeCallGuest {
		return nil
	}
	if callGuestForbiddenEventTypes[eventType] {
		return &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Call guest accounts may not send " + eventType + " events"),
		}
	}
	if eventType == spec.MRoomMember && (stateKey == nil || *stateKey != device.UserID) {
		return &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Call guest accounts may only change their own membership"),
		}
	}
	return nil
}
