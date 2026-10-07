package types

import (
	"context"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type SyncRequest struct {
	Context       context.Context
	Log           *logrus.Entry
	Device        *userapi.Device
	Response      *Response
	Filter        synctypes.Filter
	Since         StreamingToken
	Timeout       time.Duration
	WantFullState bool
	// StateAfter is the room response key for the state at the end of the
	// timeline, StateAfterKey or UnstableStateAfterKey, when the client opted
	// in to MSC4222. It is empty for the state at the start of the timeline.
	StateAfter string

	// Updated by the PDU stream.
	Rooms map[string]string
	// Updated by the PDU stream.
	MembershipChanges map[string]struct{}
	// Updated by the PDU stream.
	IgnoredUsers IgnoredUsers
	// Rooms the user joined within this incremental sync. Updated by the PDU
	// stream, read by the sticky events stream.
	NewlyJoinedRooms map[string]struct{}
}

// UseStateAfter reports whether the client opted in to MSC4222, so rooms
// carry the state at the end of the timeline rather than at its start.
func (r *SyncRequest) UseStateAfter() bool {
	return r.StateAfter != ""
}

func (r *SyncRequest) IsRoomPresent(roomID string) bool {
	membership, ok := r.Rooms[roomID]
	if !ok {
		return false
	}
	switch membership {
	case spec.Join:
		return true
	case spec.Invite:
		return true
	case spec.Peek:
		return true
	default:
		return false
	}
}
