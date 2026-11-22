// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// visibilityErrorSnapshot serves one joined shared-history room, but the
// roomserver cannot answer the history visibility membership lookup.
type visibilityErrorSnapshot struct {
	failoverSnapshot
}

func (s visibilityErrorSnapshot) GetStateDeltas(context.Context, *userapi.Device, types.Range, string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, []string, error) {
	return []types.StateDelta{{RoomID: s.roomID, Membership: spec.Join}}, nil, nil
}

func (s visibilityErrorSnapshot) SelectMembershipForUser(context.Context, string, string, int64) (string, int64, error) {
	return spec.Join, 0, nil
}

func (s visibilityErrorSnapshot) UnPartialStatedRoomsInRange(context.Context, string, types.Range) ([]string, error) {
	return nil, nil
}

type visibilityErrorAPI struct {
	roomserverAPI.SyncRoomserverAPI
}

func (visibilityErrorAPI) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderIDFromUserID(userID)
	return &senderID, nil
}

func (visibilityErrorAPI) QueryMembershipAtEvent(context.Context, spec.RoomID, []string, spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	return nil, errors.New("membership lookup failed")
}

func (visibilityErrorAPI) GetPartialStateRoomIDs(context.Context) ([]string, error) {
	return nil, nil
}

// A non-transient membership lookup failure must not advance the sync
// position past the room's timeline: the client would never be sent those
// events. The provider keeps the caller's original position instead.
func TestIncrementalSyncStaysBehindEventsWithoutVisibility(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "shared history"})
	message.Visibility = gomatrixserverlib.HistoryVisibilityShared

	snapshot := visibilityErrorSnapshot{failoverSnapshot{roomID: room.ID, events: []*rstypes.HeaderedEvent{message}}}
	provider := &PDUStreamProvider{rsAPI: visibilityErrorAPI{}}
	req := &types.SyncRequest{
		Context:           t.Context(),
		Device:            &userapi.Device{UserID: alice.ID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            synctypes.DefaultFilter(),
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
	}

	from, to := types.StreamPosition(10), types.StreamPosition(20)
	require.Equal(t, from, provider.IncrementalSync(t.Context(), snapshot, req, from, to))
}
