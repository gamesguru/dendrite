// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// TestApplyHistoryVisibilityFilterErasure checks MSC4025: a user who was
// joined when an erased user sent an event still sees it unredacted, while a
// user who sees it only through shared history gets a redacted copy. Events
// of users who are not erased stay untouched, and so does the original event.
func TestApplyHistoryVisibilityFilterErasure(t *testing.T) {
	ctx := context.Background()
	roomID := "!erasure:domain"
	erasedUser := spec.NewUserIDOrPanic("@erased:domain", false)
	otherSender := spec.NewUserIDOrPanic("@other:domain", false)
	viewer := spec.NewUserIDOrPanic("@viewer:domain", false)
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)

	newEvent := func(id, json string) *types.HeaderedEvent {
		t.Helper()
		pdu, err := verImpl.NewEventFromTrustedJSONWithEventID(id, []byte(json), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: pdu, Visibility: gomatrixserverlib.HistoryVisibilityShared}
	}
	message := func(id string, sender spec.UserID) *types.HeaderedEvent {
		return newEvent(id, fmt.Sprintf(`{"type":"m.room.message","room_id":"%s","sender":"%s","content":{"body":"secret %s"}}`, roomID, sender.String(), id))
	}
	viewerJoin := newEvent("$viewer-join", fmt.Sprintf(`{"type":"m.room.member","state_key":"%s","room_id":"%s","sender":"%s","content":{"membership":"join"}}`, viewer.String(), roomID, viewer.String()))

	beforeJoin := message("$before-join", erasedUser)
	afterJoin := message("$after-join", erasedUser)
	otherBeforeJoin := message("$other-before-join", otherSender)

	rsAPI := &mockHisVisRoomserverAPI{
		roomID:       roomID,
		membershipAt: map[string]*types.HeaderedEvent{afterJoin.EventID(): viewerJoin},
		erased:       map[string]struct{}{erasedUser.String(): {}},
	}
	syncDB := &mockDB{roomID: roomID, currentMembership: map[string]string{viewer.String(): spec.Join}}

	filtered, err := ApplyHistoryVisibilityFilter(ctx, syncDB, rsAPI, []*types.HeaderedEvent{beforeJoin, otherBeforeJoin, afterJoin}, nil, viewer, "erasureTest")
	require.NoError(t, err)
	require.Len(t, filtered, 3)

	assert.Equal(t, beforeJoin.EventID(), filtered[0].EventID())
	assert.True(t, filtered[0].Redacted(), "event sent before the viewer joined must be redacted")
	assert.JSONEq(t, `{}`, string(filtered[0].Content()))
	assert.Equal(t, gomatrixserverlib.HistoryVisibilityShared, filtered[0].Visibility)

	assert.False(t, filtered[1].Redacted(), "events of users who are not erased must stay intact")
	assert.False(t, filtered[2].Redacted(), "the viewer was joined when this event was sent")

	assert.False(t, beforeJoin.Redacted(), "the original event, which may be cached, must not change")
	assert.Contains(t, string(beforeJoin.Content()), "secret")
}
