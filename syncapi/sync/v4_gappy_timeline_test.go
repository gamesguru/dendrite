// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// TestGetTimelineEvents_RoomReenteringAfterGap_DoesNotDropEvents guards
// against the regression from 015bf515: incremental fetches used the
// connection-level since token instead of the room's own last-sent token
// (see getTimelineEvents). Synapse never makes this substitution -
// HaveSentRoomFlag.PREVIOUSLY always uses room_status.last_token.
func TestGetTimelineEvents_RoomReenteringAfterGap_DoesNotDropEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!gappy:localhost"
	userID := "@alice:localhost"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	// The room was last actually delivered up to position 50 (its LastToken).
	// While it was excluded from responses, the connection's own since token
	// advanced to 90 because of unrelated activity. Two messages were sent
	// into the room during that gap (60, 75); one more arrived after the
	// connection's since token (95).
	snapshot := newMockSnapshot()
	snapshot.SetRecentEvents(roomID, types.RecentEvents{
		Events: []types.StreamEvent{
			createMockTimelineEvent("$gap1:localhost", 60, `{"msgtype":"m.text","body":"missed while asleep 1"}`),
			createMockTimelineEvent("$gap2:localhost", 75, `{"msgtype":"m.text","body":"missed while asleep 2"}`),
			createMockTimelineEvent("$live:localhost", 95, `{"msgtype":"m.text","body":"sent after reconnect"}`),
		},
	})

	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomPreviously,
		LastToken: &types.StreamingToken{PDUPosition: 50},
	}
	fromToken := &types.StreamingToken{PDUPosition: 90} // connection-level since, ahead of LastToken
	currentPos := types.StreamingToken{PDUPosition: 100}

	timeline, limited, numLive, err := rp.getTimelineEvents(ctx, snapshot, roomID, userID, 10, roomState, currentPos, fromToken)
	require.NoError(t, err)
	assert.False(t, limited)

	gotIDs := make([]string, len(timeline))
	for i, ev := range timeline {
		gotIDs[i] = ev.EventID
	}
	assert.Equal(t, []string{"$gap1:localhost", "$gap2:localhost", "$live:localhost"}, gotIDs,
		"messages sent while the room was excluded from responses must still be delivered once it re-enters sync")

	// Only the event after the connection's own since token counts as "live"
	// (drives notification/highlight handling); the two gap messages are still
	// delivered in the timeline but aren't counted as live, matching Synapse's
	// num_live semantics.
	assert.Equal(t, 1, numLive)
}

// TestGetTimelineEvents_RoomContinuouslyLive_OnlyReturnsNewEvents is the
// steady-state counterpart: when a room has no gap (its LastToken matches the
// connection's since token, because it was included in every response), only
// events strictly after that point should come back. This must keep passing
// unchanged by the fix above - it's what the original since-token-only code
// was trying to preserve.
func TestGetTimelineEvents_RoomContinuouslyLive_OnlyReturnsNewEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!steady:localhost"
	userID := "@alice:localhost"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	snapshot := newMockSnapshot()
	snapshot.SetRecentEvents(roomID, types.RecentEvents{
		Events: []types.StreamEvent{
			createMockTimelineEvent("$old:localhost", 40, `{"msgtype":"m.text","body":"already delivered previously"}`),
			createMockTimelineEvent("$new:localhost", 95, `{"msgtype":"m.text","body":"brand new"}`),
		},
	})

	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomLive,
		LastToken: &types.StreamingToken{PDUPosition: 90},
	}
	fromToken := &types.StreamingToken{PDUPosition: 90}
	currentPos := types.StreamingToken{PDUPosition: 100}

	timeline, _, numLive, err := rp.getTimelineEvents(ctx, snapshot, roomID, userID, 10, roomState, currentPos, fromToken)
	require.NoError(t, err)

	require.Len(t, timeline, 1)
	assert.Equal(t, "$new:localhost", timeline[0].EventID)
	assert.Equal(t, 1, numLive)
}
