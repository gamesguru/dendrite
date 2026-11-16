// Copyright 2026 The Dendrite contributors.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/test"
)

type fakeStateAfterEvents struct {
	api.SyncRoomserverAPI
	prev  *rstypes.HeaderedEvent
	calls int
	last  *api.QueryStateAfterEventsRequest
}

func (f *fakeStateAfterEvents) QueryStateAfterEvents(
	_ context.Context, req *api.QueryStateAfterEventsRequest, res *api.QueryStateAfterEventsResponse,
) error {
	f.calls++
	f.last = req
	res.RoomExists = true
	res.PrevEventsExist = true
	res.StateEvents = []*rstypes.HeaderedEvent{f.prev}
	return nil
}

// A backfilled state event must be annotated from the state before it, not the
// sync API's current state: that state may already hold the event itself or a
// later change for the same state key.
func TestSetHistoricalPrevStateFromStateBeforeEvent(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice)

	prev := room.CreateEvent(t, bob, "m.room.member", map[string]any{
		"membership":  "join",
		"displayname": "old name",
	}, test.WithStateKey(bob.ID))
	current := room.CreateEvent(t, bob, "m.room.member", map[string]any{
		"membership":  "join",
		"displayname": "new name",
	}, test.WithStateKey(bob.ID))
	require.NoError(t, current.SetUnsignedField("age", 42))

	fake := &fakeStateAfterEvents{prev: prev}
	internal.SetHistoricalPrevState(context.Background(), fake, current)

	unsigned := current.Unsigned()
	require.Equal(t, "old name", gjson.GetBytes(unsigned, "prev_content.displayname").String())
	require.Equal(t, bob.ID, gjson.GetBytes(unsigned, "prev_sender").String())
	require.Equal(t, prev.EventID(), gjson.GetBytes(unsigned, "replaces_state").String())
	require.Equal(t, int64(42), gjson.GetBytes(unsigned, "age").Int(),
		"other unsigned fields must be left alone")

	require.Equal(t, current.PrevEventIDs(), fake.last.PrevEventIDs)
	require.Equal(t, []gomatrixserverlib.StateKeyTuple{{EventType: "m.room.member", StateKey: bob.ID}}, fake.last.StateToFetch)

	// Idempotent: an event that already carries prev_content is not looked up
	// or rewritten again.
	internal.SetHistoricalPrevState(context.Background(), fake, current)
	require.Equal(t, 1, fake.calls)
}

// A redacted state event has nothing left to describe, so it is left alone.
func TestSetHistoricalPrevStateSkipsRedactedEvents(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	ev := room.CreateEvent(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID))
	require.NoError(t, ev.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))

	fake := &fakeStateAfterEvents{prev: ev}
	internal.SetHistoricalPrevState(context.Background(), fake, ev)
	require.Zero(t, fake.calls)
}

// A PDU with the redacted flag set but no unsigned.redacted_because marker is
// still redacted, so it is left alone.
func TestSetHistoricalPrevStateSkipsRedactedFlagWithoutMarker(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	ev := room.CreateEvent(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID))
	redacted, err := rstypes.RedactedCopy(ev)
	require.NoError(t, err)
	require.True(t, redacted.Redacted())
	require.False(t, gjson.GetBytes(redacted.Unsigned(), "redacted_because").Exists())
	headered := &rstypes.HeaderedEvent{PDU: redacted}

	fake := &fakeStateAfterEvents{prev: ev}
	internal.SetHistoricalPrevState(context.Background(), fake, headered)
	require.Zero(t, fake.calls)
}
