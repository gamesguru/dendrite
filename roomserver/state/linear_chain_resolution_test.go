// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package state

import (
	"fmt"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// TestResolveLinearMembershipChain mirrors Complement's
// TestStateIdsFallbackFetchesFullAuthChain: our current state holds Bob's
// original join, while the incoming state holds E at the end of an
// A -> B -> C -> D -> E chain of profile changes built on that join.
func TestResolveLinearMembershipChain(t *testing.T) {
	for name, sameTimestamp := range map[string]bool{"distinct timestamps": false, "same timestamp": true} {
		t.Run(name, func(t *testing.T) {
			alice := test.NewUser(t)
			bob := test.NewUser(t)
			room := test.NewRoom(t, alice, test.RoomVersion(gomatrixserverlib.RoomVersionV10))
			ts := time.Now()
			bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join},
				test.WithStateKey(bob.ID), test.WithTimestamp(ts))
			ourState := room.CurrentState()
			ts = ts.Add(time.Second)
			var chain []gomatrixserverlib.PDU
			for _, name := range []string{"A", "B", "C", "D", "E"} {
				if !sameTimestamp {
					ts = ts.Add(time.Millisecond)
				}
				ev := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join, "displayname": name},
					test.WithStateKey(bob.ID), test.WithTimestamp(ts))
				chain = append(chain, ev.PDU)
				t.Logf("%s: %s", name, ev.EventID())
			}
			theirState := room.CurrentState()

			toPDUs := func(evs []*types.HeaderedEvent) []gomatrixserverlib.PDU {
				pdus := make([]gomatrixserverlib.PDU, 0, len(evs))
				for _, ev := range evs {
					pdus = append(pdus, ev.PDU)
				}
				return pdus
			}
			var authEvents []gomatrixserverlib.PDU
			for _, ev := range room.Events() {
				authEvents = append(authEvents, ev.PDU)
			}
			resolved, err := gomatrixserverlib.ResolveConflictsNew(
				gomatrixserverlib.RoomVersionV10,
				[][]gomatrixserverlib.PDU{toPDUs(ourState), toPDUs(theirState)},
				authEvents,
				func(_ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
					return spec.NewUserID(string(senderID), true)
				},
				func(string) bool { return false },
			)
			require.NoError(t, err)

			var got string
			for _, ev := range resolved {
				if ev.Type() == spec.MRoomMember && ev.StateKeyEquals(bob.ID) {
					got = ev.EventID()
				}
			}
			names := map[string]string{bobJoin.EventID(): "original join"}
			for i, ev := range chain {
				names[ev.EventID()] = fmt.Sprintf("%c", 'A'+i)
			}
			if !sameTimestamp {
				require.Equal(t, "E", names[got])
				return
			}
			// With A-E sharing a timestamp, mainline ordering falls back to event
			// ID, so whichever of A-E has the greatest ID is applied last and wins.
			last := chain[0]
			for _, ev := range chain[1:] {
				if ev.EventID() > last.EventID() {
					last = ev
				}
			}
			t.Logf("resolved bob membership to %s", names[got])
			require.Equal(t, names[last.EventID()], names[got])
		})
	}
}
