// Copyright 2025 Shane Jaroch
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergePartialStateAuth(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	member := func(sender, target, membership string, ts int) gomatrixserverlib.PDU {
		raw := fmt.Sprintf(
			`{"type":"m.room.member","state_key":%q,"room_id":"!r:test","sender":%q,"content":{"membership":%q},"origin_server_ts":%d,"depth":%d,"prev_events":[],"auth_events":[]}`,
			target, sender, membership, ts, ts,
		)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return ev
	}
	byKey := func(evs []gomatrixserverlib.PDU) map[string]string {
		out := map[string]string{}
		for _, ev := range evs {
			out[*ev.StateKey()] = ev.EventID()
		}
		return out
	}

	ban := member("@fred:test", "@derek:test", "ban", 3)
	derekJoin := member("@derek:test", "@derek:test", "join", 1)
	elsieJoin := member("@elsie:test", "@elsie:test", "join", 2)
	fredJoin := member("@fred:test", "@fred:test", "join", 1)

	t.Run("the state before the event wins over the membership the event cites", func(t *testing.T) {
		// derek was banned and cites his earlier join: he is still banned.
		got := byKey(mergePartialStateAuth(
			[]gomatrixserverlib.PDU{ban, fredJoin},
			[]gomatrixserverlib.PDU{derekJoin, elsieJoin},
		))
		assert.Equal(t, ban.EventID(), got["@derek:test"])
	})

	t.Run("cited auth events fill in what the partial state lacks", func(t *testing.T) {
		got := byKey(mergePartialStateAuth(
			[]gomatrixserverlib.PDU{ban},
			[]gomatrixserverlib.PDU{derekJoin, elsieJoin},
		))
		assert.Equal(t, elsieJoin.EventID(), got["@elsie:test"], "a membership the state omits is taken from the auth events")
		assert.Equal(t, ban.EventID(), got["@derek:test"])
	})

	t.Run("an event without auth events keeps the state as it is", func(t *testing.T) {
		got := byKey(mergePartialStateAuth([]gomatrixserverlib.PDU{ban}, nil))
		assert.Equal(t, map[string]string{"@derek:test": ban.EventID()}, got)
	})
}
