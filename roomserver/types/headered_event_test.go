// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package types

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactedCopy(t *testing.T) {
	t.Parallel()

	for _, version := range []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV1, gomatrixserverlib.RoomVersionV10, gomatrixserverlib.RoomVersionV11} {
		t.Run(string(version), func(t *testing.T) {
			t.Parallel()
			verImpl := gomatrixserverlib.MustGetRoomVersion(version)
			eventJSON := `{"type":"m.room.message","room_id":"!room:test","sender":"@alice:test","origin_server_ts":1,"depth":3,"prev_events":[],"auth_events":[],"content":{"body":"secret"}}`
			if version == gomatrixserverlib.RoomVersionV1 {
				eventJSON = `{"event_id":"$event:test","type":"m.room.message","room_id":"!room:test","sender":"@alice:test","origin_server_ts":1,"depth":3,"prev_events":[],"auth_events":[],"content":{"body":"secret"}}`
			}
			original, err := verImpl.NewEventFromTrustedJSON([]byte(eventJSON), false)
			require.NoError(t, err)
			wrapped := &HeaderedEvent{PDU: &HeaderedEvent{PDU: original}}

			redacted, err := RedactedCopy(wrapped)
			require.NoError(t, err)
			assert.True(t, redacted.Redacted())
			assert.JSONEq(t, `{}`, string(redacted.Content()))
			assert.Equal(t, original.EventID(), redacted.EventID())
			assert.Equal(t, version, redacted.Version())

			assert.False(t, original.Redacted(), "the original event must not change")
			assert.JSONEq(t, `{"body":"secret"}`, string(original.Content()))

			again, err := RedactedCopy(redacted)
			require.NoError(t, err)
			assert.Same(t, redacted, again, "an already redacted event is returned as it is")
		})
	}
}
