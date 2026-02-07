package tables

import (
	"encoding/json"
	"testing"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/dendrite/roomserver/types"
	"codefloe.com/pat-s/dendrite/test"
)

func TestExtractContentValue(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)

	tests := []struct {
		name      string
		event     *types.HeaderedEvent
		want      string
		wantJSON  bool // if true, want is a key in the JSON response
		wantValue string
	}{
		{
			name:      "returns full JSON for create events (for room_type extraction)",
			event:     room.Events()[0],
			wantJSON:  true,
			want:      "creator",
			wantValue: alice.ID,
		},
		{
			name:  "returns the alias for canonical alias events",
			event: room.CreateEvent(t, alice, spec.MRoomCanonicalAlias, map[string]string{"alias": "#test:test"}),
			want:  "#test:test",
		},
		{
			name:  "returns the history_visibility for history visibility events",
			event: room.CreateEvent(t, alice, spec.MRoomHistoryVisibility, map[string]string{"history_visibility": "shared"}),
			want:  "shared",
		},
		{
			name:  "returns the join rules for join_rules events",
			event: room.CreateEvent(t, alice, spec.MRoomJoinRules, map[string]string{"join_rule": "public"}),
			want:  "public",
		},
		{
			name:  "returns the membership for room_member events",
			event: room.CreateEvent(t, alice, spec.MRoomMember, map[string]string{"membership": "join"}, test.WithStateKey(alice.ID)),
			want:  "join",
		},
		{
			name:  "returns the room name for room_name events",
			event: room.CreateEvent(t, alice, spec.MRoomName, map[string]string{"name": "testing"}, test.WithStateKey(alice.ID)),
			want:  "testing",
		},
		{
			name:  "returns the room avatar for avatar events",
			event: room.CreateEvent(t, alice, spec.MRoomAvatar, map[string]string{"url": "mxc://testing"}, test.WithStateKey(alice.ID)),
			want:  "mxc://testing",
		},
		{
			name:  "returns the room topic for topic events",
			event: room.CreateEvent(t, alice, spec.MRoomTopic, map[string]string{"topic": "testing"}, test.WithStateKey(alice.ID)),
			want:  "testing",
		},
		{
			name:  "returns guest_access for guest access events",
			event: room.CreateEvent(t, alice, "m.room.guest_access", map[string]string{"guest_access": "forbidden"}, test.WithStateKey(alice.ID)),
			want:  "forbidden",
		},
		{
			name:  "returns empty string if key can't be found or unknown event",
			event: room.CreateEvent(t, alice, "idontexist", nil),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractContentValue(tt.event)
			if tt.wantJSON {
				// For JSON responses, verify the specific key exists with expected value
				var parsed map[string]any
				err := json.Unmarshal([]byte(result), &parsed)
				assert.NoError(t, err, "Expected valid JSON")
				assert.Equal(t, tt.wantValue, parsed[tt.want], "JSON key %q should have expected value", tt.want)
			} else {
				assert.Equalf(t, tt.want, result, "ExtractContentValue(%v)", tt.event)
			}
		})
	}
}
