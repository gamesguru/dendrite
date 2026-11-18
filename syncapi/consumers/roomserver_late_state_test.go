package consumers

import (
	"testing"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/test"
)

func TestExcludeNewEventFromSync(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	create := room.Events()[0]
	msg := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi"})
	name := room.CreateAndInsert(t, alice, "m.room.name", map[string]any{"name": "n"}, test.WithStateKey(""))
	head := msg.EventID()

	cases := map[string]struct {
		out  api.OutputNewRoomEvent
		want bool
	}{
		"late create is history":            {api.OutputNewRoomEvent{Event: create, LatestEventIDs: []string{head}}, true},
		"state forward extremity is live":   {api.OutputNewRoomEvent{Event: name, LatestEventIDs: []string{name.EventID()}}, false},
		"forked state alongside another":    {api.OutputNewRoomEvent{Event: name, LatestEventIDs: []string{head, name.EventID()}}, false},
		"late timeline event is delivered":  {api.OutputNewRoomEvent{Event: msg, LatestEventIDs: []string{name.EventID()}}, false},
		"no extremity info stays delivered": {api.OutputNewRoomEvent{Event: create}, false},
	}
	for label, tc := range cases {
		if got := excludeNewEventFromSync(&tc.out); got != tc.want {
			t.Errorf("%s: got %v, want %v", label, got, tc.want)
		}
	}
}
