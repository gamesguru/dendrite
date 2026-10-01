package routing

import (
	"context"
	"net/http"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

type threadRootRoomserver struct {
	rsapi.ClientRoomserverAPI
	events map[string]*types.HeaderedEvent
}

func (r *threadRootRoomserver) QueryEventsByID(_ context.Context, req *rsapi.QueryEventsByIDRequest, res *rsapi.QueryEventsByIDResponse) error {
	for _, id := range req.EventIDs {
		if event, ok := r.events[id]; ok {
			res.Events = append(res.Events, event)
		}
	}
	return nil
}

func TestValidateThreadRelation(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	plain := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "root"})
	reply := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "reply", "m.relates_to": map[string]any{"m.in_reply_to": map[string]any{"event_id": plain.EventID()}}})
	inThread := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "in thread", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": plain.EventID()}})
	edit := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "* edit", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": plain.EventID()}})
	otherRoom := test.NewRoom(t, alice)
	otherRoot := otherRoom.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "root"})
	foreignEdit := otherRoom.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "* edit", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": otherRoot.EventID()}})
	rs := &threadRootRoomserver{events: map[string]*types.HeaderedEvent{}}
	for _, event := range []*types.HeaderedEvent{plain, reply, inThread, edit, foreignEdit} {
		rs.events[event.EventID()] = event
	}
	thread := func(rootID string) map[string]any {
		return map[string]any{"body": "x", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": rootID}}
	}
	for name, tc := range map[string]struct {
		content  map[string]any
		rejected bool
	}{
		"plain root":            {thread(plain.EventID()), false},
		"reply as root":         {thread(reply.EventID()), false},
		"unknown root":          {thread("$unknown"), false},
		"not a thread":          {map[string]any{"body": "x", "m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": edit.EventID()}}, false},
		"no relation":           {map[string]any{"body": "x"}, false},
		"other room's relation": {thread(foreignEdit.EventID()), false},
		"thread reply as root":  {thread(inThread.EventID()), true},
		"edit as root":          {thread(edit.EventID()), true},
	} {
		t.Run(name, func(t *testing.T) {
			res := validateThreadRelation(t.Context(), rs, room.ID, tc.content)
			if !tc.rejected {
				require.Nil(t, res)
				return
			}
			require.NotNil(t, res)
			require.Equal(t, http.StatusBadRequest, res.Code)
			matrixErr, ok := res.JSON.(spec.MatrixError)
			require.True(t, ok)
			require.Equal(t, spec.ErrorUnknown, matrixErr.ErrCode)
		})
	}
}
