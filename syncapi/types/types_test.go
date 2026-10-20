package types

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
)

type FakeRoomserverAPI struct{}

func (f *FakeRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	if senderID == "" {
		return nil, nil
	}

	return spec.NewUserID(string(senderID), true)
}

func (f *FakeRoomserverAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	sender := spec.SenderID(userID.String())
	return &sender, nil
}

func TestSyncTokens(t *testing.T) {
	t.Parallel()
	shouldPass := map[string]string{
		"s4_0_0_0_0_0_0_0_3_0": StreamingToken{4, 0, 0, 0, 0, 0, 0, 0, 3, 0}.String(),
		"s3_1_0_0_0_0_2_0_5_0": StreamingToken{3, 1, 0, 0, 0, 0, 2, 0, 5, 0}.String(),
		"s3_1_2_3_5_0_0_0_6_7": StreamingToken{3, 1, 2, 3, 5, 0, 0, 0, 6, 7}.String(),
		"t3_1":                 TopologyToken{3, 1}.String(),
		"t9223372036854775807_9223372036854775807": TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}.String(),
		"s9223372036854775807_1_2_3_5_0_0_0_6_0":   StreamingToken{math.MaxInt64, 1, 2, 3, 5, 0, 0, 0, 6, 0}.String(),
	}

	for a, b := range shouldPass {
		if a != b {
			t.Errorf("expected %q, got %q", a, b)
		}

		// parse as topology token
		if a[0] == 't' {
			if _, err := NewTopologyTokenFromString(a); err != nil {
				t.Errorf("expected %q to pass, but got %q", a, err)
			}
		}

		// parse as sync token
		if a[0] == 's' {
			if _, err := NewStreamTokenFromString(a); err != nil {
				t.Errorf("expected %q to pass, but got %q", a, err)
			}
		}
	}

	shouldFail := []string{
		"",
		"s_",
		"a3_4",
		"b",
		"b-1",
		"-4",
		"2",
	}

	for _, f := range append(shouldFail, "t1_2") {
		if _, err := NewStreamTokenFromString(f); err == nil {
			t.Errorf("NewStreamTokenFromString %q should have failed", f)
		}
	}

	for _, f := range append(shouldFail, "s1_2_3_4") {
		if _, err := NewTopologyTokenFromString(f); err == nil {
			t.Errorf("NewTopologyTokenFromString %q should have failed", f)
		}
	}
}

// Tokens issued before the MSC4354 sticky events stream existed have nine
// positions. They must keep parsing, with the sticky position at zero, and
// the sticky position must take part in comparisons and updates.
func TestSyncTokenStickyPosition(t *testing.T) {
	t.Parallel()
	old, err := NewStreamTokenFromString("s3_1_2_3_5_0_0_0_6")
	if err != nil {
		t.Fatal(err)
	}
	if want := (StreamingToken{3, 1, 2, 3, 5, 0, 0, 0, 6, 0}); old != want {
		t.Fatalf("old token parsed as %+v, want %+v", old, want)
	}
	if got := old.String(); got != "s3_1_2_3_5_0_0_0_6_0" {
		t.Fatalf("old token re-encoded as %q", got)
	}

	sticky, err := NewStreamTokenFromString("s3_1_2_3_5_0_0_0_6_9")
	if err != nil {
		t.Fatal(err)
	}
	if sticky.StickyPosition != 9 {
		t.Fatalf("sticky position parsed as %d, want 9", sticky.StickyPosition)
	}
	if !sticky.IsAfter(old) || old.IsAfter(sticky) {
		t.Fatal("a token ahead only in the sticky stream must compare as after")
	}
	if (&StreamingToken{StickyPosition: 1}).IsEmpty() {
		t.Fatal("a token with only a sticky position is not empty")
	}
	old.ApplyUpdates(StreamingToken{StickyPosition: 4})
	if old.StickyPosition != 4 {
		t.Fatalf("ApplyUpdates left the sticky position at %d", old.StickyPosition)
	}
}

func TestNewInviteResponse(t *testing.T) {
	t.Parallel()
	event := `{"auth_events":["$SbSsh09j26UAXnjd3RZqf2lyA3Kw2sY_VZJVZQAV9yA","$EwL53onrLwQ5gL8Dv3VrOOCvHiueXu2ovLdzqkNi3lo","$l2wGmz9iAwevBDGpHT_xXLUA5O8BhORxWIGU1cGi1ZM","$GsWFJLXgdlF5HpZeyWkP72tzXYWW3uQ9X28HBuTztHE"],"content":{"avatar_url":"","displayname":"neilalexander","membership":"invite"},"depth":9,"hashes":{"sha256":"8p+Ur4f8vLFX6mkIXhxI0kegPG7X3tWy56QmvBkExAg"},"origin":"matrix.org","origin_server_ts":1602087113066,"prev_events":["$1v-O6tNwhOZcA8bvCYY-Dnj1V2ZDE58lLPxtlV97S28"],"prev_state":[],"room_id":"!XbeXirGWSPXbEaGokF:matrix.org","sender":"@neilalexander:matrix.org","signatures":{"zendrite.neilalexander.dev":{"ed25519:BMJi":"05KQ5lPw0cSFsE4A0x1z7vi/3cc8bG4WHUsFWYkhxvk/XkXMGIYAYkpNThIvSeLfdcHlbm/k10AsBSKH8Uq4DA"},"matrix.org":{"ed25519:a_RXGa":"jeovuHr9E/x0sHbFkdfxDDYV/EyoeLi98douZYqZ02iYddtKhfB7R3WLay/a+D3V3V7IW0FUmPh/A404x5sYCw"}},"state_key":"@neilalexander:zendrite.neilalexander.dev","type":"m.room.member","unsigned":{"age":2512,"invite_room_state":[{"content":{"join_rule":"invite"},"sender":"@neilalexander:matrix.org","state_key":"","type":"m.room.join_rules"},{"content":{"avatar_url":"mxc://matrix.org/BpDaozLwgLnlNStxDxvLzhPr","displayname":"neilalexander","membership":"join"},"sender":"@neilalexander:matrix.org","state_key":"@neilalexander:matrix.org","type":"m.room.member"},{"content":{"name":"Test room"},"sender":"@neilalexander:matrix.org","state_key":"","type":"m.room.name"}]},"_room_version":"5"}`
	expected := `{"invite_state":{"events":[{"content":{"join_rule":"invite"},"sender":"@neilalexander:matrix.org","state_key":"","type":"m.room.join_rules"},{"content":{"avatar_url":"mxc://matrix.org/BpDaozLwgLnlNStxDxvLzhPr","displayname":"neilalexander","membership":"join"},"sender":"@neilalexander:matrix.org","state_key":"@neilalexander:matrix.org","type":"m.room.member"},{"content":{"name":"Test room"},"sender":"@neilalexander:matrix.org","state_key":"","type":"m.room.name"},{"content":{"avatar_url":"","displayname":"neilalexander","membership":"invite"},"event_id":"$GQmw8e8-26CQv1QuFoHBHpKF1hQj61Flg3kvv_v_XWs","origin_server_ts":1602087113066,"sender":"@neilalexander:matrix.org","state_key":"@neilalexander:zendrite.neilalexander.dev","type":"m.room.member"}]}}`

	ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV5).NewEventFromTrustedJSON([]byte(event), false)
	if err != nil {
		t.Fatal(err)
	}

	rsAPI := FakeRoomserverAPI{}
	res, err := NewInviteResponse(context.Background(), &rsAPI, &types.HeaderedEvent{PDU: ev}, synctypes.FormatSync)
	if err != nil {
		t.Fatal(err)
	}
	j, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}

	// Unmarshal both and compare, ignoring the event_id field which is
	// derived from a content hash that varies with JSON encoder output.
	var gotResp, expectedResp InviteResponse
	if err := json.Unmarshal(j, &gotResp); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(expected), &expectedResp); err != nil {
		t.Fatal(err)
	}
	if len(gotResp.InviteState.Events) != len(expectedResp.InviteState.Events) {
		t.Fatalf("expected %d events, got %d", len(expectedResp.InviteState.Events), len(gotResp.InviteState.Events))
	}
	for i := range expectedResp.InviteState.Events {
		var gotEv, expectedEv map[string]any
		if err := json.Unmarshal(gotResp.InviteState.Events[i], &gotEv); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(expectedResp.InviteState.Events[i], &expectedEv); err != nil {
			t.Fatal(err)
		}
		// Remove event_id before comparison since it's derived from a content
		// hash that depends on JSON serialization order.
		delete(gotEv, "event_id")
		delete(expectedEv, "event_id")
		if !reflect.DeepEqual(gotEv, expectedEv) {
			t.Fatalf("event %d mismatch, \nexpected: %s \ngot: %s", i, string(expectedResp.InviteState.Events[i]), string(gotResp.InviteState.Events[i]))
		}
	}
}

// Invites stored before invite_room_state was always a list carry an empty
// object there. They must still reach the client.
func TestNewInviteResponseWithEmptyObjectState(t *testing.T) {
	t.Parallel()
	event := `{"auth_events":[],"content":{"membership":"invite","is_direct":true},"depth":9,"hashes":{"sha256":"8p+Ur4f8vLFX6mkIXhxI0kegPG7X3tWy56QmvBkExAg"},"origin":"matrix.org","origin_server_ts":1602087113066,"prev_events":[],"room_id":"!XbeXirGWSPXbEaGokF:matrix.org","sender":"@neilalexander:matrix.org","signatures":{},"state_key":"@alice:zendrite.test","type":"m.room.member","unsigned":{"invite_room_state":{}}}`
	ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV5).NewEventFromTrustedJSON([]byte(event), false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := NewInviteResponse(context.Background(), &FakeRoomserverAPI{}, &types.HeaderedEvent{PDU: ev}, synctypes.FormatSync)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.InviteState.Events) != 1 {
		t.Fatalf("expected only the invite event, got %d events", len(res.InviteState.Events))
	}
}

func TestJoinResponse_MarshalJSON(t *testing.T) {
	t.Parallel()
	type fields struct {
		Summary             *Summary
		State               *ClientEvents
		Timeline            *Timeline
		Ephemeral           *ClientEvents
		AccountData         *ClientEvents
		UnreadNotifications *UnreadNotifications
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name: "empty state is removed",
			fields: fields{
				State: &ClientEvents{},
			},
			want: []byte("{}"),
		},
		{
			name: "empty accountdata is removed",
			fields: fields{
				AccountData: &ClientEvents{},
			},
			want: []byte("{}"),
		},
		{
			name: "empty ephemeral is kept",
			fields: fields{
				Ephemeral: &ClientEvents{},
			},
			want: []byte(`{"ephemeral":{"events":[]}}`),
		},
		{
			name: "empty timeline is removed",
			fields: fields{
				Timeline: &Timeline{},
			},
			want: []byte("{}"),
		},
		{
			name: "empty summary is removed",
			fields: fields{
				Summary: &Summary{},
			},
			want: []byte("{}"),
		},
		{
			name: "unread notifications are removed, if everything else is empty",
			fields: fields{
				UnreadNotifications: &UnreadNotifications{},
			},
			want: []byte("{}"),
		},
		{
			name: "unread notifications are removed, if only ephemeral is empty",
			fields: fields{
				Ephemeral:           &ClientEvents{},
				UnreadNotifications: &UnreadNotifications{},
			},
			want: []byte(`{"ephemeral":{"events":[]}}`),
		},
		{
			name: "unread notifications are NOT removed, if state is set",
			fields: fields{
				State:               &ClientEvents{Events: []synctypes.ClientEvent{{Content: []byte("{}")}}},
				UnreadNotifications: &UnreadNotifications{NotificationCount: 1},
			},
			want: []byte(`{"state":{"events":[{"content":{},"type":""}]},"unread_notifications":{"highlight_count":0,"notification_count":1}}`),
		},
		{
			name: "roomID is removed from EDUs",
			fields: fields{
				Ephemeral: &ClientEvents{
					Events: []synctypes.ClientEvent{
						{RoomID: "!someRandomRoomID:test", Content: []byte("{}")},
					},
				},
			},
			want: []byte(`{"ephemeral":{"events":[{"content":{},"type":""}]}}`),
		},
	}
	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			jr := JoinResponse{
				Summary:             tt.fields.Summary,
				State:               tt.fields.State,
				Timeline:            tt.fields.Timeline,
				Ephemeral:           tt.fields.Ephemeral,
				AccountData:         tt.fields.AccountData,
				UnreadNotifications: tt.fields.UnreadNotifications,
			}
			got, err := jr.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MarshalJSON() got = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func TestResponseMoveStateToStateAfter(t *testing.T) {
	t.Parallel()
	state := []synctypes.ClientEvent{{Type: "m.room.topic", Content: []byte("{}")}}
	tests := []struct {
		key  string
		want string
	}{
		{key: StateAfterKey, want: `"state_after"`},
		{key: UnstableStateAfterKey, want: `"org.matrix.msc4222.state_after"`},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			res := NewResponse()
			res.Rooms.Join["!joined:test"] = NewJoinResponse()
			res.Rooms.Join["!joined:test"].State.Events = state
			// A room only another stream added still carries an empty state_after.
			res.Rooms.Join["!typing:test"] = NewJoinResponse()
			res.Rooms.Leave["!left:test"] = NewLeaveResponse()
			res.MoveStateToStateAfter(tt.key)

			for roomID, want := range map[string]string{
				"!joined:test": `{` + tt.want + `:{"events":[{"content":{},"type":"m.room.topic"}]},` +
					`"ephemeral":{"events":[]},"unread_notifications":{"highlight_count":0,"notification_count":0}}`,
				"!typing:test": `{` + tt.want + `:{"events":[]},"ephemeral":{"events":[]}}`,
			} {
				got, err := json.Marshal(res.Rooms.Join[roomID])
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != want {
					t.Errorf("%s: got %s want %s", roomID, got, want)
				}
			}
			got, err := json.Marshal(res.Rooms.Leave["!left:test"])
			if err != nil {
				t.Fatal(err)
			}
			if want := `{` + tt.want + `:{"events":[]}}`; string(got) != want {
				t.Errorf("left room: got %s want %s", got, want)
			}
		})
	}
}
