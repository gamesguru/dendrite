package auth

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/test"
)

type FakeQuerier struct {
	api.QuerySenderIDAPI
}

func (f *FakeQuerier) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func TestIsServerAllowed(t *testing.T) {
	alice := test.NewUser(t)

	tests := []struct {
		name                  string
		want                  bool
		roomFunc              func() *test.Room
		serverName            spec.ServerName
		serverCurrentlyInRoom bool
	}{
		{
			name:     "no servername specified",
			roomFunc: func() *test.Room { return test.NewRoom(t, alice) },
		},
		{
			name:       "no authEvents specified",
			serverName: "test",
			roomFunc:   func() *test.Room { return &test.Room{} },
		},
		{
			name:       "default denied",
			serverName: "test2",
			roomFunc:   func() *test.Room { return test.NewRoom(t, alice) },
		},
		{
			name:       "world readable room",
			serverName: "test",
			roomFunc: func() *test.Room {
				return test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityWorldReadable))
			},
			want: true,
		},
		{
			name:       "allowed due to alice being joined",
			serverName: "test",
			roomFunc:   func() *test.Room { return test.NewRoom(t, alice) },
			want:       true,
		},
		{
			name:                  "allowed due to 'serverCurrentlyInRoom'",
			serverName:            "test2",
			roomFunc:              func() *test.Room { return test.NewRoom(t, alice) },
			want:                  true,
			serverCurrentlyInRoom: true,
		},
		{
			name:       "allowed due to pending invite",
			serverName: "test2",
			roomFunc: func() *test.Room {
				bob := test.User{ID: "@bob:test2"}
				r := test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityInvited))
				r.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{
					"membership": spec.Invite,
				}, test.WithStateKey(bob.ID))
				return r
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.roomFunc == nil {
				t.Fatalf("missing roomFunc")
			}
			var authEvents []gomatrixserverlib.PDU
			for _, ev := range tt.roomFunc().Events() {
				authEvents = append(authEvents, ev.PDU)
			}

			if got := IsServerAllowed(context.Background(), &FakeQuerier{}, tt.serverName, tt.serverCurrentlyInRoom, authEvents); got != tt.want {
				t.Errorf("IsServerAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestServerEventAccess checks which allowed servers count as having had a
// member in the room, and so see events of erased users unredacted (MSC4025).
func TestServerEventAccess(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.User{ID: "@bob:test2"}

	tests := []struct {
		name                  string
		room                  func() *test.Room
		serverName            spec.ServerName
		serverCurrentlyInRoom bool
		wantAllowed           bool
		wantMember            bool
	}{
		{
			name:        "joined user",
			serverName:  "test",
			room:        func() *test.Room { return test.NewRoom(t, alice) },
			wantAllowed: true,
			wantMember:  true,
		},
		{
			name:       "joined user in world readable room",
			serverName: "test",
			room: func() *test.Room {
				return test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityWorldReadable))
			},
			wantAllowed: true,
			wantMember:  true,
		},
		{
			name:       "world readable without member",
			serverName: "test2",
			room: func() *test.Room {
				return test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityWorldReadable))
			},
			wantAllowed: true,
		},
		{
			name:                  "shared history after joining",
			serverName:            "test2",
			room:                  func() *test.Room { return test.NewRoom(t, alice) },
			serverCurrentlyInRoom: true,
			wantAllowed:           true,
		},
		{
			name:       "invited under invited visibility",
			serverName: "test2",
			room: func() *test.Room {
				r := test.NewRoom(t, alice, test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityInvited))
				r.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(bob.ID))
				return r
			},
			wantAllowed: true,
			wantMember:  true,
		},
		{
			name:       "denied",
			serverName: "test2",
			room:       func() *test.Room { return test.NewRoom(t, alice) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var authEvents []gomatrixserverlib.PDU
			for _, ev := range tt.room().Events() {
				authEvents = append(authEvents, ev.PDU)
			}
			allowed, member := ServerEventAccess(context.Background(), &FakeQuerier{}, tt.serverName, tt.serverCurrentlyInRoom, authEvents)
			if allowed != tt.wantAllowed || member != tt.wantMember {
				t.Errorf("ServerEventAccess() = (%v, %v), want (%v, %v)", allowed, member, tt.wantAllowed, tt.wantMember)
			}
			if got := IsServerAllowed(context.Background(), &FakeQuerier{}, tt.serverName, tt.serverCurrentlyInRoom, authEvents); got != tt.wantAllowed {
				t.Errorf("IsServerAllowed() = %v, want %v", got, tt.wantAllowed)
			}
		})
	}
}
