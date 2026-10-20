// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"regexp"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
)

// interestRoomserver resolves every sender to sender and reports joined as
// the room's joined members.
type interestRoomserver struct {
	api.AppserviceRoomserverAPI
	sender string
	joined []string
}

func (r *interestRoomserver) QueryUserIDForSender(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(r.sender, true)
}

func (r *interestRoomserver) GetAliasesForRoomID(context.Context, *api.GetAliasesForRoomIDRequest, *api.GetAliasesForRoomIDResponse) error {
	return nil
}

func (r *interestRoomserver) QueryMembershipsForRoom(_ context.Context, _ *api.QueryMembershipsForRoomRequest, res *api.QueryMembershipsForRoomResponse) error {
	for _, userID := range r.joined {
		stateKey := userID
		res.JoinEvents = append(res.JoinEvents, synctypes.ClientEvent{Type: spec.MRoomMember, StateKey: &stateKey, Content: []byte(`{"membership":"join"}`)})
	}
	return nil
}

// TestAppserviceInterestLocalUsersOnly checks that user namespaces only match
// local users (MSC3905).
func TestAppserviceInterestLocalUsersOnly(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	message := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "hi"})
	remoteMember := room.CreateEvent(t, alice, spec.MRoomMember, map[string]any{"membership": "invite"}, test.WithStateKey("@bridge_bob:remote.test"))
	localMember := room.CreateEvent(t, alice, spec.MRoomMember, map[string]any{"membership": "invite"}, test.WithStateKey("@bridge_bob:test"))

	appservice := &config.ApplicationService{
		ID:  "bridge",
		URL: "http://bridge.test",
		NamespaceMap: map[string][]config.ApplicationServiceNamespace{
			"users": {{RegexpObject: regexp.MustCompile(`@bridge_.*`)}},
		},
	}
	cfg := &config.AppServiceAPI{Matrix: &config.Global{ServerName: "test"}}
	cfg.Matrix.VirtualHosts = []*config.VirtualHost{{SigningIdentity: fclient.SigningIdentity{ServerName: "virtual.test"}}}

	for _, tc := range []struct {
		name   string
		rs     *interestRoomserver
		event  bool
		member bool
		want   bool
	}{
		{name: "local sender", rs: &interestRoomserver{sender: "@bridge_alice:test"}, want: true},
		{name: "virtual host sender", rs: &interestRoomserver{sender: "@bridge_alice:virtual.test"}, want: true},
		{name: "remote sender", rs: &interestRoomserver{sender: "@bridge_alice:remote.test"}, want: false},
		{name: "local member", rs: &interestRoomserver{sender: "@alice:test"}, member: true, want: true},
		{name: "remote member", rs: &interestRoomserver{sender: "@alice:test"}, event: true, want: false},
		{name: "local user joined", rs: &interestRoomserver{sender: "@alice:test", joined: []string{"@alice:test", "@bridge_carol:test"}}, want: true},
		{name: "remote user joined", rs: &interestRoomserver{sender: "@alice:test", joined: []string{"@alice:test", "@bridge_carol:remote.test"}}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &OutputRoomEventConsumer{cfg: cfg, rsAPI: tc.rs}
			event := message
			switch {
			case tc.event:
				event = remoteMember
			case tc.member:
				event = localMember
			}
			require.Equal(t, tc.want, s.appserviceIsInterestedInEvent(t.Context(), event, appservice))
		})
	}
}
