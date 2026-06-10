// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"reflect"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
)

// fakePartialStateRoomserverAPI implements just enough of
// api.FederationRoomserverAPI to exercise partialStateServersForEvent.
type fakePartialStateRoomserverAPI struct {
	api.FederationRoomserverAPI
	roomNID         rstypes.RoomNID
	partialState    bool
	partialStateErr error
	servers         []string
	serversErr      error
}

func (f *fakePartialStateRoomserverAPI) QueryRoomInfo(ctx context.Context, roomID spec.RoomID) (*rstypes.RoomInfo, error) {
	return &rstypes.RoomInfo{RoomNID: f.roomNID}, nil
}

func (f *fakePartialStateRoomserverAPI) IsRoomPartialState(ctx context.Context, roomNID rstypes.RoomNID) (bool, error) {
	return f.partialState, f.partialStateErr
}

func (f *fakePartialStateRoomserverAPI) GetPartialStateServers(ctx context.Context, roomNID rstypes.RoomNID) ([]string, error) {
	return f.servers, f.serversErr
}

func TestPartialStateServersForEvent(t *testing.T) {
	roomID, err := spec.NewRoomID("!room:example.com")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("local event in partial state room targets reported servers", func(t *testing.T) {
		rsAPI := &fakePartialStateRoomserverAPI{
			roomNID:      1,
			partialState: true,
			servers:      []string{"a.example.com", "b.example.com", "c.example.com"},
		}
		s := &OutputRoomEventConsumer{ctx: context.Background(), rsAPI: rsAPI}

		servers, skip, err := s.partialStateServersForEvent(*roomID, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if skip {
			t.Fatal("local event should not be skipped during partial state")
		}
		want := []spec.ServerName{"a.example.com", "b.example.com", "c.example.com"}
		if !reflect.DeepEqual(servers, want) {
			t.Fatalf("got servers %v, want %v", servers, want)
		}
	})

	t.Run("remote event in partial state room is skipped", func(t *testing.T) {
		rsAPI := &fakePartialStateRoomserverAPI{
			roomNID:      1,
			partialState: true,
			servers:      []string{"a.example.com"},
		}
		s := &OutputRoomEventConsumer{ctx: context.Background(), rsAPI: rsAPI}

		servers, skip, err := s.partialStateServersForEvent(*roomID, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !skip {
			t.Fatal("remote event in partial state room should be skipped")
		}
		if servers != nil {
			t.Fatalf("expected no servers for a skipped event, got %v", servers)
		}
	})

	t.Run("fully joined room adds no servers and is not skipped", func(t *testing.T) {
		rsAPI := &fakePartialStateRoomserverAPI{
			roomNID:      1,
			partialState: false,
			servers:      []string{"a.example.com"},
		}
		s := &OutputRoomEventConsumer{ctx: context.Background(), rsAPI: rsAPI}

		servers, skip, err := s.partialStateServersForEvent(*roomID, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if skip {
			t.Fatal("fully joined room should not be skipped")
		}
		if len(servers) != 0 {
			t.Fatalf("expected no extra servers for a fully joined room, got %v", servers)
		}
	})
}
