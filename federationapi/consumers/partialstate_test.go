// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
)

// fakePartialStateRoomserverAPI implements just enough of
// api.FederationRoomserverAPI to exercise partialStateServersForEvent.
type fakePartialStateRoomserverAPI struct {
	api.FederationRoomserverAPI
	roomNID          rstypes.RoomNID
	partialState     bool
	partialStateErr  error
	allRoomsErr      error
	servers          []string
	serversErr       error
	roomInfoNil      bool
	queryRoomInfoErr error
}

func (f *fakePartialStateRoomserverAPI) QueryRoomInfo(ctx context.Context, roomID spec.RoomID) (*rstypes.RoomInfo, error) {
	if f.queryRoomInfoErr != nil {
		return nil, f.queryRoomInfoErr
	}
	if f.roomInfoNil {
		return nil, nil
	}
	return &rstypes.RoomInfo{RoomNID: f.roomNID}, nil
}

func (f *fakePartialStateRoomserverAPI) GetAllPartialStateRooms(ctx context.Context) ([]rstypes.RoomNID, error) {
	if f.allRoomsErr != nil {
		return nil, f.allRoomsErr
	}
	if f.partialState {
		return []rstypes.RoomNID{f.roomNID}, f.partialStateErr
	}
	return nil, f.partialStateErr
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

func TestRoomPurged(t *testing.T) {
	ctx := context.Background()

	t.Run("purged room reports true", func(t *testing.T) {
		s := &OutputRoomEventConsumer{rsAPI: &fakePartialStateRoomserverAPI{roomInfoNil: true}}
		if !s.roomPurged(ctx, "!room:example.com") {
			t.Fatal("expected a room with no roomInfo to be reported as purged")
		}
	})

	t.Run("existing room reports false", func(t *testing.T) {
		s := &OutputRoomEventConsumer{rsAPI: &fakePartialStateRoomserverAPI{roomNID: 1}}
		if s.roomPurged(ctx, "!room:example.com") {
			t.Fatal("expected an existing room not to be reported as purged")
		}
	})

	t.Run("query error reports false", func(t *testing.T) {
		s := &OutputRoomEventConsumer{rsAPI: &fakePartialStateRoomserverAPI{queryRoomInfoErr: errors.New("boom")}}
		if s.roomPurged(ctx, "!room:example.com") {
			t.Fatal("expected a query error to fall back to not purged")
		}
	})

	t.Run("invalid room id reports false", func(t *testing.T) {
		s := &OutputRoomEventConsumer{rsAPI: &fakePartialStateRoomserverAPI{roomInfoNil: true}}
		if s.roomPurged(ctx, "not-a-room-id") {
			t.Fatal("expected an invalid room id to fall back to not purged")
		}
	})
}

func TestKeyChangePartialStateServers(t *testing.T) {
	room, err := spec.NewRoomID("!room:example.com")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("a room with partial state adds the servers reported for it", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{
			roomNID: 1, partialState: true, servers: []string{"a.example.com", "b.example.com"},
		}}
		got, err := consumer.partialStateServers([]spec.RoomID{*room})
		if err != nil {
			t.Fatal(err)
		}
		if want := []spec.ServerName{"a.example.com", "b.example.com"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("a room with its full state adds nothing", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{
			roomNID: 1, servers: []string{"a.example.com"},
		}}
		got, err := consumer.partialStateServers([]spec.RoomID{*room})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v, want none", got, err)
		}
	})

	t.Run("an unknown room adds nothing", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{roomInfoNil: true}}
		got, err := consumer.partialStateServers([]spec.RoomID{*room})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v, want none", got, err)
		}
	})

	t.Run("a failed lookup is an error", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{
			partialState: true, queryRoomInfoErr: errors.New("boom"),
		}}
		if _, err := consumer.partialStateServers([]spec.RoomID{*room}); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("a failed partial state room lookup is an error", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{
			allRoomsErr: errors.New("boom"),
		}}
		if _, err := consumer.partialStateServers([]spec.RoomID{*room}); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("no partial state rooms skips the room lookup", func(t *testing.T) {
		consumer := &KeyChangeConsumer{ctx: context.Background(), rsAPI: &fakePartialStateRoomserverAPI{
			roomInfoNil: true,
		}}
		got, err := consumer.partialStateServers([]spec.RoomID{*room})
		if err != nil || len(got) != 0 {
			t.Fatalf("got %v, %v, want none", got, err)
		}
	})
}
