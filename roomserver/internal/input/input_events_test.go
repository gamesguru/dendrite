package input

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/internal/sqlutil"
	"github.com/element-hq/dendrite/roomserver/api"
	"github.com/element-hq/dendrite/roomserver/storage"
	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/test/testrig"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/element-hq/dendrite/test"
)

func Test_EventAuth(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	// create two rooms, so we can craft "illegal" auth events
	room1 := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))

	authEventIDs := make([]string, 0, 4)
	authEvents := []gomatrixserverlib.PDU{}

	// Add the legal auth events from room2
	for _, x := range room2.Events() {
		if x.Type() == spec.MRoomCreate {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
		if x.Type() == spec.MRoomPowerLevels {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
		if x.Type() == spec.MRoomJoinRules {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
	}

	// Add the illegal auth event from room1 (rooms are different)
	for _, x := range room1.Events() {
		if x.Type() == spec.MRoomMember {
			authEventIDs = append(authEventIDs, x.EventID())
			authEvents = append(authEvents, x.PDU)
		}
	}

	// Craft the illegal join event, with auth events from different rooms
	ev := room2.CreateEvent(t, bob, "m.room.member", map[string]interface{}{
		"membership": "join",
	}, test.WithStateKey(bob.ID), test.WithAuthIDs(authEventIDs))

	// Add the auth events to the allower
	allower, _ := gomatrixserverlib.NewAuthEvents(nil)
	for _, a := range authEvents {
		if err := allower.AddEvent(a); err != nil {
			t.Fatalf("allower.AddEvent failed: %v", err)
		}
	}

	// Finally check that the event is NOT allowed
	if err := gomatrixserverlib.Allowed(ev.PDU, allower, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return spec.NewUserID(string(senderID), true)
	}); err == nil {
		t.Fatalf("event should not be allowed, but it was")
	}
}

func TestRoomInfoFromSuppliedStateRejectsForeignStateEvents(t *testing.T) {
	alice := test.NewUser(t)
	room1 := test.NewRoom(t, alice)
	room2 := test.NewRoom(t, alice)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		db, err := storage.Open(processCtx.Context(), cm, &cfg.RoomServer.Database, caches)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(processCtx.Context(), 5*time.Second)
		defer cancel()

		create1 := stateEventOfType(t, room1, spec.MRoomCreate)
		foreignMember := stateEventOfType(t, room2, spec.MRoomMember)
		storeEventForSuppliedState(t, ctx, db, create1)
		storeEventForSuppliedState(t, ctx, db, foreignMember)

		inputer := &Inputer{DB: db}
		event := room1.CreateEvent(t, alice, spec.MRoomMember, map[string]any{
			"membership": spec.Join,
		}, test.WithStateKey(alice.ID))
		_, err = inputer.roomInfoFromSuppliedState(ctx, &api.InputRoomEvent{
			Event: &rstypes.HeaderedEvent{PDU: event.PDU},
			StateEventIDs: []string{
				create1.EventID(),
				foreignMember.EventID(),
			},
		})
		if err == nil {
			t.Fatal("expected foreign supplied state event to be rejected")
		}
		for _, want := range []string{foreignMember.EventID(), room2.ID, room1.ID} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("expected error %q to contain %q", err.Error(), want)
			}
		}
	})
}

func stateEventOfType(t *testing.T, room *test.Room, eventType string) *rstypes.HeaderedEvent {
	t.Helper()
	for _, ev := range room.Events() {
		if ev.Type() == eventType && ev.StateKey() != nil {
			return ev
		}
	}
	t.Fatalf("room %s has no state event of type %s", room.ID, eventType)
	return nil
}

func storeEventForSuppliedState(t *testing.T, ctx context.Context, db storage.Database, ev *rstypes.HeaderedEvent) {
	t.Helper()
	roomInfo, err := db.GetOrCreateRoomInfo(ctx, ev.PDU)
	if err != nil {
		t.Fatal(err)
	}
	eventTypeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
	if err != nil {
		t.Fatal(err)
	}
	eventStateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = db.StoreEvent(ctx, ev.PDU, roomInfo, eventTypeNID, eventStateKeyNID, nil, false); err != nil {
		t.Fatal(err)
	}
}
