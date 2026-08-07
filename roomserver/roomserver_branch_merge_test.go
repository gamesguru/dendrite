// Copyright 2024 New Vector Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial

package roomserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/internal/sqlutil"
	"github.com/element-hq/dendrite/roomserver"
	"github.com/element-hq/dendrite/roomserver/api"
	"github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/setup/jetstream"
	"github.com/element-hq/dendrite/test"
	"github.com/element-hq/dendrite/test/testrig"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// TestDisconnectedJoinerSendsMessage reproduces the den.nutra.tk production
// scenario: a user (dave) joins a room, but the room's mainline then moves
// on for a long time without ever referencing dave's join again (a
// disconnected branch, same shape as gg/ggdev/shane:wombatx.me on the real
// room - present as a stored event, but not an ancestor of the current
// extremity). Later, dave sends a brand new message whose prev_events span
// both his own last-known event and the room's current tip - the "someone
// sends a message" fix floated for production tonight.
//
// This checks whether that new message is correctly authed (dave recognised
// as a room member) and whether it's own state gets folded back in
// afterwards.
func TestDisconnectedJoinerSendsMessage(t *testing.T) {
	alice := test.NewUser(t)
	dave := test.NewUser(t)
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()

		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		natsInstance := jetstream.NATSInstance{}
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)

		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))

		// dave joins early on.
		daveJoinEv := room.CreateAndInsert(t, dave, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(dave.ID))

		if err := api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send events: %v", err)
		}
		if err := api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{daveJoinEv}, "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send dave's join: %v", err)
		}

		// Sanity check: dave is currently a member.
		if ev := api.GetStateEvent(ctx, rsAPI, room.ID, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: dave.ID}); ev == nil {
			t.Fatalf("dave's join did not land")
		}

		// Now the room moves on WITHOUT ever referencing dave's join again -
		// mirrors the real room continuing on a mainline that never
		// re-references the branch dave's join is on.
		var tip *types.HeaderedEvent
		for i := 0; i < 10; i++ {
			msg := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hello"})
			if err := api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{msg}, "test", "test", "test", nil, false); err != nil {
				t.Fatalf("failed to send filler message %d: %v", i, err)
			}
			tip = msg
		}

		// dave sends a brand new message. Its prev_events span BOTH his own
		// last known event (the join, now "disconnected") and the room's
		// current tip - exactly the shape of a live message fixing a stuck
		// state gap.
		daveMsg := mustCreateEvent(t, fledglingEvent{
			Type:     "m.room.message",
			SenderID: dave.ID,
			RoomID:   room.ID,
			Depth:    tip.Depth() + 1,
			Content:  map[string]any{"body": "still here"},
			PrevEvents: []any{
				daveJoinEv.EventID(),
				tip.EventID(),
			},
			AuthEvents: []any{
				room.Events()[0].EventID(), // create event
				room.Events()[2].EventID(), // power levels event
				daveJoinEv.EventID(),       // dave's own join, for membership auth
			},
		})

		res := &api.InputRoomEventsResponse{}
		rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{{
				Kind:   api.KindNew,
				Event:  daveMsg,
				Origin: "test",
			}},
			Asynchronous: false,
		}, res)

		if res.ErrMsg != "" {
			t.Fatalf("dave's bridging message was rejected: %s", res.ErrMsg)
		}

		// And check whether dave is now recognised as a current member.
		if ev := api.GetStateEvent(ctx, rsAPI, room.ID, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: dave.ID}); ev == nil {
			t.Fatalf("dave dropped out of current state after his bridging message")
		}
	})
}
