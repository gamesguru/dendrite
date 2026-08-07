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
	"github.com/element-hq/dendrite/setup/jetstream"
	"github.com/element-hq/dendrite/test"
	"github.com/element-hq/dendrite/test/testrig"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

// TestDisconnectedJoinerSendsMessage reproduces the den.nutra.tk production
// scenario: a user (dave) joins a room, but that join is never referenced by
// the room's forward extremities - the shape you get when a join is learned
// about via backfill/gap-fill rather than processed live, which is exactly
// what "KindOld" means in this codebase (see input_events.go: KindOld skips
// updateLatestEvents entirely, so it never enters current state's fold no
// matter how valid the event is). A KindNew join, by contrast, becomes an
// extremity and stays part of current state even if nothing later
// references it - that would NOT reproduce the production bug, which is why
// this test deliberately avoids test.Room's normal CreateAndInsert helper
// for the events built after dave's join: that helper chains every new
// event's prev_events onto whatever was inserted last, which would make
// dave's join an ancestor of everything that follows regardless of Kind.
//
// After the room's mainline moves on with no path back to dave's join, dave
// sends a brand new message whose prev_events span both his own last-known
// event and the room's current tip - the "someone sends a message" fix
// floated for production tonight. This checks whether that new message is
// correctly authed (dave recognised as a room member) and whether it's own
// state gets folded back into current state afterwards.
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

		if err := api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send create-room events: %v", err)
		}

		// The room's tip right after creation, before dave joins - this is
		// the branch point everything below diverges from.
		preJoinTip := room.Events()[len(room.Events())-1]

		// dave's join, submitted as KindOld (i.e. as if learned about via
		// backfill/gap-fill, not processed live). Per input_events.go, this
		// skips updateLatestEvents entirely, so it will never become part of
		// current state's fold no matter what its own state resolves to -
		// exactly the shape of the three stuck production events (present,
		// signature-valid, is_rejected=false, but never an extremity).
		daveJoinEv := mustCreateEvent(t, fledglingEvent{
			Type:     spec.MRoomMember,
			StateKey: &dave.ID,
			SenderID: dave.ID,
			RoomID:   room.ID,
			Content:  map[string]any{"membership": "join"},
			Depth:    preJoinTip.Depth() + 1,
			PrevEvents: []any{
				preJoinTip.EventID(),
			},
			AuthEvents: []any{
				room.Events()[0].EventID(), // create event
				room.Events()[2].EventID(), // power levels event
				room.Events()[3].EventID(), // join rules event
			},
		})

		res := &api.InputRoomEventsResponse{}
		rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{{
				Kind:   api.KindOld,
				Event:  daveJoinEv,
				Origin: "test",
			}},
			Asynchronous: false,
		}, res)
		if res.ErrMsg != "" {
			t.Fatalf("failed to insert dave's KindOld join: %s", res.ErrMsg)
		}

		// Sanity check: dave must NOT be part of current state at this
		// point - if he is, this test isn't reproducing the bug shape and
		// everything below is meaningless.
		if ev := api.GetStateEvent(ctx, rsAPI, room.ID, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: dave.ID}); ev != nil {
			t.Fatalf("dave is already in current state right after a KindOld join - test setup didn't reproduce a disconnected join")
		}

		// The room moves on from the PRE-join tip, never referencing dave's
		// join. Built manually (not via room.CreateAndInsert, which would
		// chain prev_events onto dave's join since it's the last-inserted
		// event) so the mainline genuinely never has dave's join as an
		// ancestor.
		tip := preJoinTip
		for i := 0; i < 5; i++ {
			msg := mustCreateEvent(t, fledglingEvent{
				Type:     "m.room.message",
				SenderID: alice.ID,
				RoomID:   room.ID,
				Content:  map[string]any{"body": "hello"},
				Depth:    tip.Depth() + 1,
				PrevEvents: []any{
					tip.EventID(),
				},
				AuthEvents: []any{
					room.Events()[0].EventID(), // create event
					room.Events()[1].EventID(), // alice's own join, for membership auth
					room.Events()[2].EventID(), // power levels event
				},
			})
			res = &api.InputRoomEventsResponse{}
			rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
				InputRoomEvents: []api.InputRoomEvent{{
					Kind:   api.KindNew,
					Event:  msg,
					Origin: "test",
				}},
				Asynchronous: false,
			}, res)
			if res.ErrMsg != "" {
				t.Fatalf("failed to send filler message %d: %s", i, res.ErrMsg)
			}
			tip = msg
		}

		// dave sends a brand new message. Its prev_events span BOTH his own
		// last known event (the join, still disconnected from current
		// state) and the room's current tip - exactly the shape of a live
		// message fixing a stuck state gap. EXPECTATION: this fails, because
		// CheckForSoftFail (roomserver/internal/helpers/auth.go) checks the
		// event against roomInfo's CURRENT state snapshot regardless of the
		// event's own prev_events - dave isn't currently a member, so this
		// soft-fails no matter what his message's ancestry says.
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

		res = &api.InputRoomEventsResponse{}
		rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{{
				Kind:   api.KindNew,
				Event:  daveMsg,
				Origin: "test",
			}},
			Asynchronous: false,
		}, res)
		if res.ErrMsg != "" {
			t.Fatalf("dave's own bridging message failed outright (expected soft-fail, not an error): %s", res.ErrMsg)
		}
		if ev := api.GetStateEvent(ctx, rsAPI, room.ID, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: dave.ID}); ev != nil {
			t.Fatalf("dave's own message unexpectedly fixed current state - soft-fail theory is wrong, investigate")
		}

		// Now: alice (a CURRENTLY recognised member) sends a message whose
		// prev_events span the same disconnected branch. Her sender identity
		// is valid against CURRENT state, so CheckForSoftFail should pass
		// regardless of her prev_events - and if it does, her event's own
		// "state after" gets computed via the same multi-branch merge that
		// dave's message exercised, which should now correctly fold dave
		// back into state, and because this event isn't soft-failed, it
		// SHOULD be allowed to become a new forward extremity and actually
		// update current state.
		aliceBridge := mustCreateEvent(t, fledglingEvent{
			Type:     "m.room.message",
			SenderID: alice.ID,
			RoomID:   room.ID,
			Depth:    tip.Depth() + 1,
			Content:  map[string]any{"body": "bridging"},
			PrevEvents: []any{
				daveJoinEv.EventID(),
				tip.EventID(),
			},
			AuthEvents: []any{
				room.Events()[0].EventID(), // create event
				room.Events()[1].EventID(), // alice's own join, for membership auth
				room.Events()[2].EventID(), // power levels event
			},
		})

		res = &api.InputRoomEventsResponse{}
		rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{{
				Kind:   api.KindNew,
				Event:  aliceBridge,
				Origin: "test",
			}},
			Asynchronous: false,
		}, res)
		if res.ErrMsg != "" {
			t.Fatalf("alice's bridging message was rejected: %s", res.ErrMsg)
		}

		if ev := api.GetStateEvent(ctx, rsAPI, room.ID, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: dave.ID}); ev == nil {
			t.Fatalf("dave still isn't in current state after a CURRENTLY-recognised member's bridging message")
		}
	})
}
