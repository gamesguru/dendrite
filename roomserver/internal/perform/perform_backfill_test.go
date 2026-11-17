package perform

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	federationAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// stateLookupFedAPI fails every state lookup with a per-server error.
type stateLookupFedAPI struct {
	federationAPI.RoomserverFederationAPI
	errs map[spec.ServerName]error
}

func (f stateLookupFedAPI) LookupStateIDs(
	_ context.Context, _, s spec.ServerName, _, _ string,
) (gomatrixserverlib.StateIDResponse, error) {
	return nil, f.errs[s]
}

func (f stateLookupFedAPI) LookupState(
	_ context.Context, _, s spec.ServerName, _, _ string, _ gomatrixserverlib.RoomVersion,
) (gomatrixserverlib.StateResponse, error) {
	return nil, f.errs[s]
}

// noLocalEventsDB makes the local event lookup fail so StateBeforeEvent falls
// through to federation.
type noLocalEventsDB struct{ storage.Database }

func (noLocalEventsDB) EventNIDs(context.Context, []string) (map[string]types.EventMetadata, error) {
	return nil, errors.New("no local events")
}

// StateAtEventIDs is the lookup StateIDsBeforeEvent consults first; the event
// is not stored locally, so federation has to answer.
func (noLocalEventsDB) StateAtEventIDs(context.Context, []string) ([]types.StateAtEvent, error) {
	return nil, errors.New("no local events")
}

func TestBackfillStateLookupFailuresAreRecorded(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	// Two prev events force a /state_ids request instead of rolling state forward.
	ev, err := verImpl.NewEventFromTrustedJSON([]byte(
		`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{},"origin_server_ts":1,"depth":2,`+
			`"prev_events":["$p1","$p2"],"auth_events":[]}`), false)
	require.NoError(t, err)

	servers := []spec.ServerName{"flaky", "gone"}
	cases := []struct {
		name      string
		errs      map[spec.ServerName]error
		wantCodes []int
		retryable bool
	}{
		{
			name: "transient then not found",
			errs: map[spec.ServerName]error{
				"flaky": spec.HTTPError{Code: http.StatusInternalServerError},
				"gone":  spec.HTTPError{Code: http.StatusNotFound},
			},
			wantCodes: []int{http.StatusInternalServerError, http.StatusNotFound},
			retryable: true,
		},
		{
			// If recording ever turned a definitive answer into a retryable
			// one, pagination would never see the end of history.
			name: "not found on every server",
			errs: map[spec.ServerName]error{
				"flaky": spec.HTTPError{Code: http.StatusNotFound},
				"gone":  spec.HTTPError{Code: http.StatusNotFound},
			},
			wantCodes: []int{http.StatusNotFound, http.StatusNotFound},
			retryable: false,
		},
	}

	lookups := map[string]func(*backfillRequester) error{
		"StateIDsBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateIDsBeforeEvent(context.Background(), ev)
			return err
		},
		"StateBeforeEvent": func(r *backfillRequester) error {
			_, err := r.StateBeforeEvent(context.Background(), gomatrixserverlib.RoomVersionV10, ev, []string{"$s"})
			return err
		},
	}

	for lookupName, lookup := range lookups {
		for _, tc := range cases {
			t.Run(lookupName+"/"+tc.name, func(t *testing.T) {
				r := newBackfillRequester(
					noLocalEventsDB{}, stateLookupFedAPI{errs: tc.errs},
					nil, "origin", nil, nil, nil, gomatrixserverlib.RoomVersionV10,
				)
				r.servers = servers

				err := lookup(r)
				// The last server's error is returned; earlier ones must still
				// count towards classification.
				var httpErr spec.HTTPError
				require.ErrorAs(t, err, &httpErr)
				require.Equal(t, tc.wantCodes[len(tc.wantCodes)-1], httpErr.Code)

				require.Len(t, r.backfillErrors, len(tc.wantCodes))
				for i, wantCode := range tc.wantCodes {
					var recorded spec.HTTPError
					require.ErrorAs(t, r.backfillErrors[i], &recorded)
					require.Equal(t, wantCode, recorded.Code, "recorded error %d", i)
				}
				require.Equal(t, tc.retryable, backfillFailuresRetryable(r.backfillErrors))
			})
		}
	}
}

func TestBackfillFailuresRetryable(t *testing.T) {
	notFound := spec.HTTPError{Code: http.StatusNotFound}
	cases := []struct {
		name string
		errs []error
		want bool
	}{
		{"no server responses", nil, true},
		{"not found", []error{notFound}, false},
		{"not found from every server", []error{notFound, notFound}, false},
		{"wrapped not found", []error{fmt.Errorf("backfill: %w", notFound)}, false},
		{"forbidden", []error{spec.HTTPError{Code: http.StatusForbidden}}, true},
		{"bad request", []error{spec.HTTPError{Code: http.StatusBadRequest}}, true},
		{"server error", []error{spec.HTTPError{Code: http.StatusInternalServerError}}, true},
		{"rate limited", []error{spec.HTTPError{Code: http.StatusTooManyRequests}}, true},
		{"mixed definitive and transient", []error{notFound, spec.HTTPError{Code: http.StatusBadGateway}}, true},
		{"network failure", []error{context.DeadlineExceeded}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backfillFailuresRetryable(tc.errs); got != tc.want {
				t.Fatalf("backfillFailuresRetryable(%v) = %v, want %v", tc.errs, got, tc.want)
			}
		})
	}
}

func TestPrioritiseBackfillFromEventIDsPrefersMostServers(t *testing.T) {
	fromEventIDs := []string{"$one", "$two", "$three"}
	serverCounts := map[string][]spec.ServerName{
		"$one":   {"hs1"},
		"$two":   {"hs1", "hs2"},
		"$three": {},
	}

	got := prioritiseBackfillFromEventIDs(
		context.Background(),
		"!room:test",
		fromEventIDs,
		func(_ context.Context, _, eventID string) []spec.ServerName {
			return serverCounts[eventID]
		},
	)

	if len(got) != len(fromEventIDs) {
		t.Fatalf("expected %d from-event IDs, got %d", len(fromEventIDs), len(got))
	}
	if got[0] != "$two" || got[1] != "$one" || got[2] != "$three" {
		t.Fatalf("expected best from-event first and others in order, got %v", got)
	}
}

func TestPrioritiseBackfillFromEventIDsKeepsOrderWhenNoBetterCandidate(t *testing.T) {
	fromEventIDs := []string{"$one", "$two"}

	got := prioritiseBackfillFromEventIDs(
		context.Background(),
		"!room:test",
		fromEventIDs,
		func(_ context.Context, _, eventID string) []spec.ServerName {
			if eventID == "$one" {
				return []spec.ServerName{"hs1"}
			}
			return []spec.ServerName{"hs2"}
		},
	)

	if got[0] != "$one" || got[1] != "$two" {
		t.Fatalf("expected original ordering to be preserved, got %v", got)
	}
}

// calculateNewStateIDs rolls the previous event's before-state forward into a
// new slice. The slice it is given is the previous event's own before-state,
// which is looked up again when another event is processed, so it must not be
// modified in place.
func TestCalculateNewStateIDsCopiesBeforeState(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}

	oldMember := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	prevEvent := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"leave"},"origin_server_ts":2,"depth":2,"prev_events":["$old"],"auth_events":[]}`)
	targetEvent := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["$prev"],"auth_events":[]}`)

	prevEventStateIDs := []string{oldMember.EventID()}
	requester := &backfillRequester{
		eventIDToBeforeStateIDs: map[string][]string{prevEvent.EventID(): prevEventStateIDs},
		eventIDMap:              map[string]gomatrixserverlib.PDU{oldMember.EventID(): oldMember},
	}

	got := requester.calculateNewStateIDs(targetEvent, prevEvent, prevEventStateIDs)
	require.Equal(t, []string{prevEvent.EventID()}, got)
	require.Equal(t, []string{oldMember.EventID()}, prevEventStateIDs,
		"the previous event's before-state must be left unchanged")
	require.Equal(t, []string{oldMember.EventID()}, requester.eventIDToBeforeStateIDs[prevEvent.EventID()])

	// An event without a state key inherits the previous state. An empty state
	// must stay non-nil so callers do not mistake it for a failed calculation.
	messageEvent := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":4,"depth":4,"prev_events":["$prev"],"auth_events":[]}`)
	inherited := requester.calculateNewStateIDs(targetEvent, messageEvent, nil)
	require.NotNil(t, inherited)
	require.Empty(t, inherited)
}

// storedEventsDB serves events by ID so a backfill can resolve state locally.
type storedEventsDB struct {
	storage.Database
	events map[string]gomatrixserverlib.PDU
	nids   map[types.EventNID]gomatrixserverlib.PDU
	reads  int

	// beforeState maps an event ID to the snapshot holding its before-state.
	beforeState map[string]types.StateSnapshotNID
	// snapshots maps a snapshot to the NIDs of the events it contains.
	snapshots map[types.StateSnapshotNID][]types.EventNID
	// partial marks the room as having partial state, so snapshots must not
	// be reused.
	partial bool
	// incomplete is the durable marker left behind when a room leaves partial
	// state, whether by resync or by an abandoned join.
	incomplete bool
}

func (d *storedEventsDB) IsRoomPartialState(context.Context, types.RoomNID) (bool, error) {
	return d.partial, nil
}

func (d *storedEventsDB) StateSnapshotsIncomplete(context.Context, types.RoomNID) (bool, error) {
	return d.incomplete, nil
}

func newStoredEventsDB(events ...gomatrixserverlib.PDU) *storedEventsDB {
	db := &storedEventsDB{
		events: map[string]gomatrixserverlib.PDU{},
		nids:   map[types.EventNID]gomatrixserverlib.PDU{},
	}
	for i, ev := range events {
		db.events[ev.EventID()] = ev
		db.nids[types.EventNID(i+1)] = ev
	}
	return db
}

func (d *storedEventsDB) EventNIDs(_ context.Context, ids []string) (map[string]types.EventMetadata, error) {
	d.reads++
	out := map[string]types.EventMetadata{}
	for nid, ev := range d.nids {
		for _, id := range ids {
			if ev.EventID() == id {
				out[id] = types.EventMetadata{EventNID: nid}
			}
		}
	}
	return out, nil
}

func (d *storedEventsDB) StateAtEventIDs(_ context.Context, ids []string) ([]types.StateAtEvent, error) {
	var out []types.StateAtEvent
	for _, id := range ids {
		if nid, ok := d.beforeState[id]; ok {
			out = append(out, types.StateAtEvent{BeforeStateSnapshotNID: nid})
		}
	}
	return out, nil
}

func (d *storedEventsDB) StateHAMTEntries(_ context.Context, nid types.StateSnapshotNID) ([]types.StateEntry, bool, error) {
	nids, ok := d.snapshots[nid]
	if !ok {
		return nil, false, nil
	}
	entries := make([]types.StateEntry, 0, len(nids))
	for _, n := range nids {
		entries = append(entries, types.StateEntry{EventNID: n})
	}
	return entries, true, nil
}

func (d *storedEventsDB) EventIDs(_ context.Context, nids []types.EventNID) (map[types.EventNID]string, error) {
	out := map[types.EventNID]string{}
	for _, n := range nids {
		if ev, ok := d.nids[n]; ok {
			out[n] = ev.EventID()
		}
	}
	return out, nil
}

func (d *storedEventsDB) Events(_ context.Context, _ gomatrixserverlib.RoomVersion, nids []types.EventNID) ([]types.Event, error) {
	var out []types.Event
	for _, nid := range nids {
		out = append(out, types.Event{EventNID: nid, PDU: d.nids[nid]})
	}
	return out, nil
}

// countingFedClient records /state_ids lookups. It always fails, so a test
// that reaches federation sees an error as well as a counted request.
type countingFedClient struct {
	federationAPI.RoomserverFederationAPI
	stateIDLookups int
}

func (c *countingFedClient) LookupStateIDs(
	context.Context, spec.ServerName, spec.ServerName, string, string,
) (gomatrixserverlib.StateIDResponse, error) {
	c.stateIDLookups++
	return nil, errors.New("fake federation: no /state_ids available")
}

// Rolling a state event forward needs the type and state key of every event in
// the previous before-state. Those events are usually older than the batch but
// already stored, so they must come from the database, not from /state_ids.
func TestStateIDsBeforeEventRollsForwardFromStoredState(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	oldMember := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	prevEvent := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"leave"},"origin_server_ts":2,"depth":2,"prev_events":["$old"],"auth_events":[]}`)
	target := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["` + prevEvent.EventID() + `"],"auth_events":[]}`)

	fed := &countingFedClient{}
	build := func(db storage.Database) *backfillRequester {
		return &backfillRequester{
			fsAPI:                   fed,
			servers:                 []spec.ServerName{"remote.test"},
			db:                      db,
			roomVersion:             gomatrixserverlib.RoomVersionV10,
			eventIDToBeforeStateIDs: map[string][]string{prevEvent.EventID(): {oldMember.EventID()}},
			eventIDMap:              map[string]gomatrixserverlib.PDU{prevEvent.EventID(): prevEvent},
		}
	}
	db := newStoredEventsDB(oldMember)
	got, err := build(db).StateIDsBeforeEvent(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, []string{prevEvent.EventID()}, got,
		"the member event the previous event replaces is stored, so no federation request is needed")
	require.Zero(t, fed.stateIDLookups, "local state must not trigger /state_ids")

	// With the old member not stored locally the state cannot be rolled forward,
	// so the requester must attempt federation.
	got, err = build(newStoredEventsDB()).StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, 1, fed.stateIDLookups)
}

// A later backfill page gets a new requester. An event stored by an earlier page
// already has a before-state snapshot, which must be reused instead of asking
// the remote server for /state_ids again.
func TestStateIDsBeforeEventReusesStoredSnapshot(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	member := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	target := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["$unknown1","$unknown2"],"auth_events":[]}`)

	// target has two unstored parents, so it cannot be rolled forward: only the
	// stored snapshot can answer without a federation request.
	db := newStoredEventsDB(member, target)
	db.beforeState = map[string]types.StateSnapshotNID{target.EventID(): 7}
	db.snapshots = map[types.StateSnapshotNID][]types.EventNID{7: {1}}

	fed := &countingFedClient{}
	requester := &backfillRequester{
		fsAPI:                   fed,
		servers:                 []spec.ServerName{"remote.test"},
		db:                      db,
		roomVersion:             gomatrixserverlib.RoomVersionV10,
		eventIDToBeforeStateIDs: map[string][]string{},
		eventIDMap:              map[string]gomatrixserverlib.PDU{},
	}
	got, err := requester.StateIDsBeforeEvent(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, []string{member.EventID()}, got)
	require.Zero(t, fed.stateIDLookups, "a stored snapshot must not trigger /state_ids")

	// An event with no stored snapshot falls through to federation.
	db.beforeState = nil
	requester.eventIDToBeforeStateIDs = map[string][]string{}
	got, err = requester.StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, 1, fed.stateIDLookups)
}

// An event whose single parent is stored, with a before-state snapshot, from an
// earlier page or request has its state computed locally from that parent. The
// parent is not part of this requester's own maps.
func TestStateIDsBeforeEventRollsForwardFromStoredParent(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	member := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	parent := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":2,"depth":2,"prev_events":["$older"],"auth_events":[]}`)
	target := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["` + parent.EventID() + `"],"auth_events":[]}`)

	db := newStoredEventsDB(member, parent)
	db.beforeState = map[string]types.StateSnapshotNID{parent.EventID(): 7}
	db.snapshots = map[types.StateSnapshotNID][]types.EventNID{7: {1}}

	fed := &countingFedClient{}
	build := func() *backfillRequester {
		return &backfillRequester{
			fsAPI:                   fed,
			servers:                 []spec.ServerName{"remote.test"},
			db:                      db,
			roomVersion:             gomatrixserverlib.RoomVersionV10,
			roomNID:                 1,
			eventIDToBeforeStateIDs: map[string][]string{},
			eventIDMap:              map[string]gomatrixserverlib.PDU{},
		}
	}
	got, err := build().StateIDsBeforeEvent(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, []string{member.EventID()}, got,
		"a message does not change state, so the target has its parent's before-state")
	require.Zero(t, fed.stateIDLookups, "a stored parent must not trigger /state_ids")

	// Without a snapshot for the parent the state is unknown, so federation is asked.
	db.beforeState = nil
	got, err = build().StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, 1, fed.stateIDLookups)

	// A room whose snapshots cannot be trusted never reuses the parent's.
	db.beforeState = map[string]types.StateSnapshotNID{parent.EventID(): 7}
	db.incomplete = true
	got, err = build().StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, 2, fed.stateIDLookups)
}

// A room with partial state (MSC3706) can hold incomplete before-state
// snapshots, so they must not be reused: backfill asks federation instead.
func TestStateIDsBeforeEventIgnoresSnapshotForPartialRoom(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	member := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	target := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["$unknown1","$unknown2"],"auth_events":[]}`)

	db := newStoredEventsDB(member, target)
	db.beforeState = map[string]types.StateSnapshotNID{target.EventID(): 7}
	db.snapshots = map[types.StateSnapshotNID][]types.EventNID{7: {1}}
	db.partial = true

	fed := &countingFedClient{}
	requester := &backfillRequester{
		fsAPI:                   fed,
		servers:                 []spec.ServerName{"remote.test"},
		db:                      db,
		roomNID:                 1,
		roomVersion:             gomatrixserverlib.RoomVersionV10,
		eventIDToBeforeStateIDs: map[string][]string{},
		eventIDMap:              map[string]gomatrixserverlib.PDU{},
	}
	got, err := requester.StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got, "a partially-stated room must not reuse its stored snapshot")
	require.Equal(t, 1, fed.stateIDLookups, "a partial room must ask federation")

	// The same snapshot is reused once the room is no longer partial.
	db.partial = false
	requester.partialStateChecked = false
	got, err = requester.StateIDsBeforeEvent(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, []string{member.EventID()}, got)
	require.Equal(t, 1, fed.stateIDLookups, "no further /state_ids once the room is not partial")
}

// The partial flag can be cleared without a successful resync (for example,
// when an abandoned join is removed). The durable marker still prevents reuse.
func TestStateIDsBeforeEventIgnoresSnapshotAfterPartialStateEnds(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	member := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test",` +
		`"content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`)
	target := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@b:test",` +
		`"content":{},"origin_server_ts":3,"depth":3,"prev_events":["$unknown1","$unknown2"],"auth_events":[]}`)

	db := newStoredEventsDB(member, target)
	db.beforeState = map[string]types.StateSnapshotNID{target.EventID(): 7}
	db.snapshots = map[types.StateSnapshotNID][]types.EventNID{7: {1}}
	db.incomplete = true

	fed := &countingFedClient{}
	requester := &backfillRequester{
		fsAPI:                   fed,
		servers:                 []spec.ServerName{"remote.test"},
		db:                      db,
		roomNID:                 1,
		roomVersion:             gomatrixserverlib.RoomVersionV10,
		eventIDToBeforeStateIDs: map[string][]string{},
		eventIDMap:              map[string]gomatrixserverlib.PDU{},
	}
	got, err := requester.StateIDsBeforeEvent(context.Background(), target)
	require.Error(t, err)
	require.Nil(t, got, "a room that left partial state must not reuse snapshots it may not have repaired")
	require.Equal(t, 1, fed.stateIDLookups, "a room that left partial state must ask federation")
}
