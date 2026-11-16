// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package helpers

import (
	"context"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// softFailTestDB answers the lookups CheckForSoftFail makes once it has state
// entries to authenticate against.
type softFailTestDB struct {
	storage.RoomDatabase
	entries []types.StateEntry
}

func (d softFailTestDB) StateEntriesForEventIDs(
	context.Context, []string, bool,
) ([]types.StateEntry, error) {
	return d.entries, nil
}

func (d softFailTestDB) EventStateKeyNIDs(
	context.Context, []string,
) (map[string]types.EventStateKeyNID, error) {
	return map[string]types.EventStateKeyNID{}, nil
}

func (d softFailTestDB) Events(
	context.Context, gomatrixserverlib.RoomVersion, []types.EventNID,
) ([]types.Event, error) {
	return nil, nil
}

type failingUserIDQuerier struct {
	err error
}

func (q failingUserIDQuerier) QuerySenderIDForUser(
	context.Context, spec.RoomID, spec.UserID,
) (*spec.SenderID, error) {
	return nil, q.err
}

func (q failingUserIDQuerier) QueryUserIDForSender(
	context.Context, spec.RoomID, spec.SenderID,
) (*spec.UserID, error) {
	return nil, q.err
}

func newSoftFailTestEvent(t *testing.T, prevEvents string) gomatrixserverlib.PDU {
	t.Helper()
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	event, err := verImpl.NewEventFromTrustedJSON([]byte(
		`{"type":"m.room.create","room_id":"!r:test","sender":"@a:test","state_key":"",`+
			`"content":{"room_version":"10"},"origin_server_ts":1,"depth":1,`+
			`"prev_events":[`+prevEvents+`],"auth_events":[]}`), false)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

// A genuine auth denial is a soft-fail, even though Allowed reports it as an
// error.
func TestCheckForSoftFailAuthDenialIsASoftFail(t *testing.T) {
	db := softFailTestDB{entries: []types.StateEntry{{EventNID: 1}}}
	softFail, err := CheckForSoftFail(
		context.Background(),
		db,
		&types.RoomInfo{RoomVersion: gomatrixserverlib.RoomVersionV10},
		&types.HeaderedEvent{PDU: newSoftFailTestEvent(t, `"$previous"`)},
		[]string{"$one", "$two"}, // more than one ID rewrites state.
		failingUserIDQuerier{err: errors.New("unused")},
	)
	if err != nil {
		t.Fatalf("auth denial was returned as an error: %v", err)
	}
	if !softFail {
		t.Fatal("auth denial must be reported as a soft-fail")
	}
}

// A sender-ID lookup failure is not evidence of an auth conflict, so it must
// be returned as an error rather than quietly soft-failing the event.
func TestCheckForSoftFailLookupErrorIsNotASoftFail(t *testing.T) {
	lookupErr := errors.New("sender lookup failed")
	db := softFailTestDB{entries: []types.StateEntry{{EventNID: 1}}}
	softFail, err := CheckForSoftFail(
		context.Background(),
		db,
		&types.RoomInfo{RoomVersion: gomatrixserverlib.RoomVersionV10},
		&types.HeaderedEvent{PDU: newSoftFailTestEvent(t, "")},
		[]string{"$one", "$two"}, // more than one ID rewrites state.
		failingUserIDQuerier{err: lookupErr},
	)
	if !errors.Is(err, lookupErr) {
		t.Fatalf("expected the sender lookup error, got %v", err)
	}
	if softFail {
		t.Fatal("a sender lookup error must not be reported as a soft-fail")
	}
}

// failingStateEntriesDB fails the state lookup that CheckForSoftFail uses when
// the input rewrites state.
type failingStateEntriesDB struct {
	storage.RoomDatabase
}

func (failingStateEntriesDB) StateEntriesForEventIDs(
	context.Context, []string, bool,
) ([]types.StateEntry, error) {
	return nil, errors.New("state lookup failed")
}

// A lookup failure must not be reported as a soft-fail: telling the caller
// "soft-fail" would store the event but never show it to clients.
func TestCheckForSoftFailStateLookupFailureIsNotASoftFail(t *testing.T) {
	softFail, err := CheckForSoftFail(
		context.Background(),
		failingStateEntriesDB{},
		nil,
		nil,
		[]string{"$one", "$two"}, // more than one ID rewrites state.
		nil,
	)
	if err == nil {
		t.Fatal("expected the state lookup failure to be returned")
	}
	if softFail {
		t.Fatal("a state lookup failure must not be reported as a soft-fail")
	}
}

var _ api.QuerySenderIDAPI = failingUserIDQuerier{}
