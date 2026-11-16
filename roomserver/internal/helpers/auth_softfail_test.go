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

func (d softFailTestDB) StateHAMTEntries(context.Context, types.StateSnapshotNID) ([]types.StateEntry, bool, error) {
	return nil, false, nil
}

func (d softFailTestDB) StateBlockNIDs(_ context.Context, stateNIDs []types.StateSnapshotNID) ([]types.StateBlockNIDList, error) {
	return []types.StateBlockNIDList{{StateSnapshotNID: stateNIDs[0], StateBlockNIDs: []types.StateBlockNID{1}}}, nil
}

func (d softFailTestDB) StateEntries(context.Context, []types.StateBlockNID) ([]types.StateEntryList, error) {
	return []types.StateEntryList{{StateBlockNID: 1, StateEntries: d.entries}}, nil
}

type failingSnapshotDB struct {
	softFailTestDB
	err error
}

func (d failingSnapshotDB) StateHAMTEntries(context.Context, types.StateSnapshotNID) ([]types.StateEntry, bool, error) {
	return nil, false, d.err
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
		[]string{"$one"},
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
		[]string{"$one"},
		failingUserIDQuerier{err: lookupErr},
	)
	if !errors.Is(err, lookupErr) {
		t.Fatalf("expected the sender lookup error, got %v", err)
	}
	if softFail {
		t.Fatal("a sender lookup error must not be reported as a soft-fail")
	}
}

// A lookup failure must not be reported as a soft-fail: telling the caller
// "soft-fail" would store the event but never show it to clients.
func TestCheckForSoftFailSnapshotLookupFailureIsNotASoftFail(t *testing.T) {
	lookupErr := errors.New("snapshot lookup failed")
	softFail, err := CheckForSoftFail(
		context.Background(),
		failingSnapshotDB{softFailTestDB: softFailTestDB{}, err: lookupErr},
		&types.RoomInfo{RoomVersion: gomatrixserverlib.RoomVersionV10},
		nil,
		[]string{"$one"},
		nil,
	)
	if !errors.Is(err, lookupErr) {
		t.Fatalf("expected snapshot lookup failure, got %v", err)
	}
	if softFail {
		t.Fatal("a state lookup failure must not be reported as a soft-fail")
	}
}

// currentStateDB serves the current-state snapshot path, taken when the input
// carries at most one state event ID.
type currentStateDB struct {
	softFailTestDB
	entriesErr error
}

func (d currentStateDB) StateHAMTEntries(
	context.Context, types.StateSnapshotNID,
) ([]types.StateEntry, bool, error) {
	return d.entries, d.entriesErr == nil, d.entriesErr
}

// With a single state event ID the current state comes from the snapshot, and
// neither a snapshot load failure nor a sender lookup failure is a soft-fail.
func TestCheckForSoftFailCurrentStateLookupFailuresAreNotSoftFails(t *testing.T) {
	snapshotErr := errors.New("snapshot load failed")
	senderErr := errors.New("sender lookup failed")
	for name, tc := range map[string]struct {
		db      currentStateDB
		querier api.QuerySenderIDAPI
		wantErr error
	}{
		"snapshot load": {
			db:      currentStateDB{entriesErr: snapshotErr},
			querier: failingUserIDQuerier{err: errors.New("unused")},
			wantErr: snapshotErr,
		},
		"sender lookup": {
			db: currentStateDB{softFailTestDB: softFailTestDB{
				entries: []types.StateEntry{{EventNID: 1}},
			}},
			querier: failingUserIDQuerier{err: senderErr},
			wantErr: senderErr,
		},
	} {
		t.Run(name, func(t *testing.T) {
			softFail, err := CheckForSoftFail(
				context.Background(),
				tc.db,
				&types.RoomInfo{RoomVersion: gomatrixserverlib.RoomVersionV10},
				&types.HeaderedEvent{PDU: newSoftFailTestEvent(t, "")},
				[]string{"$one"}, // a single ID reads the current snapshot.
				tc.querier,
			)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
			if softFail {
				t.Fatal("a lookup failure must not be reported as a soft-fail")
			}
		})
	}
}

var _ api.QuerySenderIDAPI = failingUserIDQuerier{}
