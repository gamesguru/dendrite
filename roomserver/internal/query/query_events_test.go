// Copyright 2026 Shane Jaroch
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// eventsByIDDB serves a fixed event set and reports a before-state snapshot only
// for events that are part of the room DAG.
type eventsByIDDB struct {
	storage.Database
	info      *types.RoomInfo
	events    []types.Event
	snapshots map[string]types.StateSnapshotNID
	sent      map[types.EventNID]bool
}

func (d *eventsByIDDB) RoomInfo(context.Context, string) (*types.RoomInfo, error) {
	return d.info, nil
}

func (d *eventsByIDDB) EventsFromIDs(context.Context, *types.RoomInfo, []string) ([]types.Event, error) {
	return d.events, nil
}

func (d *eventsByIDDB) StateEntriesForEventIDs(context.Context, []string, bool) ([]types.StateEntry, error) {
	return nil, nil
}

func (d *eventsByIDDB) SnapshotNIDFromEventID(_ context.Context, eventID string) (types.StateSnapshotNID, error) {
	nid, ok := d.snapshots[eventID]
	if !ok || nid == 0 {
		return 0, errors.New("event has no state")
	}
	return nid, nil
}

func (d *eventsByIDDB) EventSentToOutput(_ context.Context, eventNID types.EventNID) (bool, error) {
	return d.sent[eventNID], nil
}

// An outlier is stored without a before-state and has no ancestry. A caller
// asking whether missing history is covered must not be told it can stop.
// An event already sent to the output log only needs the consumer to catch up,
// so it must not close a gap either.
func TestQueryEventsByIDExcludesUnusableEvents(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string) gomatrixserverlib.PDU {
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(body), false)
		require.NoError(t, err)
		return ev
	}
	outputSent := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{},"origin_server_ts":2,"depth":2,"prev_events":["$p"],"auth_events":[]}`)
	stillPending := newEvent(`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{},"origin_server_ts":3,"depth":3,"prev_events":["$p2"],"auth_events":[]}`)
	outlier := newEvent(`{"type":"m.room.member","room_id":"!r:test","sender":"@b:test","state_key":"@b:test","content":{"membership":"join"},"origin_server_ts":1,"depth":1,"prev_events":["$q"],"auth_events":[]}`)

	db := &eventsByIDDB{
		info: &types.RoomInfo{RoomNID: 1, RoomVersion: gomatrixserverlib.RoomVersionV10},
		events: []types.Event{
			{EventNID: 1, PDU: outputSent},
			{EventNID: 2, PDU: stillPending},
			{EventNID: 3, PDU: outlier},
		},
		snapshots: map[string]types.StateSnapshotNID{
			outputSent.EventID():   5,
			stillPending.EventID(): 6,
		},
		sent: map[types.EventNID]bool{1: true},
	}
	q := &Queryer{DB: db}

	var res api.QueryEventsByIDResponse
	err := q.QueryEventsByID(context.Background(), &api.QueryEventsByIDRequest{
		RoomID:          "!r:test",
		EventIDs:        []string{outputSent.EventID(), stillPending.EventID(), outlier.EventID()},
		ExcludeOutliers: true,
	}, &res)
	require.NoError(t, err)
	require.Len(t, res.Events, 2, "the outlier must be omitted")
	require.Contains(t, res.EventNIDs, outputSent.EventID())
	require.Contains(t, res.EventNIDs, stillPending.EventID())

	res = api.QueryEventsByIDResponse{}
	err = q.QueryEventsByID(context.Background(), &api.QueryEventsByIDRequest{
		RoomID:              "!r:test",
		EventIDs:            []string{outputSent.EventID(), stillPending.EventID(), outlier.EventID()},
		ExcludeOutliers:     true,
		ExcludeSentToOutput: true,
	}, &res)
	require.NoError(t, err)
	require.Len(t, res.Events, 1, "only the event still waiting for output may close a gap")
	require.Equal(t, stillPending.EventID(), res.Events[0].EventID())
}
