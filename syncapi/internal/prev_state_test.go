package internal

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// stateBefore answers QueryStateAfterEvents from a table keyed by the first
// prev event ID, so each test decides exactly what the roomserver knows.
type stateBefore struct {
	answers map[string]rsapi.QueryStateAfterEventsResponse
}

func (s *stateBefore) QueryStateAfterEvents(
	_ context.Context, req *rsapi.QueryStateAfterEventsRequest, res *rsapi.QueryStateAfterEventsResponse,
) error {
	if answer, ok := s.answers[req.PrevEventIDs[0]]; ok {
		*res = answer
	}
	return nil
}

// repairDB is an in-memory room, in row id order.
type repairDB struct {
	rows         []types.StreamEvent
	updates      int
	refuseUpdate bool
}

func (d *repairDB) SelectRoomEventsAfterID(
	_ context.Context, _ string, afterID types.StreamPosition, limit int,
) ([]types.StreamEvent, error) {
	var out []types.StreamEvent
	for _, row := range d.rows {
		if row.StreamPosition > afterID && len(out) < limit {
			out = append(out, row)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].StreamPosition < out[j].StreamPosition })
	return out, nil
}

// Events returns a detached copy of the stored event, so a repair only changes
// what it persists through UpdateUnredactedEventJSON.
func (d *repairDB) Events(_ context.Context, ids []string) ([]*rstypes.HeaderedEvent, error) {
	var out []*rstypes.HeaderedEvent
	for _, row := range d.rows {
		if row.HeaderedEvent != nil && row.EventID() == ids[0] {
			eventJSON, err := row.MarshalJSON()
			if err != nil {
				return nil, err
			}
			var event rstypes.HeaderedEvent
			if err = event.UnmarshalJSON(eventJSON); err != nil {
				return nil, err
			}
			out = append(out, &event)
		}
	}
	return out, nil
}

// UpdateUnredactedEventJSON persists the event it is given, unless refuseUpdate
// simulates a row that changed since the read.
func (d *repairDB) UpdateUnredactedEventJSON(_ context.Context, eventID string, _ []byte, event *rstypes.HeaderedEvent) (bool, error) {
	d.updates++
	if d.refuseUpdate {
		return false, nil
	}
	for i := range d.rows {
		if d.rows[i].HeaderedEvent != nil && d.rows[i].EventID() == eventID {
			d.rows[i].HeaderedEvent = event
		}
	}
	return true, nil
}

// stored returns the persisted copy of an event.
func (d *repairDB) stored(t *testing.T, eventID string) *rstypes.HeaderedEvent {
	t.Helper()
	for _, row := range d.rows {
		if row.HeaderedEvent != nil && row.EventID() == eventID {
			return row.HeaderedEvent
		}
	}
	t.Fatalf("event %s not stored", eventID)
	return nil
}

func found(prev ...*rstypes.HeaderedEvent) rsapi.QueryStateAfterEventsResponse {
	return rsapi.QueryStateAfterEventsResponse{RoomExists: true, PrevEventsExist: true, StateEvents: prev}
}

// profileChange builds a room where Alice's join is replaced by a join with a
// new display name, plus a plain message that must be ignored.
func profileChange(t *testing.T) (oldJoin, newJoin *rstypes.HeaderedEvent, roomID string, q *stateBefore, db *repairDB) {
	t.Helper()
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for _, ev := range room.Events() {
		if ev.Type() == "m.room.member" && ev.StateKey() != nil && *ev.StateKey() == alice.ID {
			oldJoin = ev
		}
	}
	require.NotNil(t, oldJoin)
	newJoin = room.CreateEvent(t, alice, "m.room.member",
		map[string]any{"membership": "join", "displayname": "new name"},
		test.WithStateKey(alice.ID), test.WithPrevEvents(oldJoin.EventID()))
	message := room.CreateEvent(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "hi"},
		test.WithPrevEvents(newJoin.EventID()))

	q = &stateBefore{answers: map[string]rsapi.QueryStateAfterEventsResponse{
		oldJoin.EventID(): found(oldJoin),
	}}
	db = &repairDB{rows: []types.StreamEvent{
		{HeaderedEvent: newJoin, StreamPosition: 10, ExcludeFromSync: true},
		{HeaderedEvent: message, StreamPosition: 11, ExcludeFromSync: true},
	}}
	return oldJoin, newJoin, room.ID, q, db
}

func TestRepairRoomPrevStateFillsProfileChange(t *testing.T) {
	oldJoin, newJoin, roomID, q, db := profileChange(t)

	report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 2, report.Scanned)
	assert.Equal(t, 1, report.StateEvents)
	assert.Equal(t, 1, report.Repairable)
	assert.Equal(t, 1, report.Repaired)
	assert.Equal(t, 0, report.Errors)

	unsigned := gjson.ParseBytes(db.stored(t, newJoin.EventID()).Unsigned())
	assert.Equal(t, "join", unsigned.Get("prev_content.membership").Str)
	assert.Equal(t, oldJoin.EventID(), unsigned.Get("replaces_state").Str)
	assert.Equal(t, string(oldJoin.SenderID()), unsigned.Get("prev_sender").Str)
	assert.Equal(t, gjson.GetBytes(oldJoin.Content(), "displayname").Str, unsigned.Get("prev_content.displayname").Str)
}

func TestRepairRoomPrevStateDryRunWritesNothing(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)

	report, err := RepairRoomPrevState(context.Background(), db, q, roomID, false)
	require.NoError(t, err)

	assert.Equal(t, 1, report.Repairable)
	assert.Equal(t, 0, report.Repaired)
	require.Len(t, report.Samples, 1)
	assert.Equal(t, newJoin.EventID(), report.Samples[0].EventID)
	assert.Zero(t, db.updates)
	assert.False(t, HasPrevContent(newJoin))
}

func TestRepairRoomPrevStateKeepsOtherUnsignedKeys(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)
	require.NoError(t, newJoin.SetUnsignedField("age", 42))
	require.NoError(t, newJoin.SetUnsignedField("redacted_by", "$other"))

	_, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	unsigned := gjson.ParseBytes(db.stored(t, newJoin.EventID()).Unsigned())
	assert.Equal(t, int64(42), unsigned.Get("age").Int())
	assert.Equal(t, "$other", unsigned.Get("redacted_by").Str)
	assert.True(t, unsigned.Get("prev_content").Exists())
}

func TestRepairRoomPrevStateSkipsRedactedEvents(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)
	require.NoError(t, newJoin.SetUnsignedField("redacted_because", map[string]any{"type": "m.room.redaction"}))

	report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 1, report.Redacted)
	assert.Equal(t, 0, report.Repaired)
	assert.False(t, HasPrevContent(newJoin))
}

// A redaction that lands after the repair re-reads the event but before the
// conditional write must win: the update is refused and the event is skipped.
func TestRepairRoomPrevStateSkipsWriteWhenJSONChangedMeanwhile(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)
	db.refuseUpdate = true

	report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 1, report.WriteSkipped)
	assert.Equal(t, 0, report.Repaired)
	assert.Equal(t, 0, report.Repairable)
	assert.Equal(t, 1, db.updates)
	assert.False(t, HasPrevContent(newJoin), "a refused write must leave the stored event unchanged")
}

func TestRepairRoomPrevStateIsIdempotentAndNeverOverwrites(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)

	_, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)
	before := string(db.stored(t, newJoin.EventID()).Unsigned())

	second, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 1, second.AlreadyHad)
	assert.Equal(t, 0, second.Repaired)
	assert.Equal(t, before, string(db.stored(t, newJoin.EventID()).Unsigned()))
	assert.Equal(t, 1, db.updates)
}

func TestRepairRoomPrevStateLeavesEventsWithoutAnAnswer(t *testing.T) {
	t.Run("no predecessor", func(t *testing.T) {
		oldJoin, newJoin, roomID, q, db := profileChange(t)
		// Roomserver answers but holds no earlier event for the key.
		q.answers[oldJoin.EventID()] = rsapi.QueryStateAfterEventsResponse{RoomExists: true, PrevEventsExist: true}

		report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
		require.NoError(t, err)

		assert.Equal(t, 1, report.NoPredecessor)
		assert.Equal(t, 0, report.Repaired)
		assert.False(t, HasPrevContent(newJoin))
	})
	t.Run("prev events unknown to the roomserver", func(t *testing.T) {
		oldJoin, newJoin, roomID, q, db := profileChange(t)
		q.answers[oldJoin.EventID()] = rsapi.QueryStateAfterEventsResponse{RoomExists: true}

		report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
		require.NoError(t, err)

		assert.Equal(t, 1, report.LookupUnavailable)
		assert.Equal(t, 0, report.Repaired)
		assert.False(t, HasPrevContent(newJoin))
	})
	t.Run("state may be incomplete", func(t *testing.T) {
		oldJoin, newJoin, roomID, q, db := profileChange(t)
		// The predecessor is present, but the room keeps snapshots from a
		// partial-state period. A present entry was resolved over a partial set,
		// so it is not proof of the true predecessor; the answer stays
		// unavailable rather than being written as prev_content.
		q.answers[oldJoin.EventID()] = rsapi.QueryStateAfterEventsResponse{
			RoomExists: true, PrevEventsExist: true, StateIncomplete: true,
			StateEvents: []*rstypes.HeaderedEvent{oldJoin},
		}

		report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
		require.NoError(t, err)

		assert.Equal(t, 1, report.LookupUnavailable)
		assert.Equal(t, 0, report.Repaired)
		assert.False(t, HasPrevContent(newJoin))
	})
	t.Run("the event itself is not its predecessor", func(t *testing.T) {
		oldJoin, newJoin, roomID, q, db := profileChange(t)
		q.answers[oldJoin.EventID()] = found(newJoin)

		report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
		require.NoError(t, err)

		assert.Equal(t, 1, report.NoPredecessor)
		assert.False(t, HasPrevContent(newJoin))
	})
}

func TestRepairRoomPrevStateKeepsExistingPrevContent(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)
	require.NoError(t, newJoin.SetUnsignedField("prev_content", map[string]any{"membership": "invite"}))

	report, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 1, report.AlreadyHad)
	assert.Equal(t, 0, report.Repaired)
	assert.Equal(t, "invite", gjson.GetBytes(newJoin.Unsigned(), "prev_content.membership").Str)
}
