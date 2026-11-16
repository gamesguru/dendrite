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
	rows    []types.StreamEvent
	updates int
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

func (d *repairDB) UpdateEventJSON(_ context.Context, _ string, _ *rstypes.HeaderedEvent) error {
	// The rows hold the event pointers the repair mutates, so the write is the
	// mutation; counting calls is enough here.
	d.updates++
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

	unsigned := gjson.ParseBytes(newJoin.Unsigned())
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

	unsigned := gjson.ParseBytes(newJoin.Unsigned())
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

func TestRepairRoomPrevStateIsIdempotentAndNeverOverwrites(t *testing.T) {
	_, newJoin, roomID, q, db := profileChange(t)

	_, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)
	before := string(newJoin.Unsigned())

	second, err := RepairRoomPrevState(context.Background(), db, q, roomID, true)
	require.NoError(t, err)

	assert.Equal(t, 1, second.AlreadyHad)
	assert.Equal(t, 0, second.Repaired)
	assert.Equal(t, before, string(newJoin.Unsigned()))
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
