package shared

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

type historicalQuerierFunc func(context.Context, *api.QueryEventsByIDRequest, *api.QueryEventsByIDResponse) error

func (f historicalQuerierFunc) QueryEventsByID(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
	return f(ctx, req, res)
}

func TestHistoricalStateValidation(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	otherRoom := test.NewRoom(t, alice)
	wanted := room.CreateEvent(t, alice, "m.room.name", map[string]string{"name": "historical"}, test.WithStateKey(""))
	unrequested := room.CreateEvent(t, alice, "m.room.name", map[string]string{"name": "current"}, test.WithStateKey(""))
	wrongRoom := otherRoom.CreateEvent(t, alice, "m.room.name", map[string]string{"name": "other room"}, test.WithStateKey(""))
	message := room.CreateEvent(t, alice, "m.room.message", map[string]string{"body": "not state"})
	query := historicalQuerierFunc(func(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
		require.Equal(t, room.ID, req.RoomID)
		require.True(t, req.ExcludeRejected)
		res.Events = []*rstypes.HeaderedEvent{nil, {}, wrongRoom, unrequested, message, wanted, wanted}
		return nil
	})
	events, err := fetchHistoricalStateEvents(t.Context(), query, map[string][]string{room.ID: {wanted.EventID(), wrongRoom.EventID(), message.EventID(), "$absent"}})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, wanted.EventID(), events[0].EventID())
	require.Zero(t, events[0].StreamPosition)
}

func TestHistoricalStateBatching(t *testing.T) {
	missing := map[string][]string{}
	for _, roomID := range []string{"!one:test", "!two:test"} {
		for i := 0; i < historicalStateBatchSize+1; i++ {
			missing[roomID] = append(missing[roomID], fmt.Sprintf("$%s-%d", roomID, i))
		}
	}
	calls := 0
	seen := map[string]bool{}
	query := historicalQuerierFunc(func(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
		calls++
		require.LessOrEqual(t, len(req.EventIDs), historicalStateBatchSize)
		require.True(t, req.ExcludeRejected)
		for _, id := range req.EventIDs {
			require.Contains(t, missing[req.RoomID], id)
			require.False(t, seen[id])
			seen[id] = true
		}
		return nil
	})
	events, err := fetchHistoricalStateEvents(t.Context(), query, missing)
	require.NoError(t, err)
	require.Empty(t, events)
	require.Equal(t, 4, calls)
	require.Len(t, seen, 2*(historicalStateBatchSize+1))
}

func TestHistoricalStateErrors(t *testing.T) {
	sentinel := errors.New("roomserver unavailable")
	query := historicalQuerierFunc(func(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
		return sentinel
	})
	missing := map[string][]string{"!room:test": {"$one"}}
	_, err := fetchHistoricalStateEvents(t.Context(), query, missing)
	require.ErrorIs(t, err, sentinel)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)
	_, err = fetchHistoricalStateEvents(ctx, query, missing)
	require.ErrorIs(t, err, context.Canceled)
	// A cancellation during the last query must not yield a partial success.
	ctx, cancel = context.WithCancelCause(t.Context())
	defer cancel(nil)
	query = historicalQuerierFunc(func(ctx context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
		cancel(context.Canceled)
		return nil
	})
	_, err = fetchHistoricalStateEvents(ctx, query, missing)
	require.ErrorIs(t, err, context.Canceled)
}
