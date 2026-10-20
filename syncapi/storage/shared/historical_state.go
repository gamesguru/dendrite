package shared

import (
	"context"
	"fmt"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

type stateEventQuerier interface {
	QueryEventsByID(context.Context, *api.QueryEventsByIDRequest, *api.QueryEventsByIDResponse) error
}

const historicalStateBatchSize = 1000

// fetchHistoricalStateEvents recovers exact state references that have fallen
// out of sync's current state and were never timeline events. The roomserver
// retains their event JSON even after a newer state event replaces them.
func fetchHistoricalStateEvents(ctx context.Context, rsAPI stateEventQuerier, missing map[string][]string) ([]types.StreamEvent, error) {
	var events []types.StreamEvent
	if rsAPI == nil {
		return events, nil
	}
	for roomID, ids := range missing {
		for start := 0; start < len(ids); start += historicalStateBatchSize {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			batch := ids[start:min(start+historicalStateBatchSize, len(ids))]
			req := api.QueryEventsByIDRequest{RoomID: roomID, EventIDs: batch, ExcludeRejected: true}
			var res api.QueryEventsByIDResponse
			if err := rsAPI.QueryEventsByID(ctx, &req, &res); err != nil {
				return nil, fmt.Errorf("fetch historical sync state for room %s: %w", roomID, err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			wanted := make(map[string]bool, len(batch))
			for _, id := range batch {
				wanted[id] = true
			}
			for _, event := range res.Events {
				if event == nil || event.PDU == nil || event.StateKey() == nil || event.RoomID().String() != roomID || !wanted[event.EventID()] {
					continue
				}
				delete(wanted, event.EventID())
				// These events belong only to this response's state. Do not insert them
				// into the current state or timeline, or invent a new stream position.
				events = append(events, types.StreamEvent{HeaderedEvent: event})
			}
		}
	}
	return events, nil
}
