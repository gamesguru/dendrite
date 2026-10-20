package receipts

import (
	"context"
	"errors"

	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// EventQuerier retrieves events for receipt validation and notification classification.
type EventQuerier interface {
	QueryEventsByID(context.Context, *api.QueryEventsByIDRequest, *api.QueryEventsByIDResponse) error
}

// ErrUnknownEvent means the acknowledged event is unavailable in the requested room.
var ErrUnknownEvent = errors.New("receipt event is not in the room")

// ThreadID resolves an event's thread through at most three relationship hops.
// Thread roots and non-thread relations to roots belong to the main timeline.
func ThreadID(ctx context.Context, query EventQuerier, roomID, eventID string) (string, error) {
	event, err := queryEvent(ctx, query, roomID, eventID)
	if err != nil {
		return "", err
	}
	return EventThreadID(ctx, query, event)
}

// EventThreadID classifies an event already available to the caller.
func EventThreadID(ctx context.Context, query EventQuerier, event *types.HeaderedEvent) (string, error) {
	roomID := event.RoomID().String()
	seen := make(map[string]bool)
	for range 3 {
		if seen[event.EventID()] {
			break
		}
		seen[event.EventID()] = true
		relation := gjson.GetBytes(event.Content(), "m\\.relates_to")
		parentID := relation.Get("event_id").Str
		if parentID == "" {
			break
		}
		if relation.Get("rel_type").Str == "m.thread" {
			return parentID, nil
		}
		parent, err := queryEvent(ctx, query, roomID, parentID)
		if errors.Is(err, ErrUnknownEvent) {
			break
		}
		if err != nil {
			return "", err
		}
		event = parent
	}
	return "main", nil
}

func queryEvent(ctx context.Context, query EventQuerier, roomID, eventID string) (*types.HeaderedEvent, error) {
	var res api.QueryEventsByIDResponse
	if err := query.QueryEventsByID(ctx, &api.QueryEventsByIDRequest{RoomID: roomID, EventIDs: []string{eventID}, ExcludeRejected: true}, &res); err != nil {
		return nil, err
	}
	for _, event := range res.Events {
		if event.EventID() == eventID && event.RoomID().String() == roomID {
			return event, nil
		}
	}
	return nil, ErrUnknownEvent
}
