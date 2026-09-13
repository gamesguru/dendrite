package consumers

import (
	"context"
	"fmt"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/fulltext"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
)

// RecoverSearch rebuilds a fresh index before sync consumers or routes start.
// PostgreSQL is authoritative; local index files are disposable derived state.
func RecoverSearch(ctx context.Context, db storage.Database, index fulltext.Indexer) error {
	var after int64
	replaced := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := db.ReIndex(ctx, 1000, after) //nolint:mnd
		if err != nil {
			return fmt.Errorf("read search recovery batch: %w", err)
		}
		if len(events) == 0 {
			// Apply edit removals after all pages, including backfilled originals.
			for eventID := range replaced {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := index.Delete(eventID); err != nil {
					return fmt.Errorf("remove replaced search document: %w", err)
				}
			}
			return nil
		}
		next := after
		elements := make([]fulltext.IndexElement, 0, len(events))
		for position, event := range events {
			// ReIndex returns a map, so iteration order cannot be used as a cursor.
			if position > next {
				next = position
			}
			element := fulltext.IndexElement{EventID: event.EventID(), RoomID: event.RoomID().String(), StreamPosition: position}
			element.SetContentType(event.Type())
			var key string
			switch event.Type() {
			case "m.room.message":
				key = "body"
				relation := gjson.GetBytes(event.Content(), "m\\.relates_to")
				if relation.Get("rel_type").String() == "m.replace" && gjson.GetBytes(event.Content(), key).String() != "" {
					if original := relation.Get("event_id").String(); original != "" {
						replaced[original] = struct{}{}
					}
				}
			case spec.MRoomName:
				key = "name"
			case spec.MRoomTopic:
				key = "topic"
			default:
				continue
			}
			element.Content = gjson.GetBytes(event.Content(), key).String()
			if strings.TrimSpace(element.Content) != "" {
				elements = append(elements, element)
			}
		}
		if next <= after {
			return fmt.Errorf("search recovery cursor did not advance")
		}
		if err = index.Index(elements...); err != nil {
			return fmt.Errorf("write search recovery batch: %w", err)
		}
		after = next
	}
}
