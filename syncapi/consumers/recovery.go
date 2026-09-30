package consumers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/fulltext"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
)

// searchRebuildRetry spaces out rebuild attempts after a failure.
var searchRebuildRetry = 5 * time.Second

// RebuildSearch rebuilds the index behind recovering while the homeserver
// already serves, then admits searches. The rebuild takes time in proportion
// to the searchable history, so serving must not wait for it: on a takeover
// that wait held every client request back, not just search (#451).
//
// A failed attempt is retried from the start, which rewrites the same
// documents, until it succeeds or ctx ends. Searches stay refused meanwhile,
// so a failure is visible and never serves partial results.
func RebuildSearch(ctx context.Context, db storage.Database, recovering *fulltext.Recovering) {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		err := RecoverSearch(ctx, db, recovering.Indexer)
		if err == nil {
			err = recovering.Finish()
		}
		if err == nil {
			logrus.WithField("duration", time.Since(start).Round(time.Millisecond)).Info("Search index rebuilt; search is available")
			return
		}
		if ctx.Err() != nil {
			return
		}
		logrus.WithError(err).WithField("attempt", attempt).Error("Failed to rebuild the search index; retrying")
		if !process.Sleep(ctx, searchRebuildRetry) {
			return
		}
	}
}

// RecoverSearch writes every searchable event in PostgreSQL to a fresh index.
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
