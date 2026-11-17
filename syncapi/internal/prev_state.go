package internal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// PrevStateOutcome says what LookupPrevState learned about a state event's
// predecessor.
type PrevStateOutcome int

const (
	// PrevStateFound means the state before the event held an earlier event for
	// the same (type, state key).
	PrevStateFound PrevStateOutcome = iota
	// PrevStateNone means the roomserver answered but returned no earlier event
	// for the key. This is both a genuine first event for the key and a state
	// the roomserver could not load: the query reports the two identically, so
	// callers must not write anything.
	PrevStateNone
	// PrevStateUnavailable means the roomserver does not know the room, does
	// not hold the event's prev events, or reports the room's state snapshots
	// as incomplete (they may have been built during a partial-state join). In
	// every case nothing reliable can be said about the state before the event.
	PrevStateUnavailable
)

// StateAfterQuerier is the part of the roomserver API used to read the state
// before an event.
type StateAfterQuerier interface {
	QueryStateAfterEvents(ctx context.Context, req *rsapi.QueryStateAfterEventsRequest, res *rsapi.QueryStateAfterEventsResponse) error
}

// LookupPrevState finds the state event that ev replaces, from the room state
// before ev (the state after ev's prev events), the way the live path does it
// from current state. A historical event must not use current state: the
// current state is usually newer than the event.
func LookupPrevState(
	ctx context.Context, q StateAfterQuerier, ev *rstypes.HeaderedEvent,
) (synctypes.PrevEventRef, PrevStateOutcome, error) {
	stateKey := ev.StateKey()
	prevEventIDs := ev.PrevEventIDs()
	if stateKey == nil || len(prevEventIDs) == 0 {
		return synctypes.PrevEventRef{}, PrevStateNone, nil
	}
	req := &rsapi.QueryStateAfterEventsRequest{
		RoomID:       ev.RoomID().String(),
		PrevEventIDs: prevEventIDs,
		// The raw state key, which is what the roomserver stores.
		StateToFetch: []gomatrixserverlib.StateKeyTuple{{EventType: ev.Type(), StateKey: *stateKey}},
	}
	res := &rsapi.QueryStateAfterEventsResponse{}
	if err := q.QueryStateAfterEvents(ctx, req, res); err != nil {
		return synctypes.PrevEventRef{}, PrevStateUnavailable, fmt.Errorf("QueryStateAfterEvents: %w", err)
	}
	if !res.RoomExists || !res.PrevEventsExist || res.StateIncomplete {
		// Incomplete state may omit the true predecessor, and an entry it does
		// contain was resolved over a partial set, so it can differ from the
		// full-state winner (for example an event accepted provisionally during
		// the partial-state period). Treat the answer as unavailable rather than
		// absent: do not persist prev_content from a snapshot that is not
		// provably complete.
		return synctypes.PrevEventRef{}, PrevStateUnavailable, nil
	}
	for _, prev := range res.StateEvents {
		if prev.Type() != ev.Type() || prev.StateKey() == nil || *prev.StateKey() != *stateKey {
			continue
		}
		if prev.EventID() == ev.EventID() {
			continue
		}
		return synctypes.PrevEventRef{
			PrevContent:   prev.Content(),
			ReplacesState: prev.EventID(),
			PrevSenderID:  string(prev.SenderID()),
		}, PrevStateFound, nil
	}
	return synctypes.PrevEventRef{}, PrevStateNone, nil
}

// HasPrevContent reports whether ev already carries unsigned.prev_content.
func HasPrevContent(ev *rstypes.HeaderedEvent) bool {
	return gjson.GetBytes(ev.Unsigned(), "prev_content").Exists()
}

// IsRedacted reports whether ev has been redacted, either by the redacted flag
// on the event or by an unsigned.redacted_because marker. A redacted state event
// has nothing left to describe, so it is not annotated.
func IsRedacted(ev *rstypes.HeaderedEvent) bool {
	return ev.Redacted() || gjson.GetBytes(ev.Unsigned(), "redacted_because").Exists()
}

// ApplyPrevState sets unsigned.prev_content, prev_sender and replaces_state
// on ev. It sets the three keys one at a time and leaves every other unsigned
// key, such as redacted_by and redacted_because, as it is.
func ApplyPrevState(ev *rstypes.HeaderedEvent, prev synctypes.PrevEventRef) error {
	if !gjson.GetBytes(ev.Unsigned(), "prev_content").Exists() {
		if err := ev.SetUnsignedField("prev_content", prev.PrevContent); err != nil {
			return fmt.Errorf("prev_content: %w", err)
		}
	}
	if !gjson.GetBytes(ev.Unsigned(), "prev_sender").Exists() {
		if err := ev.SetUnsignedField("prev_sender", prev.PrevSenderID); err != nil {
			return fmt.Errorf("prev_sender: %w", err)
		}
	}
	if !gjson.GetBytes(ev.Unsigned(), "replaces_state").Exists() {
		if err := ev.SetUnsignedField("replaces_state", prev.ReplacesState); err != nil {
			return fmt.Errorf("replaces_state: %w", err)
		}
	}
	return nil
}

// SetHistoricalPrevState sets unsigned.prev_content, prev_sender and
// replaces_state on a backfilled state event from the state before it. It does
// nothing for other events, for events that already have prev_content, for
// redacted events, and when the predecessor cannot be found; a lookup error is
// logged and ignored, so the event is stored without prev_content.
func SetHistoricalPrevState(ctx context.Context, q StateAfterQuerier, ev *rstypes.HeaderedEvent) {
	if ev.StateKey() == nil || HasPrevContent(ev) || IsRedacted(ev) {
		return
	}
	fields := logrus.Fields{"room_id": ev.RoomID().String(), "event_id": ev.EventID()}
	prev, outcome, err := LookupPrevState(ctx, q, ev)
	if err != nil {
		logrus.WithFields(fields).WithError(err).Warn("Failed to look up the state before a backfilled event; storing it without prev_content")
		return
	}
	if outcome != PrevStateFound {
		return
	}
	if err = ApplyPrevState(ev, prev); err != nil {
		logrus.WithFields(fields).WithError(err).Warn("Failed to set prev_content on a backfilled event")
	}
}

// PrevStateRepairDatabase is the part of the sync API database the repair
// reads and writes.
type PrevStateRepairDatabase interface {
	SelectRoomEventsAfterID(ctx context.Context, roomID string, afterID types.StreamPosition, limit int) ([]types.StreamEvent, error)
	Events(ctx context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error)
	// UpdateUnredactedEventJSON writes only if the stored JSON still matches expectedJSON
	// and the event is not redacted.
	UpdateUnredactedEventJSON(ctx context.Context, eventID string, expectedJSON []byte, event *rstypes.HeaderedEvent) (bool, error)
}

// PrevStateRepairSample is one event the repair would change, or changed.
type PrevStateRepairSample struct {
	EventID       string `json:"event_id"`
	Type          string `json:"type"`
	StateKey      string `json:"state_key"`
	ReplacesState string `json:"replaces_state"`
}

// PrevStateRepairReport is the outcome of RepairRoomPrevState.
type PrevStateRepairReport struct {
	RoomID string `json:"room_id"`
	Apply  bool   `json:"apply"`
	// Scanned is every stored event of the room that was looked at.
	Scanned int `json:"scanned"`
	// StateEvents is how many of them are state events.
	StateEvents int `json:"state_events"`
	// Redacted is how many state events were skipped because they are redacted.
	Redacted int `json:"redacted"`
	// AlreadyHad is how many state events already had prev_content.
	AlreadyHad int `json:"already_had_prev_content"`
	// Repairable is how many events have a predecessor in the state before them.
	// With Apply set they were written.
	Repairable int `json:"repairable"`
	// Repaired is how many events were written. Always zero for a dry run.
	Repaired int `json:"repaired"`
	// NoPredecessor is how many events have no earlier event for their key in
	// the state before them. It includes first events for a key and states the
	// roomserver could not load.
	NoPredecessor int `json:"no_predecessor"`
	// LookupUnavailable is how many events the roomserver cannot answer for.
	LookupUnavailable int `json:"lookup_unavailable"`
	// WriteSkipped counts apply writes skipped because the stored JSON changed
	// after it was read by the repair.
	WriteSkipped int `json:"write_skipped"`
	// Errors is how many events failed to look up or to write.
	Errors  int                     `json:"errors"`
	Samples []PrevStateRepairSample `json:"samples,omitempty"`
}

const (
	prevStateRepairBatch   = 500
	prevStateRepairSamples = 25
)

// RepairRoomPrevState fills unsigned.prev_content, prev_sender and
// replaces_state on the state events of a room that lack them, from the state
// before each event. It never overwrites an existing prev_content and never
// touches any other unsigned key. With apply false it only counts.
func RepairRoomPrevState(
	ctx context.Context, db PrevStateRepairDatabase, q StateAfterQuerier, roomID string, apply bool,
) (*PrevStateRepairReport, error) {
	report := &PrevStateRepairReport{RoomID: roomID, Apply: apply}
	var after types.StreamPosition
	for {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		batch, err := db.SelectRoomEventsAfterID(ctx, roomID, after, prevStateRepairBatch)
		if err != nil {
			return report, fmt.Errorf("SelectRoomEventsAfterID: %w", err)
		}
		if len(batch) == 0 {
			return report, nil
		}
		for _, stored := range batch {
			after = stored.StreamPosition
			report.Scanned++
			ev := stored.HeaderedEvent
			if ev == nil || ev.StateKey() == nil {
				continue
			}
			report.StateEvents++
			if IsRedacted(ev) {
				report.Redacted++
				continue
			}
			if HasPrevContent(ev) {
				report.AlreadyHad++
				continue
			}
			prev, outcome, err := LookupPrevState(ctx, q, ev)
			if err != nil {
				report.Errors++
				continue
			}
			switch outcome {
			case PrevStateNone:
				report.NoPredecessor++
				continue
			case PrevStateUnavailable:
				report.LookupUnavailable++
				continue
			}
			sample := PrevStateRepairSample{
				EventID: ev.EventID(), Type: ev.Type(), StateKey: *ev.StateKey(), ReplacesState: prev.ReplacesState,
			}
			if apply {
				// The scan may be stale. Re-read the event and make the write
				// conditional on that exact JSON still being stored, so concurrent
				// redactions or other event rewrites are not overwritten.
				latest, lerr := db.Events(ctx, []string{ev.EventID()})
				if lerr != nil || len(latest) != 1 || latest[0] == nil {
					report.Errors++
					continue
				}
				ev = latest[0]
				if IsRedacted(ev) || HasPrevContent(ev) {
					continue
				}
				expectedJSON, lerr := json.Marshal(ev)
				if lerr != nil {
					report.Errors++
					continue
				}
				if err = ApplyPrevState(ev, prev); err != nil {
					report.Errors++
					continue
				}
				// The write fails if the row was redacted or changed since the
				// re-read, so neither a redaction nor another rewrite is
				// overwritten.
				written, werr := db.UpdateUnredactedEventJSON(ctx, ev.EventID(), expectedJSON, ev)
				if werr != nil {
					report.Errors++
					continue
				}
				if !written {
					report.WriteSkipped++
					continue
				}
				report.Repaired++
			}
			report.Repairable++
			if len(report.Samples) < prevStateRepairSamples {
				report.Samples = append(report.Samples, sample)
			}
		}
	}
}
