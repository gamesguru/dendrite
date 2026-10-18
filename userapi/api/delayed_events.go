// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
)

// DelayedEventsAPI stores MSC4140 delayed events. The client API schedules,
// manages and sends them; the user API keeps them and cancels them when the
// state they target changes or their owner is deactivated.
type DelayedEventsAPI interface {
	// PerformDelayedEventAdd stores a new scheduled delayed event. It fails
	// with a *DelayedEventLimitError when the owner already has maxScheduled
	// events scheduled.
	PerformDelayedEventAdd(ctx context.Context, event *DelayedEvent, maxScheduled int) error
	// PerformDelayedEventRestart moves a scheduled event's send time to now
	// plus its delay, and reports whether it was scheduled.
	PerformDelayedEventRestart(ctx context.Context, delayID string, now spec.Timestamp) (bool, error)
	// PerformDelayedEventCancel finalizes a scheduled event without sending
	// it, and reports whether it was scheduled.
	PerformDelayedEventCancel(ctx context.Context, delayID string, now spec.Timestamp) (bool, error)
	// PerformDelayedEventClaim claims a scheduled event for sending now. It
	// returns nil if the event is not scheduled.
	PerformDelayedEventClaim(ctx context.Context, delayID string) (*DelayedEvent, error)
	// PerformDelayedEventsClaimDue claims every scheduled event that is due at
	// now, ordered by send time. With reclaim, it also returns the events that
	// a previous process claimed but did not finalize.
	PerformDelayedEventsClaimDue(ctx context.Context, now spec.Timestamp, reclaim bool) ([]DelayedEvent, error)
	// PerformDelayedEventUnclaim returns a claimed event to the schedule.
	PerformDelayedEventUnclaim(ctx context.Context, delayID string) error
	// PerformDelayedEventFinalize records the outcome of a claimed event: the
	// ID of the event it was sent as, or the error that prevented sending.
	PerformDelayedEventFinalize(ctx context.Context, delayID, eventID string, sendErr json.RawMessage, now spec.Timestamp) error
	// PerformDelayedEventsPurge forgets events finalized before the given time.
	PerformDelayedEventsPurge(ctx context.Context, before spec.Timestamp) error
	// QueryDelayedEvent returns an event of any owner and status, or nil.
	QueryDelayedEvent(ctx context.Context, delayID string) (*DelayedEvent, error)
	// QueryDelayedEvents returns a user's finalized events, newest first, or
	// their pending (scheduled or claimed) events in order of send time.
	QueryDelayedEvents(ctx context.Context, localpart string, serverName spec.ServerName, finalized bool) ([]DelayedEvent, error)
	// QueryNextDelayedEventSendTS returns the earliest send time of any
	// scheduled event, and false if none is scheduled.
	QueryNextDelayedEventSendTS(ctx context.Context) (spec.Timestamp, bool, error)
}

// DelayedEventStatus is the stage of a delayed event's life.
type DelayedEventStatus string

const (
	// DelayedEventScheduled is waiting for its send time.
	DelayedEventScheduled DelayedEventStatus = "scheduled"
	// DelayedEventSending has been claimed by a send attempt that has not
	// recorded its outcome yet. Only one attempt can claim an event, and a
	// process that starts after a crash reclaims and resends what it left.
	DelayedEventSending DelayedEventStatus = "sending"
	// DelayedEventFinalized has been sent, canceled, or failed to send.
	DelayedEventFinalized DelayedEventStatus = "finalized"
)

const (
	// DelayedEventRetention is how long a finalized delayed event stays
	// available for lookup, as MSC4140 recommends.
	DelayedEventRetention = 7 * 24 * time.Hour
	// DelayedEventsRetainedPerUser caps the finalized delayed events kept for
	// one user, as MSC4140 recommends.
	DelayedEventsRetainedPerUser = 1000
)

// DelayedEvent is an event that a client scheduled to be sent into a room
// later (MSC4140).
type DelayedEvent struct {
	DelayID    string
	Localpart  string
	ServerName spec.ServerName
	// DeviceID is the device that scheduled the event, if any.
	DeviceID string
	RoomID   string
	Type     string
	// StateKey is nil for message events.
	StateKey *string
	// Content is the event content as the client sent it.
	Content json.RawMessage
	// OriginServerTS is an application service's massaged timestamp, or zero
	// to timestamp the event when it is sent.
	OriginServerTS spec.Timestamp
	DelayMS        int64
	// StickyDurationMS makes the event an MSC4354 sticky event for that many
	// milliseconds once it is sent. Nil for events that are not sticky.
	StickyDurationMS *int64
	// DelayedSince is when the event was scheduled or last restarted.
	DelayedSince spec.Timestamp
	SendTS       spec.Timestamp
	Status       DelayedEventStatus
	// EventID is set once the event has been sent.
	EventID string
	// Error is the standard error response that prevented sending.
	Error       json.RawMessage
	FinalizedTS spec.Timestamp
}

// DelayedEventLimitError reports that a user has scheduled as many delayed
// events as they may.
type DelayedEventLimitError struct {
	// NextSendTS is when the user's next delayed event is due to be sent.
	NextSendTS spec.Timestamp
}

func (e *DelayedEventLimitError) Error() string {
	return fmt.Sprintf("the maximum number of delayed events has been reached; the next is due at %d", e.NextSendTS)
}
