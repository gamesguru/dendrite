// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package shared

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

// AddDelayedEvent stores a new scheduled delayed event, unless its owner
// already has maxScheduled pending, in which case it returns a
// *api.DelayedEventLimitError.
func (d *Database) AddDelayedEvent(ctx context.Context, event *api.DelayedEvent, maxScheduled int) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		// Every finalized event was scheduled first, so trimming them here
		// bounds what one user keeps by the retention cap plus their pending
		// events.
		if err := d.DelayedEvents.DeleteExcessFinalizedDelayedEvents(
			ctx, txn, event.Localpart, event.ServerName, api.DelayedEventsRetainedPerUser,
		); err != nil {
			return err
		}
		count, nextSendTS, err := d.DelayedEvents.CountPendingDelayedEvents(ctx, txn, event.Localpart, event.ServerName)
		if err != nil {
			return err
		}
		if count >= maxScheduled {
			return &api.DelayedEventLimitError{NextSendTS: nextSendTS}
		}
		return d.DelayedEvents.InsertDelayedEvent(ctx, txn, event)
	})
}

// RestartDelayedEvent restarts a scheduled event's delay from now.
func (d *Database) RestartDelayedEvent(ctx context.Context, delayID string, now spec.Timestamp) (restarted bool, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		restarted, err = d.DelayedEvents.RestartDelayedEvent(ctx, txn, delayID, now)
		return err
	})
	return restarted, err
}

// CancelDelayedEvent finalizes a scheduled event without an outcome.
func (d *Database) CancelDelayedEvent(ctx context.Context, delayID string, now spec.Timestamp) (canceled bool, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		canceled, err = d.DelayedEvents.FinalizeDelayedEvent(ctx, txn, delayID, api.DelayedEventScheduled, "", nil, now)
		return err
	})
	return canceled, err
}

// ClaimDelayedEvent claims a scheduled event for sending, or returns nil.
func (d *Database) ClaimDelayedEvent(ctx context.Context, delayID string) (event *api.DelayedEvent, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		event, err = d.DelayedEvents.ClaimDelayedEvent(ctx, txn, delayID)
		return err
	})
	return event, err
}

// ClaimDueDelayedEvents claims the scheduled events due at now and, with
// reclaim, returns those that are already claimed.
func (d *Database) ClaimDueDelayedEvents(ctx context.Context, now spec.Timestamp, reclaim bool) (events []api.DelayedEvent, err error) {
	err = d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		events, err = d.DelayedEvents.ClaimDueDelayedEvents(ctx, txn, now, reclaim)
		return err
	})
	return events, err
}

// UnclaimDelayedEvent returns a claimed event to the schedule.
func (d *Database) UnclaimDelayedEvent(ctx context.Context, delayID string) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.DelayedEvents.UnclaimDelayedEvent(ctx, txn, delayID)
	})
}

// FinalizeDelayedEvent records the outcome of a claimed event.
func (d *Database) FinalizeDelayedEvent(ctx context.Context, delayID, eventID string, sendErr json.RawMessage, now spec.Timestamp) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		_, err := d.DelayedEvents.FinalizeDelayedEvent(ctx, txn, delayID, api.DelayedEventSending, eventID, sendErr, now)
		return err
	})
}

// CancelDelayedStateEvents cancels the scheduled events for one piece of
// state, except those of the given local user. An empty localpart cancels
// every user's.
func (d *Database) CancelDelayedStateEvents(
	ctx context.Context, roomID, eventType, stateKey, exceptLocalpart string,
	exceptServerName spec.ServerName, now spec.Timestamp,
) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.DelayedEvents.CancelDelayedStateEvents(ctx, txn, roomID, eventType, stateKey, exceptLocalpart, exceptServerName, now)
	})
}

// PurgeDelayedEvents forgets events finalized before the given time.
func (d *Database) PurgeDelayedEvents(ctx context.Context, before spec.Timestamp) error {
	return d.Writer.Do(d.DB, nil, func(txn *sql.Tx) error {
		return d.DelayedEvents.DeleteFinalizedDelayedEvents(ctx, txn, before)
	})
}

// GetDelayedEvent returns an event of any owner and status, or nil.
func (d *Database) GetDelayedEvent(ctx context.Context, delayID string) (*api.DelayedEvent, error) {
	return d.DelayedEvents.SelectDelayedEvent(ctx, nil, delayID)
}

// GetDelayedEventsForUser returns a user's finalized or pending events.
func (d *Database) GetDelayedEventsForUser(ctx context.Context, localpart string, serverName spec.ServerName, finalized bool) ([]api.DelayedEvent, error) {
	return d.DelayedEvents.SelectDelayedEventsForUser(ctx, nil, localpart, serverName, finalized)
}

// NextDelayedEventSendTS returns the earliest send time of any scheduled
// event, and false if none is scheduled.
func (d *Database) NextDelayedEventSendTS(ctx context.Context) (spec.Timestamp, bool, error) {
	return d.DelayedEvents.SelectNextSendTS(ctx, nil)
}
