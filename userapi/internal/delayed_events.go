// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"encoding/json"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

func (a *UserInternalAPI) PerformDelayedEventAdd(ctx context.Context, event *api.DelayedEvent, maxScheduled int) error {
	return a.DB.AddDelayedEvent(ctx, event, maxScheduled)
}

func (a *UserInternalAPI) PerformDelayedEventRestart(ctx context.Context, delayID string, now spec.Timestamp) (bool, error) {
	return a.DB.RestartDelayedEvent(ctx, delayID, now)
}

func (a *UserInternalAPI) PerformDelayedEventCancel(ctx context.Context, delayID string, now spec.Timestamp) (bool, error) {
	return a.DB.CancelDelayedEvent(ctx, delayID, now)
}

func (a *UserInternalAPI) PerformDelayedEventClaim(ctx context.Context, delayID string) (*api.DelayedEvent, error) {
	return a.DB.ClaimDelayedEvent(ctx, delayID)
}

func (a *UserInternalAPI) PerformDelayedEventsClaimDue(ctx context.Context, now spec.Timestamp, reclaim bool) ([]api.DelayedEvent, error) {
	return a.DB.ClaimDueDelayedEvents(ctx, now, reclaim)
}

func (a *UserInternalAPI) PerformDelayedEventUnclaim(ctx context.Context, delayID string) error {
	return a.DB.UnclaimDelayedEvent(ctx, delayID)
}

func (a *UserInternalAPI) PerformDelayedEventFinalize(ctx context.Context, delayID, eventID string, sendErr json.RawMessage, now spec.Timestamp) error {
	return a.DB.FinalizeDelayedEvent(ctx, delayID, eventID, sendErr, now)
}

func (a *UserInternalAPI) PerformDelayedEventsPurge(ctx context.Context, before spec.Timestamp) error {
	return a.DB.PurgeDelayedEvents(ctx, before)
}

func (a *UserInternalAPI) QueryDelayedEvent(ctx context.Context, delayID string) (*api.DelayedEvent, error) {
	return a.DB.GetDelayedEvent(ctx, delayID)
}

func (a *UserInternalAPI) QueryDelayedEvents(ctx context.Context, localpart string, serverName spec.ServerName, finalized bool) ([]api.DelayedEvent, error) {
	return a.DB.GetDelayedEventsForUser(ctx, localpart, serverName, finalized)
}

func (a *UserInternalAPI) QueryNextDelayedEventSendTS(ctx context.Context) (spec.Timestamp, bool, error) {
	return a.DB.NextDelayedEventSendTS(ctx)
}
