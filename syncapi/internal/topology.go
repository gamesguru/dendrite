// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
)

// membershipWaitTimeout bounds how long a request waits for the sync API to
// catch up with a membership event the roomserver has already accepted.
const membershipWaitTimeout = 3 * time.Second

// WaitForMembershipInTopology polls until the sync API has the given membership
// event in its topology, or the timeout or context ends. It never fails the
// request: an event that doesn't arrive just gets the old behaviour.
func WaitForMembershipInTopology(ctx context.Context, db storage.Database, eventID string) {
	if eventID == "" || db == nil {
		return
	}
	deadline := time.NewTimer(membershipWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := db.NewDatabaseSnapshot(ctx)
		if err != nil {
			return
		}
		_, err = snapshot.EventPositionInTopology(ctx, eventID)
		_ = snapshot.Rollback()
		if !errors.Is(err, sql.ErrNoRows) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}
