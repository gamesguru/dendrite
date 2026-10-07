// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables

import (
	"context"
	"database/sql"
)

// AddDelayedEventStickyDuration stores the MSC4354 sticky duration of a
// delayed event, so that it is sent as a sticky event. NULL means the event
// is not sticky.
func AddDelayedEventStickyDuration(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE userapi_delayed_events ADD COLUMN sticky_duration_ms BIGINT;`)
	return err
}
