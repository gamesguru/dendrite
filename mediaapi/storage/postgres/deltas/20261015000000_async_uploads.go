// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

// UpAsyncUploads adds the unused_expires_ts column for media IDs created with
// POST /create but not uploaded yet (MSC2246), and a partial index over those
// pending rows for the per-user limit and the expiry sweep.
func UpAsyncUploads(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE mediaapi_media_repository ADD COLUMN IF NOT EXISTS unused_expires_ts BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS mediaapi_media_repository_pending_idx ON mediaapi_media_repository (user_id, unused_expires_ts) WHERE unused_expires_ts > 0;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	return nil
}

func DownAsyncUploads(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS mediaapi_media_repository_pending_idx;
DELETE FROM mediaapi_media_repository WHERE unused_expires_ts > 0;
ALTER TABLE mediaapi_media_repository DROP COLUMN IF EXISTS unused_expires_ts;`)
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	return nil
}
