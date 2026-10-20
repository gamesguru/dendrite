// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables

import (
	"context"
	"database/sql"
)

// AddAccountLocked adds the lock flag to accounts (MSC3939). A locked account
// keeps its sessions, but every request apart from logout is refused until an
// administrator unlocks it.
func AddAccountLocked(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE userapi_accounts ADD COLUMN locked BOOLEAN NOT NULL DEFAULT FALSE;`)
	return err
}

// AddAccountSuspended adds the suspension flag to accounts (MSC3823). A
// suspended account can still read, but not join rooms, send messages or
// change its profile.
func AddAccountSuspended(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE userapi_accounts ADD COLUMN suspended BOOLEAN NOT NULL DEFAULT FALSE;`)
	return err
}
