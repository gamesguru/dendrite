// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables

import (
	"context"
	"database/sql"
)

// AddDeviceRefreshTokens adds access token expiry and refresh tokens to devices
// (MSC2918). Only SHA-256 hashes of refresh tokens are stored. The previous
// refresh token stays valid until the device uses its new access token, so a
// client that lost a refresh response can repeat the refresh.
func AddDeviceRefreshTokens(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE userapi_devices ADD COLUMN access_token_expires_ts BIGINT NOT NULL DEFAULT 0;
	ALTER TABLE userapi_devices ADD COLUMN refresh_token_hash TEXT;
	ALTER TABLE userapi_devices ADD COLUMN prev_refresh_token_hash TEXT;
	CREATE UNIQUE INDEX userapi_devices_refresh_token_idx ON userapi_devices(refresh_token_hash);
	CREATE UNIQUE INDEX userapi_devices_prev_refresh_token_idx ON userapi_devices(prev_refresh_token_hash);`)
	return err
}
