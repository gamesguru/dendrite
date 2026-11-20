// Copyright 2024 New Vector Ltd.
// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package types

import (
	"database/sql"
	"fmt"
)

// ErrNoStateSnapshot reports that an event has no before-state snapshot yet.
// It wraps sql.ErrNoRows, so callers that already treat a missing row as "no
// state" keep matching it, while callers that must tell an expected missing
// snapshot apart from a database fault can match this value instead.
var ErrNoStateSnapshot = fmt.Errorf("event has no before-state snapshot: %w", sql.ErrNoRows)
