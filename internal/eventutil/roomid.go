// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package eventutil

import (
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

// RoomIDDomain returns the server named in a room ID, or "" for the
// domainless room IDs of room version 12 and later. Use it instead of
// spec.RoomID.Domain, which panics on a domainless room ID.
func RoomIDDomain(roomID spec.RoomID) spec.ServerName {
	if !strings.Contains(roomID.String(), ":") {
		return ""
	}
	return roomID.Domain() //nolint:forbidigo // guarded above
}
