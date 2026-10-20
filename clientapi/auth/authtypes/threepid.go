// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package authtypes

import (
	"strings"

	"golang.org/x/text/cases"
)

// ThreePID represents a third-party identifier.
type ThreePID struct {
	Address     string `json:"address"`
	Medium      string `json:"medium"`
	AddedAt     int64  `json:"added_at"`
	ValidatedAt int64  `json:"validated_at"`
}

// CanonicalThreePIDAddress returns the canonical form of a 3PID address. Email
// addresses are trimmed and case-folded, so that `Strauß@Example.com` becomes
// `strauss@example.com`, as the spec requires since MSC2265. Other media are
// returned unchanged.
func CanonicalThreePIDAddress(medium, address string) string {
	if medium != "email" {
		return address
	}
	return cases.Fold().String(strings.TrimSpace(address))
}
