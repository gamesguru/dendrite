// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package depth

// MaxDepth is the maximum depth value allowed for Matrix events.
// This corresponds to the canonical JSON integer limit (2^53 - 1),
// which is JavaScript's Number.MAX_SAFE_INTEGER.
// Events with depth values exceeding this cannot be serialised to valid
// canonical JSON and will be rejected by compliant servers.
const MaxDepth int64 = 9007199254740991

// Clamp ensures a depth value does not exceed MaxDepth.
// This is used when calculating new event depths to prevent overflow
// beyond the canonical JSON integer limit.
func Clamp(d int64) int64 {
	if d > MaxDepth {
		return MaxDepth
	}
	if d < 0 {
		return 0
	}
	return d
}
