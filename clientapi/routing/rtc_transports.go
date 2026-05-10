// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"

	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// RTCTransportsResponse represents the response for /rtc/transports.
type RTCTransportsResponse struct {
	RTCTransports []config.RTCFocus `json:"rtc_transports"`
}

// GetRTCTransports returns the list of MatrixRTC transports (MSC4143).
func GetRTCTransports(rtcFoci []config.RTCFocus) util.JSONResponse {
	transports := rtcFoci
	if transports == nil {
		transports = []config.RTCFocus{}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: RTCTransportsResponse{RTCTransports: transports},
	}
}
