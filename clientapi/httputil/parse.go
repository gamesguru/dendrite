// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// ParseTSParam parses the event time from the ts query parameter. Only
// application services may set it (MSC3316); for other requests, or if it is
// absent, the current time is returned.
func ParseTSParam(req *http.Request, device *userapi.Device) (time.Time, error) {
	tsStr := req.URL.Query().Get("ts")
	if tsStr == "" || device == nil || device.AppserviceID == "" {
		return time.Now(), nil
	}

	// The parameter exists, parse into a Time object
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("param 'ts' is no valid int (%s)", err.Error())
	}

	return time.UnixMilli(ts), nil
}
