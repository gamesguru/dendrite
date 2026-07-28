// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib"
	"github.com/matrix-org/util"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/version"
)

// GetCapabilities returns information about the server's supported feature set
// and other relevant capabilities to an authenticated user. When oidcEnabled is
// true (MSC3861 delegated authentication), password and 3PID management are
// delegated to the OIDC provider and reported as unavailable.
func GetCapabilities(rsAPI roomserverAPI.ClientRoomserverAPI, oidcEnabled bool) util.JSONResponse {
	versionsMap := map[gomatrixserverlib.RoomVersion]string{}
	for v, desc := range version.SupportedRoomVersions() {
		if desc.Stable() {
			versionsMap[v] = "stable"
		} else {
			versionsMap[v] = "unstable"
		}
	}

	response := map[string]any{
		"capabilities": map[string]any{
			"m.change_password": map[string]bool{
				"enabled": !oidcEnabled,
			},
			"m.room_versions": map[string]any{
				"default":   rsAPI.DefaultRoomVersion(),
				"available": versionsMap,
			},
			"m.forget_forced_upon_leave": map[string]bool{
				"enabled": rsAPI.AutoForgetOnLeaveEnabled(),
			},
			"m.3pid_changes": map[string]bool{
				"enabled": !oidcEnabled,
			},
		},
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
