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

	roomserverAPI "codefloe.com/pat-s/dendrite/roomserver/api"
	"codefloe.com/pat-s/dendrite/roomserver/version"
)

// GetCapabilities returns information about the server's supported feature set
// and other relevant capabilities to an authenticated user.
func GetCapabilities(rsAPI roomserverAPI.ClientRoomserverAPI) util.JSONResponse {
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
				"enabled": true,
			},
			"m.room_versions": map[string]any{
				"default":   rsAPI.DefaultRoomVersion(),
				"available": versionsMap,
			},
		},
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
