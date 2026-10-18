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
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// GetCapabilities returns information about the server's supported feature set
// and other relevant capabilities to an authenticated user. When oidcEnabled is
// true (MSC3861 delegated authentication), password and 3PID management are
// delegated to the OIDC provider and reported as unavailable.
func GetCapabilities(rsAPI roomserverAPI.ClientRoomserverAPI, oidcEnabled bool, delayedEvents *config.DelayedEvents, device *userapi.Device) util.JSONResponse {
	versionsMap := map[gomatrixserverlib.RoomVersion]string{}
	for v, desc := range version.SupportedRoomVersions() {
		if desc.Stable() {
			versionsMap[v] = "stable"
		} else {
			versionsMap[v] = "unstable"
		}
	}

	capabilities := map[string]any{
		"m.change_password": map[string]bool{
			"enabled": !oidcEnabled,
		},
		"m.get_login_token": map[string]bool{
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
		// Every profile field may be set (MSC4133). m.set_displayname and
		// m.set_avatar_url must agree with m.profile_fields.
		"m.profile_fields": map[string]bool{
			"enabled": true,
		},
		"uk.tcpip.msc4133.profile_fields": map[string]bool{
			"enabled": true,
		},
		"m.set_displayname": map[string]bool{
			"enabled": true,
		},
		"m.set_avatar_url": map[string]bool{
			"enabled": true,
		},
		// MSC4140 limits, as Synapse reports them: a zero maximum delay
		// tells clients that delayed events are disabled.
		"org.matrix.msc4140.delayed_events": map[string]int64{
			"max_delay_ms":  delayedEvents.MaxDelay.Milliseconds(),
			"max_scheduled": int64(delayedEvents.MaxPerUser),
		},
	}

	// Administrators can lock accounts (MSC4323), but not suspend them yet.
	// The capability is omitted for everyone who can do neither.
	if !oidcEnabled && device.AccountType == userapi.AccountTypeAdmin {
		capabilities["m.account_moderation"] = map[string]bool{
			"lock":    true,
			"suspend": false,
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]any{"capabilities": capabilities},
	}
}
