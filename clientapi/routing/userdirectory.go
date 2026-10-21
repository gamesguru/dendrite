// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type UserDirectoryResponse struct {
	Results []authtypes.FullyQualifiedProfile `json:"results"`
	Limited bool                              `json:"limited"`
}

func SearchUserDirectory(
	ctx context.Context,
	device *userapi.Device,
	rsAPI api.ClientRoomserverAPI,
	provider userapi.QuerySearchProfilesAPI,
	searchString string,
	limit int,
	localServerName spec.ServerName,
) util.JSONResponse {
	if limit < 10 { //nolint:mnd
		limit = 10
	}

	results := map[string]authtypes.FullyQualifiedProfile{}
	response := &UserDirectoryResponse{
		Results: []authtypes.FullyQualifiedProfile{},
		Limited: false,
	}

	// Get users we share a room with
	knownUsersReq := &api.QueryKnownUsersRequest{
		UserID:       device.UserID,
		SearchString: searchString,
		Limit:        limit,
	}
	knownUsersRes := &api.QueryKnownUsersResponse{}
	if err := rsAPI.QueryKnownUsers(ctx, knownUsersReq, knownUsersRes); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return util.ErrorResponse(fmt.Errorf("rsAPI.QueryKnownUsers: %w", err))
	}

knownUsersLoop:
	for _, profile := range knownUsersRes.Users {
		if len(results) == limit {
			response.Limited = true
			break
		}
		userID := profile.UserID
		// get the full profile of the local user
		localpart, serverName, _ := gomatrixserverlib.SplitID('@', userID)
		if serverName == localServerName {
			userReq := &userapi.QuerySearchProfilesRequest{
				SearchString: localpart,
				Limit:        limit,
			}
			userRes := &userapi.QuerySearchProfilesResponse{}
			if err := provider.QuerySearchProfiles(ctx, userReq, userRes); err != nil {
				return util.ErrorResponse(fmt.Errorf("userAPI.QuerySearchProfiles: %w", err))
			}
			for _, p := range userRes.Profiles {
				if strings.Contains(p.DisplayName, searchString) ||
					strings.Contains(p.Localpart, searchString) {
					profile.DisplayName = p.DisplayName
					profile.AvatarURL = p.AvatarURL
					results[userID] = profile
					if len(results) == limit {
						response.Limited = true
						break knownUsersLoop
					}
				}
			}
		} else if strings.Contains(localpart, searchString) {
			// Search remote users by ID only. Fetching profiles here creates one
			// federation request per known user, and the response isn't cached.
			results[userID] = profile
			if len(results) == limit {
				response.Limited = true
				break knownUsersLoop
			}
		}
	}

	for _, result := range results {
		response.Results = append(response.Results, result)
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}
