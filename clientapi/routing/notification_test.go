// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

type notificationUserAPI struct {
	api.ClientUserAPI
	request *api.QueryNotificationsRequest
	err     error
}

func (a *notificationUserAPI) QueryNotifications(_ context.Context, req *api.QueryNotificationsRequest, res *api.QueryNotificationsResponse) error {
	a.request = req
	res.Notifications = []*api.Notification{}
	return a.err
}

func TestGetNotifications(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		status      int
	}{
		{"default", "", http.StatusOK},
		{"pagination", "?from=42&limit=10&only=highlight", http.StatusOK},
		{"invalid limit", "?limit=abc", http.StatusBadRequest},
		{"negative limit", "?limit=-1", http.StatusBadRequest},
		{"overflow limit", "?limit=999999999999999999999", http.StatusBadRequest},
		{"invalid token", "?from=abc", http.StatusBadRequest},
		{"negative token", "?from=-1", http.StatusBadRequest},
		{"overflow token", "?from=999999999999999999999", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userAPI := &notificationUserAPI{}
			res := GetNotifications(httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/notifications"+tc.query, nil), &api.Device{UserID: "@alice:localhost"}, userAPI)
			require.Equal(t, tc.status, res.Code)
			if tc.status != http.StatusOK {
				require.Nil(t, userAPI.request)
				return
			}
			require.Equal(t, "alice", userAPI.request.Localpart)
			require.EqualValues(t, "localhost", userAPI.request.ServerName)
			if tc.name == "pagination" {
				require.Equal(t, "42", userAPI.request.From)
				require.Equal(t, 10, userAPI.request.Limit)
				require.Equal(t, "highlight", userAPI.request.Only)
			}
		})
	}
	t.Run("database failure", func(t *testing.T) {
		res := GetNotifications(httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/notifications", nil), &api.Device{UserID: "@alice:localhost"}, &notificationUserAPI{err: errors.New("database unavailable")})
		require.Equal(t, http.StatusInternalServerError, res.Code)
	})
}
