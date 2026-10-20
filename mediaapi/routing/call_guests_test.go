// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type callGuestTokenAPI struct{}

func (callGuestTokenAPI) QueryAccessToken(_ context.Context, _ *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	res.Device = &userapi.Device{UserID: "@guest:test", ID: "DEVICE", AccountType: userapi.AccountTypeCallGuest}
	return nil
}

// TestCallGuestMediaAccess checks that call guests may download media but
// not upload it.
func TestCallGuestMediaAccess(t *testing.T) {
	cfg := &config.Zendrite{}
	cfg.MediaAPI.Matrix = &cfg.Global
	cfg.ClientAPI.Matrix = &cfg.Global
	// Every request that gets past authentication is rate limited, which
	// tells it apart from one refused for its account type without a
	// database behind the handlers.
	cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: true, Threshold: 0, CooloffMS: 1000}
	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, callGuestTokenAPI{}, nil, nil, nil)

	do := func(router http.Handler, method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	for _, path := range []string{"/_matrix/media/v3/upload", "/_matrix/media/v1/upload"} {
		rec := do(routers.Media, http.MethodPost, path)
		require.Equal(t, http.StatusForbidden, rec.Code, path)
		require.Equal(t, "M_FORBIDDEN", gjson.GetBytes(rec.Body.Bytes(), "errcode").Str, path)
	}
	rec := do(routers.Client, http.MethodGet, "/_matrix/client/v1/media/config")
	require.Equal(t, http.StatusForbidden, rec.Code)
	for _, path := range []string{"/_matrix/client/v1/media/download/test/abc", "/_matrix/client/v1/media/thumbnail/test/abc"} {
		rec = do(routers.Client, http.MethodGet, path)
		require.Equal(t, http.StatusTooManyRequests, rec.Code, path)
	}
}
