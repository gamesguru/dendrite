// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/matrix-org/util"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// tokenAPI resolves every access token to a device of the given account type.
type tokenAPI struct {
	accountType userapi.AccountType
	locked      bool
}

func (a tokenAPI) QueryAccessToken(_ context.Context, _ *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	res.Device = &userapi.Device{UserID: "@someone:test", ID: "DEVICE", AccountType: a.accountType, Locked: a.locked}
	return nil
}

func serveWithToken(t *testing.T, h http.Handler) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body struct {
		ErrCode string `json:"errcode"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body.ErrCode
}

func TestCallGuestAccess(t *testing.T) {
	ok := func(*http.Request, *userapi.Device) util.JSONResponse {
		return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
	}
	okHTTP := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	callGuest := tokenAPI{accountType: userapi.AccountTypeCallGuest}
	user := tokenAPI{accountType: userapi.AccountTypeUser}
	guest := tokenAPI{accountType: userapi.AccountTypeGuest}

	tests := []struct {
		name     string
		handler  http.Handler
		wantCode int
		wantErr  string
	}{
		{"auth API denies call guests by default", MakeAuthAPI("t", callGuest, ok), http.StatusForbidden, "M_FORBIDDEN"},
		{"guest access does not admit call guests", MakeAuthAPI("t", callGuest, ok, WithAllowGuests()), http.StatusForbidden, "M_FORBIDDEN"},
		{"auth API admits call guests on opt-in", MakeAuthAPI("t", callGuest, ok, WithAllowCallGuests()), http.StatusOK, ""},
		{"call guest access does not admit guests", MakeAuthAPI("t", guest, ok, WithAllowCallGuests()), http.StatusForbidden, "M_GUEST_ACCESS_FORBIDDEN"},
		{"users are unaffected", MakeAuthAPI("t", user, ok), http.StatusOK, ""},
		{"optional auth API denies call guests by default", MakeOptionalAuthAPI("t", callGuest, ok, WithAllowGuests()), http.StatusForbidden, "M_FORBIDDEN"},
		{"optional auth API admits call guests on opt-in", MakeOptionalAuthAPI("t", callGuest, ok, WithAllowCallGuests()), http.StatusOK, ""},
		{"admin API denies call guests", MakeAdminAPI("t", callGuest, ok), http.StatusForbidden, "M_FORBIDDEN"},
		{"HTTP API denies call guests by default", MakeHTTPAPI("t", callGuest, false, okHTTP, WithAuth()), http.StatusForbidden, "M_FORBIDDEN"},
		{"HTTP API admits call guests on opt-in", MakeHTTPAPI("t", callGuest, false, okHTTP, WithAuth(), WithAllowCallGuests()), http.StatusOK, ""},
		{"HTTP API still admits guests", MakeHTTPAPI("t", guest, false, okHTTP, WithAuth()), http.StatusOK, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, errCode := serveWithToken(t, tc.handler)
			if code != tc.wantCode || errCode != tc.wantErr {
				t.Fatalf("got %d %q, want %d %q", code, errCode, tc.wantCode, tc.wantErr)
			}
		})
	}
}
