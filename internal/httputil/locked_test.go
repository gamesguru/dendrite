// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package httputil

import (
	"net/http"
	"testing"

	"github.com/matrix-org/util"

	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestLockedAccountAccess checks that every authenticating wrapper refuses
// locked accounts with M_USER_LOCKED unless the endpoint opts in (MSC3939).
func TestLockedAccountAccess(t *testing.T) {
	ok := func(*http.Request, *userapi.Device) util.JSONResponse {
		return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
	}
	okHTTP := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	locked := tokenAPI{accountType: userapi.AccountTypeUser, locked: true}
	lockedAdmin := tokenAPI{accountType: userapi.AccountTypeAdmin, locked: true}

	tests := []struct {
		name     string
		handler  http.Handler
		wantCode int
		wantErr  string
	}{
		{"auth API refuses locked accounts", MakeAuthAPI("t", locked, ok, WithAllowGuests(), WithAllowCallGuests()), http.StatusUnauthorized, "M_USER_LOCKED"},
		{"auth API admits locked accounts on opt-in", MakeAuthAPI("t", locked, ok, WithAllowLocked()), http.StatusOK, ""},
		{"admin API refuses locked administrators", MakeAdminAPI("t", lockedAdmin, ok), http.StatusUnauthorized, "M_USER_LOCKED"},
		{"optional auth API refuses locked accounts", MakeOptionalAuthAPI("t", locked, ok), http.StatusUnauthorized, "M_USER_LOCKED"},
		{"HTTP API refuses locked accounts", MakeHTTPAPI("t", locked, false, okHTTP, WithAuth()), http.StatusUnauthorized, "M_USER_LOCKED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, errCode := serveWithToken(t, tt.handler)
			if code != tt.wantCode || errCode != tt.wantErr {
				t.Fatalf("got %d %q, want %d %q", code, errCode, tt.wantCode, tt.wantErr)
			}
		})
	}
}
