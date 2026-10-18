// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// refreshableLifetime is the access token lifetime for a client that asked
// for a refresh token with `refresh_token: true`, or zero if it gets none.
func refreshableLifetime(cfg *config.ClientAPI, requested bool) time.Duration {
	if !requested {
		return 0
	}
	return cfg.RefreshableAccessTokenLifetime
}

// refreshableTokens holds a refresh token and the expiry of the access token
// issued with it. Its zero value issues a non-expiring access token.
type refreshableTokens struct {
	refreshToken string
	expiresTS    int64
	expiresInMs  int
}

// newRefreshableTokens generates a refresh token for an access token that
// expires after lifetime, or none if lifetime is zero.
func newRefreshableTokens(lifetime time.Duration) (refreshableTokens, error) {
	if lifetime <= 0 {
		return refreshableTokens{}, nil
	}
	token, err := auth.GenerateAccessToken()
	if err != nil {
		return refreshableTokens{}, err
	}
	return refreshableTokens{
		refreshToken: token,
		expiresTS:    time.Now().Add(lifetime).UnixMilli(),
		expiresInMs:  int(lifetime.Milliseconds()),
	}, nil
}

// Refresh implements POST /refresh (MSC2918): it exchanges a refresh token for
// a new access token and refresh token for the same device.
func Refresh(req *http.Request, userAPI userapi.ClientUserAPI, cfg *config.ClientAPI) util.JSONResponse {
	var r refreshRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}
	if r.RefreshToken == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Missing refresh_token")}
	}
	accessToken, err := auth.GenerateAccessToken()
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("auth.GenerateAccessToken failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	// A device keeps refreshing at the configured lifetime. When refresh
	// tokens have been disabled since, the new access token never expires.
	tokens, err := newRefreshableTokens(cfg.RefreshableAccessTokenLifetime)
	if err == nil && tokens.refreshToken == "" {
		// Replace the refresh token anyway, so the presented one cannot be reused.
		tokens.refreshToken, err = auth.GenerateAccessToken()
	}
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to generate refresh token")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	var res userapi.PerformRefreshTokenResponse
	if err = userAPI.PerformRefreshToken(req.Context(), &userapi.PerformRefreshTokenRequest{
		RefreshToken:         r.RefreshToken,
		AccessToken:          accessToken,
		NewRefreshToken:      tokens.refreshToken,
		AccessTokenExpiresTS: tokens.expiresTS,
	}, &res); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.PerformRefreshToken failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if res.Locked {
		return auth.UserLockedResponse()
	}
	if res.Device == nil {
		return util.JSONResponse{Code: http.StatusUnauthorized, JSON: spec.UnknownToken("Unknown refresh token")}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: refreshResponse{
			AccessToken:  accessToken,
			RefreshToken: tokens.refreshToken,
			ExpiresInMs:  tokens.expiresInMs,
		},
	}
}
