// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/dendrite/internal/httputil"
	"codefloe.com/pat-s/dendrite/setup/config"
)

// msc3861ForbiddenHandler returns an HTTP handler that rejects requests with
// M_FORBIDDEN when MSC3861 OIDC delegated authentication is enabled.
func msc3861ForbiddenHandler(metricsName string) http.Handler {
	return httputil.MakeExternalAPI(metricsName, func(req *http.Request) util.JSONResponse {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("This endpoint is not available when authentication is delegated to an OIDC provider via MSC3861."),
		}
	})
}

// msc3861LoginFlows returns the login flows response when MSC3861 is active.
// Only SSO login is advertised since authentication is handled by the OIDC provider.
func msc3861LoginFlows(_ *config.MSCs) util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			Flows []map[string]string `json:"flows"`
		}{
			Flows: []map[string]string{
				{"type": "m.login.sso"},
			},
		},
	}
}
