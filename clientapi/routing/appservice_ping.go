// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// maxPingBodyBytes limits how much of a failed ping's response body is
// returned to the application service.
const maxPingBodyBytes = 4096

type appservicePingRequest struct {
	TransactionID string `json:"transaction_id,omitempty"`
}

type appservicePingResponse struct {
	DurationMS int64 `json:"duration_ms"`
}

type appservicePingBadStatus struct {
	ErrCode string `json:"errcode"`
	Err     string `json:"error"`
	Status  int    `json:"status"`
	Body    string `json:"body"`
}

// AppservicePing implements POST /_matrix/client/v1/appservice/{appserviceId}/ping
// (MSC2659): it calls POST /_matrix/app/v1/ping on the application service whose
// as_token authenticated the request.
func AppservicePing(req *http.Request, cfg *config.AppServiceAPI, appserviceID string) util.JSONResponse {
	token, err := auth.ExtractAccessToken(req)
	if err != nil {
		return util.JSONResponse{Code: http.StatusUnauthorized, JSON: spec.MissingToken(err.Error())}
	}
	var as *config.ApplicationService
	for i := range cfg.Derived.ApplicationServices {
		candidate := &cfg.Derived.ApplicationServices[i]
		if subtle.ConstantTimeCompare([]byte(candidate.ASToken), []byte(token)) == 1 {
			as = candidate
			break
		}
	}
	if as == nil || as.ID != appserviceID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Provided access token is not the appservice's as_token"),
		}
	}

	var body appservicePingRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}

	if as.URL == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{ErrCode: "M_URL_NOT_SET", Err: "Application service doesn't have a URL configured"},
		}
	}

	duration, status, respBody, err := pingAppservice(req.Context(), cfg, as, body)
	switch {
	case err != nil && isTimeout(err):
		return util.JSONResponse{
			Code: http.StatusGatewayTimeout,
			JSON: spec.MatrixError{ErrCode: "M_CONNECTION_TIMEOUT", Err: "Connection to application service timed out"},
		}
	case err != nil:
		util.GetLogger(req.Context()).WithError(err).WithField("appservice", as.ID).Warn("Failed to ping application service")
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.MatrixError{ErrCode: "M_CONNECTION_FAILED", Err: "Connection to application service failed"},
		}
	case status != http.StatusOK:
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: appservicePingBadStatus{
				ErrCode: "M_BAD_STATUS",
				Err:     fmt.Sprintf("Ping returned status %d", status),
				Status:  status,
				Body:    respBody,
			},
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: appservicePingResponse{DurationMS: duration.Milliseconds()},
	}
}

// pingAppservice sends the ping and returns how long it took, the response
// status, and the start of the response body.
func pingAppservice(
	ctx context.Context, cfg *config.AppServiceAPI, as *config.ApplicationService, body appservicePingRequest,
) (time.Duration, int, string, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, 0, "", err
	}
	address := as.RequestUrl() + "/_matrix/app/v1/ping"
	if cfg.LegacyAuth {
		address += "?access_token=" + url.QueryEscape(as.HSToken)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, address, bytes.NewReader(payload))
	if err != nil {
		return 0, 0, "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+as.HSToken)

	start := time.Now()
	resp, err := as.HTTPClient.Do(httpReq)
	if err != nil {
		return 0, 0, "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxPingBodyBytes))
	duration := time.Since(start)
	if err != nil && resp.StatusCode == http.StatusOK {
		return 0, 0, "", err
	}
	return duration, resp.StatusCode, string(respBody), nil
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
