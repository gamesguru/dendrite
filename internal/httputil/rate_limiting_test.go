package httputil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/matrix-org/util"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func TestRateLimitHTTPResponse(t *testing.T) {
	for _, tc := range []struct {
		name         string
		milliseconds int64
		seconds      string
	}{
		{"zero", 0, "0"},
		{"one millisecond", 1, "1"},
		{"subsecond", 500, "1"},
		{"whole second", 1000, "1"},
		{"fractional second", 1001, "2"},
		{"multiple seconds", 2500, "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A zero-capacity limiter rejects deterministically without timers.
			limiter := NewRateLimits(&config.RateLimiting{CooloffMS: tc.milliseconds})
			limiter.enabled = true
			handler := MakeExternalAPI("rate_limit_test", func(req *http.Request) util.JSONResponse {
				return *limiter.Limit(req, nil)
			})
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
			require.Equal(t, tc.seconds, rec.Result().Header.Get("Retry-After"))
			var body struct {
				ErrCode      string `json:"errcode"`
				RetryAfterMS int64  `json:"retry_after_ms"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Equal(t, "M_LIMIT_EXCEEDED", body.ErrCode)
			require.Equal(t, tc.milliseconds, body.RetryAfterMS)
		})
	}
}

func TestRateLimitSuccessHasNoRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		device  *userapi.Device
	}{
		{name: "disabled"},
		{name: "allowed", enabled: true},
		{name: "admin", enabled: true, device: &userapi.Device{AccountType: userapi.AccountTypeAdmin}},
		{name: "appservice", enabled: true, device: &userapi.Device{AccountType: userapi.AccountTypeAppService}},
		{name: "exempt", enabled: true, device: &userapi.Device{UserID: "@exempt:test"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limiter := NewRateLimits(&config.RateLimiting{Threshold: 1, CooloffMS: 1, ExemptUserIDs: []string{"@exempt:test"}})
			limiter.enabled = tc.enabled
			handler := MakeExternalAPI("rate_limit_success_test", func(req *http.Request) util.JSONResponse {
				if response := limiter.Limit(req, tc.device); response != nil {
					return *response
				}
				return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
			})
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Empty(t, rec.Result().Header.Get("Retry-After"))
		})
	}
}

func TestLimitExceededResponseNegativeDelay(t *testing.T) {
	handler := MakeExternalAPI("negative_delay_test", func(*http.Request) util.JSONResponse {
		return LimitExceededResponse("slow down", -time.Second)
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, "0", rec.Result().Header.Get("Retry-After"))
}
