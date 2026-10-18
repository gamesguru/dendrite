package routing

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
)

func TestDownloadRateLimitHTTPResponse(t *testing.T) {
	cfg := &config.MediaAPI{Matrix: &config.Global{}}
	limiter := httputil.NewRateLimits(&config.RateLimiting{Enabled: true, Threshold: 0, CooloffMS: 1500})
	handler := makeDownloadAPI("download", cfg, limiter, nil, nil, nil, nil, nil, nil, nil, false)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	require.Equal(t, "2", rec.Result().Header.Get("Retry-After"))
	require.JSONEq(t, `{"errcode":"M_LIMIT_EXCEEDED","error":"You are sending too many requests too quickly!","retry_after_ms":1500}`, rec.Body.String())
}
