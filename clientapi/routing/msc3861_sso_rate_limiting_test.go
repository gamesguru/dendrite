package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type ssoCapacityUserAPI struct{ userapi.ClientUserAPI }

func (ssoCapacityUserAPI) QueryAccessToken(_ context.Context, _ *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	res.Device = &userapi.Device{UserID: "@alice:test", ID: "device"}
	return nil
}

func (ssoCapacityUserAPI) PerformLoginTokenCreation(_ context.Context, _ *userapi.PerformLoginTokenCreationRequest, res *userapi.PerformLoginTokenCreationResponse) error {
	res.Metadata.Token = "login-token"
	return nil
}

func TestSSOCapacityHTTPResponses(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access-token","token_type":"Bearer"}`))
	}))
	defer provider.Close()
	for _, full := range []bool{false, true} {
		t.Run(strconv.FormatBool(full), func(t *testing.T) {
			h := newSSOCompatHandler(&config.MSC3861Config{
				AuthorizationEndpoint: provider.URL + "/auth", TokenEndpoint: provider.URL + "/token",
			}, ssoCapacityUserAPI{}, provider.Client(), httputil.NewRateLimits(&config.RateLimiting{}))
			defer h.Close()
			if full {
				h.mu.Lock()
				for i := range ssoMaxEntries {
					h.sessions[strconv.Itoa(i)] = &ssoCompatSession{expiresAt: time.Now().Add(time.Hour)}
					h.oidcByLogTok[strconv.Itoa(i)] = &ssoCompatOIDCTokens{expiresAt: time.Now().Add(time.Hour)}
				}
				h.mu.Unlock()
			}
			redirect := httptest.NewRequest(http.MethodGet, "http://matrix.test/login/sso/redirect?redirectUrl="+url.QueryEscape("http://matrix.test/client"), nil)
			rec := httptest.NewRecorder()
			httputil.MakeExternalAPI("sso_capacity_redirect", h.serveRedirect).ServeHTTP(rec, redirect)
			assertSSOCapacityResponse(t, rec, full)

			h.mu.Lock()
			h.sessions["callback-state"] = &ssoCompatSession{redirectURL: "http://matrix.test/client", cookieNonce: "nonce", expiresAt: time.Now().Add(time.Hour)}
			h.mu.Unlock()
			callback := httptest.NewRequest(http.MethodGet, "http://matrix.test/login/sso/callback?state=callback-state&code=code", nil)
			callback.AddCookie(&http.Cookie{Name: ssoCookieName, Value: "nonce"})
			rec = httptest.NewRecorder()
			httputil.MakeExternalAPI("sso_capacity_callback", h.serveCallback).ServeHTTP(rec, callback)
			assertSSOCapacityResponse(t, rec, full)
		})
	}
}

func assertSSOCapacityResponse(t *testing.T, rec *httptest.ResponseRecorder, full bool) {
	t.Helper()
	if !full {
		require.Equal(t, http.StatusFound, rec.Code, rec.Body.String())
		require.Empty(t, rec.Result().Header.Get("Retry-After"))
		return
	}
	require.Equal(t, http.StatusTooManyRequests, rec.Code, rec.Body.String())
	require.Equal(t, "1", rec.Result().Header.Get("Retry-After"))
	require.JSONEq(t, `{"errcode":"M_LIMIT_EXCEEDED","error":"Too many in-flight SSO logins, please try again later.","retry_after_ms":1000}`, rec.Body.String())
}
