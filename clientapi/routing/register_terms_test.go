// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/matrix-org/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	capi "codefloe.com/pat-s/zendrite/clientapi/api"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

var testRegistrationTerms = []config.RegistrationPolicy{
	{
		ID:      "terms_of_service",
		Version: "1.2",
		Translations: map[string]config.PolicyTranslation{
			"en": {Name: "Terms of Service", URL: "https://example.org/terms-1.2-en.html"},
			"fr": {Name: "Conditions d'utilisation", URL: "https://example.org/terms-1.2-fr.html"},
		},
	},
	{
		ID:           "privacy_policy",
		Version:      "1.2",
		Translations: map[string]config.PolicyTranslation{"de": {Name: "Datenschutz", URL: "https://example.org/privacy-de.html"}},
	},
}

func registerStep(t *testing.T, userAPI api.ClientUserAPI, cfg *config.ClientAPI, body registerRequest) util.JSONResponse {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(body))
	return Register(httptest.NewRequest(http.MethodPost, "/", &buf), userAPI, cfg)
}

// TestRegisterTerms checks MSC1692: every flow ends with m.login.terms, its
// params carry the policies in the spec's shape, and registration completes
// only once the terms are accepted, directly or through the fallback page.
func TestRegisterTerms(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cfg.ClientAPI.RegistrationDisabled = false
		cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
		cfg.ClientAPI.RegistrationTerms = testRegistrationTerms
		require.NoError(t, cfg.Derive())

		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

		start := func(username string) userInteractiveResponse {
			t.Helper()
			res := registerStep(t, userAPI, &cfg.ClientAPI, registerRequest{Username: username, Password: "passwordpassword"})
			require.Equal(t, http.StatusUnauthorized, res.Code, "%+v", res.JSON)
			uia, ok := res.JSON.(userInteractiveResponse)
			require.True(t, ok, "%+v", res.JSON)
			return uia
		}
		submit := func(username, session, stage string) util.JSONResponse {
			return registerStep(t, userAPI, &cfg.ClientAPI, registerRequest{
				Username: username, Password: "passwordpassword",
				Auth: authDict{Type: authtypes.LoginType(stage), Session: session},
			})
		}

		uia := start("termsuser")
		require.Equal(t, []authtypes.Flow{{Stages: []authtypes.LoginType{authtypes.LoginTypeDummy, authtypes.LoginTypeTerms}}}, uia.Flows)
		params, err := json.Marshal(uia.Params[authtypes.LoginTypeTerms])
		require.NoError(t, err)
		assert.JSONEq(t, `{"policies":{
			"terms_of_service":{"version":"1.2",
				"en":{"name":"Terms of Service","url":"https://example.org/terms-1.2-en.html"},
				"fr":{"name":"Conditions d'utilisation","url":"https://example.org/terms-1.2-fr.html"}},
			"privacy_policy":{"version":"1.2","de":{"name":"Datenschutz","url":"https://example.org/privacy-de.html"}}}}`, string(params))

		// The dummy stage alone does not finish the flow.
		res := submit("termsuser", uia.Session, authtypes.LoginTypeDummy)
		require.Equal(t, http.StatusUnauthorized, res.Code, "%+v", res.JSON)
		res = submit("termsuser", uia.Session, authtypes.LoginTypeTerms)
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)

		// The fallback page lists every policy and completes the stage once
		// all of them are ticked.
		uia = start("fallbackuser")
		page := "/_matrix/client/v3/auth/m.login.terms/fallback/web?session=" + url.QueryEscape(uia.Session)
		rec := httptest.NewRecorder()
		AuthFallback(rec, httptest.NewRequest(http.MethodGet, page, nil), authtypes.LoginTypeTerms, &cfg.ClientAPI)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "https://example.org/terms-1.2-en.html", "English is preferred")
		assert.Contains(t, rec.Body.String(), "https://example.org/privacy-de.html")

		post := func(accept ...string) *httptest.ResponseRecorder {
			form := url.Values{"session": {uia.Session}, "accept": accept}
			req := httptest.NewRequest(http.MethodPost, page, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			AuthFallback(rec, req, authtypes.LoginTypeTerms, &cfg.ClientAPI)
			return rec
		}
		assert.Equal(t, http.StatusBadRequest, post("terms_of_service").Code, "every policy must be accepted")
		require.Equal(t, http.StatusOK, post("terms_of_service", "privacy_policy").Code)
		res = submit("fallbackuser", uia.Session, authtypes.LoginTypeDummy)
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
	})
}

// TestRegisterTermsUnconfigured refuses the stage when no terms are set.
func TestRegisterTermsUnconfigured(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{})
	cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
	require.NoError(t, cfg.Derive())
	assert.NotContains(t, cfg.Derived.Registration.Flows[0].Stages, authtypes.LoginType(authtypes.LoginTypeTerms))

	rec := httptest.NewRecorder()
	AuthFallback(rec, httptest.NewRequest(http.MethodGet, "/?session=x", nil), authtypes.LoginTypeTerms, &cfg.ClientAPI)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRegisterTermsClosedRegistration accepts the terms stage on a server
// with closed registration only once the session has redeemed a token.
func TestRegisterTermsClosedRegistration(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cfg.ClientAPI.RegistrationDisabled = true
		cfg.ClientAPI.RegistrationRequiresToken = true
		cfg.ClientAPI.RegistrationTerms = testRegistrationTerms
		require.NoError(t, cfg.Derive())

		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		token := "closedtoken"
		_, err := userAPI.PerformAdminCreateRegistrationToken(processCtx.Context(), &capi.RegistrationToken{
			Token: ptrTo(token), UsesAllowed: ptrTo(int32(1)), Pending: ptrTo(int32(0)), Completed: ptrTo(int32(0)),
		})
		require.NoError(t, err)

		step := func(session string, auth authDict) util.JSONResponse {
			auth.Session = session
			return registerStep(t, userAPI, &cfg.ClientAPI, registerRequest{Username: "closeduser", Password: "passwordpassword", Auth: auth})
		}
		// Closed registration refuses the terms stage without a token, also
		// as the first request of a session.
		res := step("", authDict{Type: authtypes.LoginTypeTerms})
		assert.Equal(t, http.StatusForbidden, res.Code, "%+v", res.JSON)

		// A token opens the session, after which the terms finish the flow.
		res = step("", authDict{Type: authtypes.LoginTypeRegistrationToken, Token: token})
		require.Equal(t, http.StatusUnauthorized, res.Code, "%+v", res.JSON)
		uia, ok := res.JSON.(userInteractiveResponse)
		require.True(t, ok, "%+v", res.JSON)
		assert.Equal(t, []authtypes.Flow{{Stages: []authtypes.LoginType{authtypes.LoginTypeRegistrationToken, authtypes.LoginTypeTerms}}}, uia.Flows)
		res = step(uia.Session, authDict{Type: authtypes.LoginTypeTerms})
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
	})
}
