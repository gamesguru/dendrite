// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	clienthttputil "codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// ssoCompatSession holds the ephemeral state for an in-flight legacy SSO -> OIDC
// login. It is keyed by the OAuth2 state parameter.
type ssoCompatSession struct {
	redirectURL  string
	codeVerifier string
	deviceID     string
	cookieNonce  string
	expiresAt    time.Time
}

// ssoCompatOIDCTokens holds the provider-issued credentials for a completed
// SSO login, keyed by the Matrix login token handed to the client. They are
// returned verbatim when the client redeems the login token via
// POST /login m.login.token: under MSC3861 a homeserver-local access token
// would never validate, since token validation is delegated to the OIDC
// provider.
type ssoCompatOIDCTokens struct {
	accessToken  string
	refreshToken string
	expiresInMs  int
	deviceID     string
	userID       string
	expiresAt    time.Time
}

// ssoCompatHandler bridges Element's legacy m.login.sso flow to the OIDC
// provider configured for MSC3861.
//
// The in-flight state below (sessions, oidcByLogTok) is process-local, so the
// SSO callback and the POST /login redemption that follows it must be handled
// by the same process. See the "known limitations" note in the MSC3861
// documentation.
type ssoCompatHandler struct {
	cfg        *config.MSC3861Config
	userAPI    userapi.ClientUserAPI
	httpClient *http.Client
	rateLimits *httputil.RateLimits

	// stop terminates cleanupLoop; closed exactly once by Close.
	stop     chan struct{}
	stopOnce sync.Once

	mu           sync.Mutex
	sessions     map[string]*ssoCompatSession
	oidcByLogTok map[string]*ssoCompatOIDCTokens
}

const (
	ssoSessionTTL      = 10 * time.Minute
	ssoCleanupInterval = 5 * time.Minute
	// Device IDs allocated for legacy SSO logins are random strings of this
	// length; 16 alphanumeric characters is comfortably unique per user, per
	// the device ID allocation guidance in the OAuth 2.0 API spec.
	ssoDeviceIDLength = 16
	// Converts the provider's expires_in seconds to the expires_in_ms
	// milliseconds used by the Matrix client API.
	millisPerSecond = 1000
	// The ssoCookieName cookie binds an in-flight SSO login to the browser
	// that started it. Without it, an attacker could start a flow with a
	// malicious redirectUrl and trick a victim's browser into completing it,
	// leaking the resulting login token.
	ssoCookieName = "zendrite_sso_nonce"
	// The cookie path scopes the SSO browser-binding cookie to the client API.
	ssoCookiePath = "/_matrix/client"
	// Caps the in-flight SSO sessions and the stashed OIDC token sets held in
	// memory, so the maps cannot grow at line rate when rate limiting is
	// disabled.
	ssoMaxEntries = 10000
	// The retry-after hint sent with M_LIMIT_EXCEEDED responses from the SSO
	// compat layer.
	ssoLimitRetryAfterMS = 1000
	// Bounds the body read on the unauthenticated POST /login endpoint handled
	// by the SSO compat layer.
	ssoLoginBodyMaxBytes = 1 << 20
	// Bounds the outbound OIDC discovery request made when no explicit
	// authorization/token endpoint is configured.
	ssoOIDCDiscoveryTimeout = 10 * time.Second
	// Discovered endpoints are cached for an hour, mirroring the auth metadata
	// and userapi discovery caches, so that /login/sso/redirect and the
	// unauthenticated POST /refresh do not perform an outbound
	// .well-known/openid-configuration fetch on every request.
	ssoOIDCDiscoveryCacheTTL = time.Hour
	// A failed discovery is negative-cached for a minute so that a provider
	// outage cannot be amplified into one outbound request per client request,
	// while still recovering quickly once the provider is back.
	ssoOIDCDiscoveryErrorCacheTTL = time.Minute
)

// errSSOStorageFull is returned when an in-memory SSO compat map has reached
// ssoMaxEntries; handlers translate it into a 429 M_LIMIT_EXCEEDED response.
var errSSOStorageFull = errors.New("sso compat storage is full")

// newSSOCompatHandler builds the legacy SSO compat handler. The Global config
// is accepted but unused: every URL the compat layer builds comes from the
// MSC3861 config or the incoming request.
func newSSOCompatHandler(
	cfg *config.MSC3861Config,
	userAPI userapi.ClientUserAPI,
	httpClient *http.Client,
	rateLimits *httputil.RateLimits,
) *ssoCompatHandler {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	h := &ssoCompatHandler{
		cfg:          cfg,
		userAPI:      userAPI,
		httpClient:   httpClient,
		rateLimits:   rateLimits,
		stop:         make(chan struct{}),
		sessions:     make(map[string]*ssoCompatSession),
		oidcByLogTok: make(map[string]*ssoCompatOIDCTokens),
	}
	go h.cleanupLoop()
	return h
}

// Close stops the background cleanup goroutine. It is safe to call more than
// once, and mainly matters for tests, which would otherwise leak one goroutine
// per handler they build.
func (h *ssoCompatHandler) Close() {
	h.stopOnce.Do(func() {
		close(h.stop)
	})
}

func (h *ssoCompatHandler) cleanupLoop() {
	ticker := time.NewTicker(ssoCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.cleanupExpiredSessions()
		case <-h.stop:
			return
		}
	}
}

func (h *ssoCompatHandler) cleanupExpiredSessions() {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for k, v := range h.sessions {
		if now.After(v.expiresAt) {
			delete(h.sessions, k)
		}
	}
	for k, v := range h.oidcByLogTok {
		if now.After(v.expiresAt) {
			delete(h.oidcByLogTok, k)
		}
	}
}

func (h *ssoCompatHandler) registerRedirect(redirectURL string) (string, *ssoCompatSession, error) {
	state, err := auth.GenerateAccessToken()
	if err != nil {
		return "", nil, fmt.Errorf("failed to generate SSO state: %w", err)
	}
	codeVerifier, err := generatePKCEVerifier()
	if err != nil {
		return "", nil, fmt.Errorf("failed to generate PKCE verifier: %w", err)
	}
	cookieNonce, err := generatePKCEVerifier()
	if err != nil {
		return "", nil, fmt.Errorf("failed to generate SSO cookie nonce: %w", err)
	}

	session := &ssoCompatSession{
		redirectURL:  redirectURL,
		codeVerifier: codeVerifier,
		// The device ID is allocated up front so it can be requested via the
		// urn:matrix:client:device: scope, as native OAuth 2.0 clients do. Each
		// SSO login therefore gets its own device instead of sharing the
		// fallback "OIDC" device.
		deviceID: util.RandomString(ssoDeviceIDLength),
		// The cookie nonce binds the callback to the browser that started this
		// flow: it is stored here and sent to the browser as an HttpOnly
		// cookie, and the callback must present a matching cookie.
		cookieNonce: cookieNonce,
		expiresAt:   time.Now().Add(ssoSessionTTL),
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sessions) >= ssoMaxEntries {
		return "", nil, errSSOStorageFull
	}
	h.sessions[state] = session
	return state, session, nil
}

// takeSession atomically returns and removes the session for the given state,
// or nil if the state is unknown or expired. Consuming the session under a
// single lock prevents duplicate callbacks from racing the same authorization
// code through the token exchange.
func (h *ssoCompatHandler) takeSession(state string) *ssoCompatSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.sessions[state]
	if !ok {
		return nil
	}
	delete(h.sessions, state)
	if time.Now().After(s.expiresAt) {
		return nil
	}
	return s
}

// stashOIDCTokens records the provider-issued credentials for a completed SSO
// login, keyed by the Matrix login token the client will redeem.
func (h *ssoCompatHandler) stashOIDCTokens(loginToken string, tokens *oidcTokenResponse, deviceID, userID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.oidcByLogTok) >= ssoMaxEntries {
		return errSSOStorageFull
	}
	h.oidcByLogTok[loginToken] = &ssoCompatOIDCTokens{
		accessToken:  tokens.AccessToken,
		refreshToken: tokens.RefreshToken,
		expiresInMs:  tokens.ExpiresIn * millisPerSecond,
		deviceID:     deviceID,
		userID:       userID,
		// The stash is worthless once the underlying Matrix login token
		// expires, so it shares the login token lifetime instead of the
		// longer SSO session TTL.
		expiresAt: time.Now().Add(userapi.DefaultLoginTokenLifetime),
	}
	return nil
}

// takeOIDCTokens returns and removes the stashed credentials for a login
// token, or nil if the token is unknown or expired.
func (h *ssoCompatHandler) takeOIDCTokens(loginToken string) *ssoCompatOIDCTokens {
	h.mu.Lock()
	defer h.mu.Unlock()
	t, ok := h.oidcByLogTok[loginToken]
	if !ok || time.Now().After(t.expiresAt) {
		return nil
	}
	delete(h.oidcByLogTok, loginToken)
	return t
}

// generatePKCEVerifier creates a 32-byte URL-safe base64 string (43 chars),
// suitable for PKCE code challenge generation.
func generatePKCEVerifier() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func pkceChallenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// requestScheme returns the URL scheme for an incoming request, honoring
// X-Forwarded-Proto when present.
func requestScheme(req *http.Request) string {
	if fwd := req.Header.Get("X-Forwarded-Proto"); fwd != "" {
		return fwd
	}
	if req.TLS != nil {
		return "https"
	}
	return "http"
}

// callbackURL returns the registered OIDC callback URL for this homeserver.
func (h *ssoCompatHandler) callbackURL(req *http.Request) string {
	if h.cfg.SSOCallbackURL != "" {
		return h.cfg.SSOCallbackURL
	}
	if h.cfg.PublicBaseURL != "" {
		return strings.TrimSuffix(h.cfg.PublicBaseURL, "/") + "/_matrix/client/v3/login/sso/callback"
	}
	// Last resort: derive the callback URL from the incoming request. This
	// trusts the Host and X-Forwarded-Proto headers, so prefer configuring
	// sso_callback_url or public_base_url on public deployments.
	return requestScheme(req) + "://" + req.Host + "/_matrix/client/v3/login/sso/callback"
}

// browserBindingCookie returns the HttpOnly cookie carrying the nonce that
// binds an in-flight SSO login to the browser that started it. SameSite=Lax
// still allows the cookie on the top-level navigation back from the OIDC
// provider; Secure is set whenever the request arrived over HTTPS.
func (h *ssoCompatHandler) browserBindingCookie(req *http.Request, nonce string) *http.Cookie {
	return &http.Cookie{
		Name:     ssoCookieName,
		Value:    nonce,
		Path:     ssoCookiePath,
		HttpOnly: true,
		Secure:   requestScheme(req) == "https",
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(ssoSessionTTL.Seconds()),
	}
}

// homeserverOrigin returns the scheme and host this homeserver is reachable
// at, preferring the configured public_base_url over the incoming request's
// own origin, which trusts the Host and X-Forwarded-Proto headers.
func (h *ssoCompatHandler) homeserverOrigin(req *http.Request) (scheme, host string) {
	if h.cfg.PublicBaseURL != "" {
		if u, err := url.Parse(h.cfg.PublicBaseURL); err == nil && u.Scheme != "" && u.Host != "" {
			return u.Scheme, u.Host
		}
	}
	return requestScheme(req), req.Host
}

// sameOrigin reports whether the parsed target has the given scheme and host.
// Scheme and host are compared case-insensitively, as both are
// case-insensitive per RFC 3986; the port is part of the host and must match
// exactly.
func sameOrigin(target *url.URL, scheme, host string) bool {
	return strings.EqualFold(target.Scheme, scheme) && strings.EqualFold(target.Host, host)
}

// redirectAllowlistEntryMatches reports whether a redirect target matches a
// single sso_redirect_allowlist entry.
//
// For http(s) entries the scheme and host are compared after parsing, because
// a bare prefix comparison would let https://app.example.com.evil.com/ match
// an entry of https://app.example.com. When the entry carries a path beyond
// "/", the target must sit at or below that path.
//
// Entries with any other scheme (e.g. the element://vector/webapp deep link
// used by Element Desktop) have no meaningful origin to compare, so they keep
// the historic prefix matching.
func redirectAllowlistEntryMatches(target *url.URL, redirectURL, entry string) bool {
	entryURL, err := url.Parse(entry)
	if err != nil || entryURL.Host == "" ||
		(!strings.EqualFold(entryURL.Scheme, "http") && !strings.EqualFold(entryURL.Scheme, "https")) {
		return strings.HasPrefix(redirectURL, entry)
	}
	if !sameOrigin(target, entryURL.Scheme, entryURL.Host) {
		return false
	}
	entryPath := strings.TrimSuffix(entryURL.Path, "/")
	if entryPath == "" {
		return true
	}
	return target.Path == entryPath || strings.HasPrefix(target.Path, entryPath+"/")
}

// redirectURLAllowed reports whether the given redirectUrl may be used for a
// legacy SSO login.
//
// The check is default-deny: with no sso_redirect_allowlist configured, only
// targets on the homeserver's own origin are accepted. The browser-binding
// cookie does not make an open redirect safe here, because the victim's own
// browser runs the whole flow and therefore holds the cookie: a link to
// /login/sso/redirect?redirectUrl=https://evil.example.com/ would hand the
// attacker a login token, which POST /login redeems into the *provider-issued*
// access and refresh tokens.
func (h *ssoCompatHandler) redirectURLAllowed(req *http.Request, redirectURL string) bool {
	// Relative or otherwise origin-less targets are rejected outright: a
	// protocol-relative //evil.example.com/ has no scheme but is very much
	// cross-origin.
	target, err := url.Parse(redirectURL)
	if err != nil || target.Scheme == "" {
		return false
	}

	if len(h.cfg.SSORedirectAllowlist) == 0 {
		scheme, host := h.homeserverOrigin(req)
		return host != "" && sameOrigin(target, scheme, host)
	}

	for _, entry := range h.cfg.SSORedirectAllowlist {
		if redirectAllowlistEntryMatches(target, redirectURL, entry) {
			return true
		}
	}
	return false
}

// serveRedirect handles GET /_matrix/client/v3/login/sso/redirect.
func (h *ssoCompatHandler) serveRedirect(req *http.Request) util.JSONResponse {
	if r := h.rateLimits.Limit(req, nil); r != nil {
		return *r
	}

	redirectURL := req.URL.Query().Get("redirectUrl")
	if redirectURL == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Missing redirectUrl query parameter."),
		}
	}
	if !h.redirectURLAllowed(req, redirectURL) {
		if len(h.cfg.SSORedirectAllowlist) == 0 {
			logrus.WithField("redirect_url", redirectURL).Warn(
				"MSC3861 SSO compat: rejected a cross-origin redirectUrl; configure mscs.msc3861.sso_redirect_allowlist to permit it",
			)
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("The given redirectUrl is not on this homeserver's origin. Ask the server administrator to add it to mscs.msc3861.sso_redirect_allowlist."),
			}
		}
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The given redirectUrl is not in mscs.msc3861.sso_redirect_allowlist on this homeserver."),
		}
	}

	state, session, err := h.registerRedirect(redirectURL)
	if err != nil {
		if errors.Is(err, errSSOStorageFull) {
			return util.JSONResponse{
				Code: http.StatusTooManyRequests,
				JSON: spec.LimitExceeded("Too many in-flight SSO logins, please try again later.", ssoLimitRetryAfterMS),
			}
		}
		logrus.WithError(err).Error("MSC3861 SSO compat: failed to create session")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	endpoints, err := h.discoverOIDCEndpoints(req.Context())
	if err != nil {
		logrus.WithError(err).Error("MSC3861 SSO compat: failed to discover OIDC endpoints")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {h.cfg.ClientID},
		"redirect_uri":          {h.callbackURL(req)},
		"state":                 {state},
		"scope":                 {"openid offline_access email profile urn:matrix:client:api:* urn:matrix:client:device:" + session.deviceID},
		"code_challenge":        {pkceChallenge(session.codeVerifier)},
		"code_challenge_method": {"S256"},
	}
	// OAuth 2.0 aware clients signal the user's intent with the action
	// parameter; registration maps to prompt=create at the provider.
	if req.URL.Query().Get("action") == "register" {
		q.Set("prompt", "create")
	}
	authURL := endpoints.authorizationEndpoint + "?" + q.Encode()

	return util.JSONResponse{
		Code: http.StatusFound,
		Headers: map[string]string{
			"Location":   authURL,
			"Set-Cookie": h.browserBindingCookie(req, session.cookieNonce).String(),
		},
	}
}

// serveCallback handles GET /_matrix/client/v3/login/sso/callback.
func (h *ssoCompatHandler) serveCallback(req *http.Request) util.JSONResponse {
	state := req.URL.Query().Get("state")
	// The session is consumed atomically here, before the network-bound token
	// exchange, so duplicate callbacks cannot race the same authorization
	// code.
	session := h.takeSession(state)
	if session == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Invalid or expired SSO session state."),
		}
	}

	// The callback must come from the browser that started the flow: it has
	// to present the browser-binding cookie issued by /login/sso/redirect.
	cookie, err := req.Cookie(ssoCookieName)
	if err != nil || subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(session.cookieNonce)) != 1 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Missing or mismatched SSO session cookie."),
		}
	}

	if errCode := req.URL.Query().Get("error"); errCode != "" {
		errDesc := req.URL.Query().Get("error_description")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("OIDC provider error: " + errCode + ": " + errDesc),
		}
	}

	code := req.URL.Query().Get("code")
	if code == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Missing authorization code."),
		}
	}

	tokens, err := h.exchangeCode(req.Context(), code, h.callbackURL(req), session.codeVerifier)
	if err != nil {
		logrus.WithError(err).Error("MSC3861 SSO compat: token exchange failed")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("Failed to exchange OIDC authorization code."),
		}
	}

	device, errRes := h.provisionUser(req.Context(), tokens.AccessToken)
	if errRes != nil {
		return *errRes
	}

	var tokenRes userapi.PerformLoginTokenCreationResponse
	if err := h.userAPI.PerformLoginTokenCreation(req.Context(), &userapi.PerformLoginTokenCreationRequest{
		Data: userapi.LoginTokenData{UserID: device.UserID},
	}, &tokenRes); err != nil {
		logrus.WithError(err).Error("MSC3861 SSO compat: failed to create login token")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Stash the provider-issued credentials against the login token so that
	// redeeming it via m.login.token returns the OIDC access token rather
	// than a homeserver-local one, which would never introspect.
	if err := h.stashOIDCTokens(tokenRes.Metadata.Token, tokens, device.ID, device.UserID); err != nil {
		logrus.WithError(err).Error("MSC3861 SSO compat: failed to stash OIDC tokens")
		return util.JSONResponse{
			Code: http.StatusTooManyRequests,
			JSON: spec.LimitExceeded("Too many in-flight SSO logins, please try again later.", ssoLimitRetryAfterMS),
		}
	}

	clientURL, err := url.Parse(session.redirectURL)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown("Invalid redirectUrl."),
		}
	}
	q := clientURL.Query()
	q.Set("loginToken", tokenRes.Metadata.Token)
	clientURL.RawQuery = q.Encode()

	return util.JSONResponse{
		Code:    http.StatusFound,
		Headers: map[string]string{"Location": clientURL.String()},
	}
}

type oidcProviderEndpoints struct {
	authorizationEndpoint string
	tokenEndpoint         string
}

// ssoDiscoveryCacheEntry holds the outcome of a discovery attempt for one
// issuer. Failures are cached too (with a shorter TTL) so that a provider
// outage does not turn every /login/sso/redirect and POST /refresh into an
// outbound request.
type ssoDiscoveryCacheEntry struct {
	endpoints *oidcProviderEndpoints
	err       error
	expiresAt time.Time
}

var (
	ssoDiscoveryCache   = map[string]ssoDiscoveryCacheEntry{}
	ssoDiscoveryCacheMu sync.RWMutex
)

// discoverOIDCEndpoints returns the provider's authorization and token
// endpoints, fetching the discovery document at most once per
// ssoOIDCDiscoveryCacheTTL per issuer.
func (h *ssoCompatHandler) discoverOIDCEndpoints(ctx context.Context) (*oidcProviderEndpoints, error) {
	return h.discoverOIDCEndpointsWithTTL(ctx, ssoOIDCDiscoveryCacheTTL, ssoOIDCDiscoveryErrorCacheTTL)
}

// discoverOIDCEndpointsWithTTL is the testable core of discoverOIDCEndpoints
// with explicit positive and negative cache TTLs.
func (h *ssoCompatHandler) discoverOIDCEndpointsWithTTL(ctx context.Context, cacheTTL, errorCacheTTL time.Duration) (*oidcProviderEndpoints, error) {
	cfg := h.cfg
	// Fully configured endpoints need no discovery at all.
	if cfg.AuthorizationEndpoint != "" && cfg.TokenEndpoint != "" {
		return &oidcProviderEndpoints{
			authorizationEndpoint: cfg.AuthorizationEndpoint,
			tokenEndpoint:         cfg.TokenEndpoint,
		}, nil
	}

	ssoDiscoveryCacheMu.RLock()
	cached, ok := ssoDiscoveryCache[cfg.Issuer]
	ssoDiscoveryCacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.endpoints, cached.err
	}

	endpoints, err := h.fetchOIDCEndpoints(ctx)
	ttl := cacheTTL
	if err != nil {
		ttl = errorCacheTTL
	}
	ssoDiscoveryCacheMu.Lock()
	ssoDiscoveryCache[cfg.Issuer] = ssoDiscoveryCacheEntry{
		endpoints: endpoints,
		err:       err,
		expiresAt: time.Now().Add(ttl),
	}
	ssoDiscoveryCacheMu.Unlock()

	return endpoints, err
}

// fetchOIDCEndpoints performs the uncached .well-known/openid-configuration
// lookup, falling back to the config-derived defaults for any endpoint the
// provider does not advertise.
func (h *ssoCompatHandler) fetchOIDCEndpoints(ctx context.Context) (*oidcProviderEndpoints, error) {
	cfg := h.cfg
	ctx, cancel := context.WithTimeout(ctx, ssoOIDCDiscoveryTimeout)
	defer cancel()

	discoveryURL := strings.TrimSuffix(cfg.Issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("OIDC discovery returned status %d: %s", resp.StatusCode, string(body))
	}

	var doc struct {
		AuthorizationEndpoint string `json:"authorization_endpoint"`
		TokenEndpoint         string `json:"token_endpoint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, err
	}

	authz := doc.AuthorizationEndpoint
	if authz == "" {
		authz = cfg.AuthorizationEndpointOrDefault()
	}
	token := doc.TokenEndpoint
	if token == "" {
		token = cfg.TokenEndpointOrDefault()
	}
	return &oidcProviderEndpoints{
		authorizationEndpoint: authz,
		tokenEndpoint:         token,
	}, nil
}

type oidcTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
}

// callTokenEndpoint POSTs a form to the provider's token endpoint,
// authenticating with the configured client credentials.
func (h *ssoCompatHandler) callTokenEndpoint(ctx context.Context, endpoint string, form url.Values) (*http.Response, error) {
	authMethod := h.cfg.ClientAuthMethod
	if authMethod == "" {
		authMethod = "client_secret_basic"
	}

	var bodyReader io.Reader
	switch authMethod {
	case "client_secret_basic":
		// body is the base form; basic auth added below.
		bodyReader = strings.NewReader(form.Encode())
	case "client_secret_post":
		form.Set("client_secret", h.cfg.ClientSecret)
		bodyReader = strings.NewReader(form.Encode())
	default:
		return nil, fmt.Errorf("unsupported client_auth_method %q", authMethod)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	if authMethod == "client_secret_basic" {
		// The credentials are sent verbatim. RFC 6749 section 2.3.1 nominally
		// asks for form-encoding first, but MAS, Keycloak and Hydra all compare
		// the raw values, so encoding them would silently break every client
		// secret containing '+', '/', '=' or '%'.
		req.SetBasicAuth(h.cfg.ClientID, h.cfg.ClientSecret)
	}

	return h.httpClient.Do(req)
}

func (h *ssoCompatHandler) exchangeCode(ctx context.Context, code, callbackURL, codeVerifier string) (*oidcTokenResponse, error) {
	endpoints, err := h.discoverOIDCEndpoints(ctx)
	if err != nil {
		return nil, err
	}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {callbackURL},
		"client_id":     {h.cfg.ClientID},
		"code_verifier": {codeVerifier},
	}

	resp, err := h.callTokenEndpoint(ctx, endpoints.tokenEndpoint, form)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokens oidcTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil {
		return nil, err
	}
	if tokens.AccessToken == "" {
		return nil, fmt.Errorf("token endpoint response did not contain access_token")
	}
	return &tokens, nil
}

// refreshRequest is the request body for POST /_matrix/client/v3/refresh.
type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// refreshResponse is the response body for POST /_matrix/client/v3/refresh.
type refreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresInMs  int    `json:"expires_in_ms,omitempty"`
}

// serveRefresh handles POST /_matrix/client/v3/refresh when MSC3861 is
// enabled, bridging the legacy refresh endpoint to the provider's refresh
// token grant. Without it, every legacy SSO session would die when the
// provider-issued access token expires.
func (h *ssoCompatHandler) serveRefresh(req *http.Request) util.JSONResponse {
	if r := h.rateLimits.Limit(req, nil); r != nil {
		return *r
	}

	var refreshReq refreshRequest
	if resErr := clienthttputil.UnmarshalJSONRequest(req, &refreshReq); resErr != nil {
		return *resErr
	}
	if refreshReq.RefreshToken == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing refresh_token"),
		}
	}

	endpoints, err := h.discoverOIDCEndpoints(req.Context())
	if err != nil {
		logrus.WithError(err).Error("MSC3861: failed to discover OIDC endpoints for token refresh")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshReq.RefreshToken},
		"client_id":     {h.cfg.ClientID},
	}

	resp, err := h.callTokenEndpoint(req.Context(), endpoints.tokenEndpoint, form)
	if err != nil {
		logrus.WithError(err).Error("MSC3861: refresh request to provider failed")
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "failed to reach the OIDC provider"},
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized {
		// The provider rejected the refresh token (e.g. invalid_grant); the
		// session is gone.
		return util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.UnknownToken("Refresh token was rejected by the OIDC provider"),
		}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logrus.WithFields(logrus.Fields{
			"status":   resp.StatusCode,
			"response": string(body),
		}).Error("MSC3861: provider token refresh returned non-200 status")
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "OIDC provider failed to refresh the token"},
		}
	}

	var tokens oidcTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil || tokens.AccessToken == "" {
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "invalid token response from the OIDC provider"},
		}
	}

	// Providers may rotate refresh tokens; if they don't, hand back the one
	// the client sent.
	newRefreshToken := tokens.RefreshToken
	if newRefreshToken == "" {
		newRefreshToken = refreshReq.RefreshToken
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: refreshResponse{
			AccessToken:  tokens.AccessToken,
			RefreshToken: newRefreshToken,
			ExpiresInMs:  tokens.ExpiresIn * millisPerSecond,
		},
	}
}

// provisionUser validates the OIDC access token and returns the Matrix device
// it represents. It uses QueryAccessToken which performs RFC 7662 token
// introspection (with a userinfo fallback) and auto-provisions the user account
// and device if necessary.
func (h *ssoCompatHandler) provisionUser(ctx context.Context, accessToken string) (*userapi.Device, *util.JSONResponse) {
	var res userapi.QueryAccessTokenResponse
	if err := h.userAPI.QueryAccessToken(ctx, &userapi.QueryAccessTokenRequest{
		AccessToken: accessToken,
	}, &res); err != nil {
		logrus.WithError(err).Error("MSC3861 SSO compat: QueryAccessToken failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "failed to look up user"},
		}
	}
	if res.Err != "" {
		logrus.WithField("query_access_token_err", res.Err).Error("MSC3861 SSO compat: QueryAccessToken returned error")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "failed to look up user"},
		}
	}
	if res.Device == nil {
		logrus.Error("MSC3861 SSO compat: OIDC access token is not active")
		return nil, &util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.UnknownToken("OIDC access token is not active"),
		}
	}
	return res.Device, nil
}

// msc3861LoginFlows returns the login flows response when MSC3861 is active.
// When the legacy SSO compat layer is enabled we advertise m.login.sso and
// m.login.token so that clients which do not yet implement native OIDC can
// still log in via the OIDC provider. The SSO flow is marked as
// oauth_aware_preferred so OAuth 2.0 aware clients only offer that flow.
func msc3861LoginFlows() util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: flows{
			Flows: []flow{
				{Type: authtypes.LoginTypeSSO, OAuthAwarePreferred: true},
				{Type: authtypes.LoginTypeToken},
			},
		},
	}
}

// serveLogin handles /_matrix/client/v3/login when MSC3861 is enabled. GET
// returns the OIDC-aware flow list; POST only accepts m.login.token and
// redeems it against the provider-issued credentials stashed by the SSO
// callback. Login tokens unknown to the SSO compat layer are rejected:
// redeeming them through the legacy login path would mint a homeserver-local
// access token that can never validate against the OIDC provider.
func (h *ssoCompatHandler) serveLogin() http.Handler {
	return httputil.MakeExternalAPI("login", func(req *http.Request) util.JSONResponse {
		if req.Method == http.MethodGet || req.Method == http.MethodOptions {
			return msc3861LoginFlows()
		}

		if req.Body == nil {
			req.Body = http.NoBody
		}
		// The login endpoint is unauthenticated, so the body read is bounded
		// to keep memory usage in check.
		body, err := io.ReadAll(io.LimitReader(req.Body, ssoLoginBodyMaxBytes+1))
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.NotJSON("Unable to read request body"),
			}
		}
		_ = req.Body.Close()
		if len(body) > ssoLoginBodyMaxBytes {
			return util.JSONResponse{
				Code: http.StatusRequestEntityTooLarge,
				JSON: spec.BadJSON("Request body too large."),
			}
		}

		// Only m.login.token is allowed under MSC3861; everything else is delegated.
		if gjson.GetBytes(body, "type").String() != authtypes.LoginTypeToken {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("Login is delegated to the OIDC provider via MSC3861."),
			}
		}

		if r := h.rateLimits.Limit(req, nil); r != nil {
			return *r
		}

		loginToken := gjson.GetBytes(body, "token").String()
		if loginToken == "" {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MissingParam("Missing token"),
			}
		}

		oidcTokens := h.takeOIDCTokens(loginToken)
		if oidcTokens == nil {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("Invalid or expired login token"),
			}
		}

		// Validate and consume the login token so it cannot be replayed.
		var queryRes userapi.QueryLoginTokenResponse
		if err := h.userAPI.QueryLoginToken(req.Context(), &userapi.QueryLoginTokenRequest{
			Token: loginToken,
		}, &queryRes); err != nil || queryRes.Data == nil {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec.Forbidden("Invalid or expired login token"),
			}
		}
		var delRes userapi.PerformLoginTokenDeletionResponse
		if err := h.userAPI.PerformLoginTokenDeletion(req.Context(), &userapi.PerformLoginTokenDeletionRequest{
			Token: loginToken,
		}, &delRes); err != nil {
			logrus.WithError(err).Error("MSC3861 SSO compat: failed to delete login token")
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: loginResponse{
				UserID:       oidcTokens.userID,
				AccessToken:  oidcTokens.accessToken,
				DeviceID:     oidcTokens.deviceID,
				RefreshToken: oidcTokens.refreshToken,
				ExpiresInMs:  oidcTokens.expiresInMs,
			},
		}
	})
}
