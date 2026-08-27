// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/jellydator/ttlcache/v3"
	"github.com/sirupsen/logrus"

	zinternal "codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// IntrospectionResponse represents the response from an RFC 7662 token introspection endpoint.
type IntrospectionResponse struct {
	Active   bool   `json:"active"`
	Sub      string `json:"sub"`
	Scope    string `json:"scope"`
	Exp      int64  `json:"exp"`
	Username string `json:"username"`
	// Email is a non-standard but RFC 7662-permitted member some providers
	// inject so the homeserver can derive a friendlier localpart than the
	// subject claim.
	Email string `json:"email"`
	// EmailVerified reports whether the provider has verified that the subject
	// controls Email. Many providers let users self-assert an address, so an
	// unverified claim must never influence the localpart: it would let anyone
	// register alice@example.com and claim @alice:example.com.
	EmailVerified bool `json:"email_verified"`
}

// oidcDiscoveryResponse represents the relevant fields from an OIDC discovery document.
type oidcDiscoveryResponse struct {
	IntrospectionEndpoint string `json:"introspection_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

// cachedIntrospection holds a cached introspection result.
type cachedIntrospection struct {
	response *IntrospectionResponse
	err      error
}

var (
	// IntrospectionCache caches token introspection results.
	introspectionCache     *ttlcache.Cache[string, *cachedIntrospection]
	introspectionCacheOnce sync.Once

	// DiscoveredEndpoints caches the discovered OIDC endpoints per issuer.
	// Entries are immutable once published: readers copy the pointer while
	// holding the mutex and may then read the fields without it.
	discoveredEndpoints   = make(map[string]*oidcDiscoveryCache)
	discoveredEndpointsMu sync.Mutex
)

// oidcDiscoveryCache holds the endpoints discovered from an issuer's OIDC
// discovery document. Each endpoint has its own timestamp so that a missing
// endpoint can be negative-cached for a shorter period than a found one.
type oidcDiscoveryCache struct {
	introspectionEndpoint string
	introspectionCachedAt time.Time
	userinfoEndpoint      string
	userinfoCachedAt      time.Time
}

const (
	// Introspection results are cached for 30 seconds. This knowingly accepts
	// a ~30s post-revocation window in which a token that was invalidated at
	// the provider may still be treated as valid here, in exchange for
	// bounding the load on the provider.
	introspectionCacheTTL = 30 * time.Second
	// Failed introspections are cached briefly so that clients polling with a
	// bad or expired token don't hammer the provider.
	introspectionErrorCacheTTL = 5 * time.Second
	introspectionCacheMaxSize  = 10_000
	// Discovery endpoints found in the issuer's discovery document are cached
	// for an hour.
	discoveryEndpointCacheTTL = 1 * time.Hour
	// Missing discovery endpoints are negative-cached for a minute, so a
	// provider that adds an endpoint later is picked up quickly while a
	// misconfigured issuer isn't queried on every token validation.
	discoveryMissingEndpointCacheTTL = 1 * time.Minute
	stableDeviceScopePrefix          = "urn:matrix:client:device:"
	deviceScopePrefix                = "urn:matrix:device:"
	legacyDeviceScopePrefix          = "urn:matrix:org.matrix.msc2967.client:device:"
	// DeviceID used when the token scope carries no device ID.
	defaultOIDCDeviceID = "OIDC"
	// The RFC 7591 client authentication methods we support for introspection.
	clientAuthMethodBasic = "client_secret_basic"
	clientAuthMethodPost  = "client_secret_post"
)

func getIntrospectionCache() *ttlcache.Cache[string, *cachedIntrospection] {
	introspectionCacheOnce.Do(func() {
		introspectionCache = ttlcache.New[string, *cachedIntrospection](
			ttlcache.WithTTL[string, *cachedIntrospection](introspectionCacheTTL),
			ttlcache.WithCapacity[string, *cachedIntrospection](introspectionCacheMaxSize),
		)
		go introspectionCache.Start()
	})
	return introspectionCache
}

// hashToken produces a SHA-256 hex hash of a token for use as a cache key.
func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// extractDeviceIDFromScope parses the OAuth scope string for a device ID
// encoded as urn:matrix:client:device:DEVICEID (the stable scope token from
// the OAuth 2.0 API), urn:matrix:device:DEVICEID, or the legacy unstable
// MSC2967 prefix.
func extractDeviceIDFromScope(scope string) string {
	for _, part := range strings.Fields(scope) {
		switch {
		case strings.HasPrefix(part, stableDeviceScopePrefix):
			return strings.TrimPrefix(part, stableDeviceScopePrefix)
		case strings.HasPrefix(part, deviceScopePrefix):
			return strings.TrimPrefix(part, deviceScopePrefix)
		case strings.HasPrefix(part, legacyDeviceScopePrefix):
			return strings.TrimPrefix(part, legacyDeviceScopePrefix)
		}
	}
	return ""
}

var localpartInvalidChars = regexp.MustCompile(`[^0-9a-z_\-+=./]+`)

// sanitizeLocalpart converts an arbitrary OIDC username claim into a valid
// Matrix localpart. Invalid characters are replaced with underscores, the
// result is lowercased, leading underscores are avoided, and it is trimmed
// to a sensible length. An empty input yields an empty result.
func sanitizeLocalpart(input string) string {
	if strings.TrimSpace(input) == "" {
		return ""
	}

	s := strings.ToLower(input)
	s = localpartInvalidChars.ReplaceAllString(s, "_")
	// Trim trailing underscores; leading underscores are handled below.
	s = strings.TrimRight(s, "_")

	if strings.HasPrefix(s, "_") {
		s = "u" + s
	}

	const maxLocalpartLen = 64
	if len(s) > maxLocalpartLen {
		s = s[:maxLocalpartLen]
	}

	return s
}

// stripEmailDomain removes the domain from an email address when it matches
// the server name, so that sienna@example.com provisions as @sienna:example.com
// rather than @sienna_example.com:example.com. Addresses on other domains are
// returned whole to keep localparts unique.
func stripEmailDomain(email, serverName string) string {
	local, domain, found := strings.Cut(email, "@")
	if found && strings.EqualFold(domain, serverName) {
		return local
	}
	return email
}

// deriveLocalpart picks the best Matrix localpart for an OIDC identity,
// preferring the email claim, then the username claim, then the subject
// claim for providers like Ory Hydra that don't expose a username.
// The email claim is only trusted once the provider has verified it: an
// unverified address is attacker-chosen, and because stripEmailDomain removes
// the local server name it would hand out localparts in our own namespace.
func deriveLocalpart(introspection *IntrospectionResponse, serverName string) string {
	if introspection.EmailVerified {
		if localpart := sanitizeLocalpart(stripEmailDomain(introspection.Email, serverName)); localpart != "" {
			return localpart
		}
	}
	if localpart := sanitizeLocalpart(introspection.Username); localpart != "" {
		return localpart
	}
	return sanitizeLocalpart(introspection.Sub)
}

// discoveryEntryFresh reports whether a cached discovery result is still
// valid: found endpoints are cached for an hour, missing endpoints are only
// negative-cached for a minute.
func discoveryEntryFresh(cachedAt time.Time, found bool) bool {
	if cachedAt.IsZero() {
		return false
	}
	if found {
		return time.Since(cachedAt) < discoveryEndpointCacheTTL
	}
	return time.Since(cachedAt) < discoveryMissingEndpointCacheTTL
}

// fetchOIDCDiscovery retrieves and decodes the OIDC discovery document of the
// given issuer.
func fetchOIDCDiscovery(ctx context.Context, issuer string, httpClient *http.Client) (*oidcDiscoveryResponse, error) {
	discoveryURL := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("msc3861: failed to create OIDC discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("msc3861: OIDC discovery request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("msc3861: OIDC discovery returned status %d", resp.StatusCode)
	}

	var discovery oidcDiscoveryResponse
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		return nil, fmt.Errorf("msc3861: failed to decode OIDC discovery response: %w", err)
	}
	return &discovery, nil
}

// updateDiscoveryCache records the endpoints from a fetched discovery
// document, including the absence of an endpoint (negative cache). The entry
// is replaced wholesale so published entries stay immutable.
func updateDiscoveryCache(issuer string, discovery *oidcDiscoveryResponse) {
	now := time.Now()
	discoveredEndpointsMu.Lock()
	defer discoveredEndpointsMu.Unlock()
	discoveredEndpoints[issuer] = &oidcDiscoveryCache{
		introspectionEndpoint: discovery.IntrospectionEndpoint,
		introspectionCachedAt: now,
		userinfoEndpoint:      discovery.UserinfoEndpoint,
		userinfoCachedAt:      now,
	}
}

// discoverIntrospectionEndpoint fetches the OIDC discovery document from the issuer
// and returns the introspection_endpoint. Found endpoints are cached for 1 hour
// per issuer, missing endpoints for 1 minute.
func discoverIntrospectionEndpoint(ctx context.Context, issuer string, httpClient *http.Client) string {
	issuer = strings.TrimSuffix(issuer, "/")

	discoveredEndpointsMu.Lock()
	cached := discoveredEndpoints[issuer]
	discoveredEndpointsMu.Unlock()
	if cached != nil && discoveryEntryFresh(cached.introspectionCachedAt, cached.introspectionEndpoint != "") {
		return cached.introspectionEndpoint
	}

	// The HTTP fetch happens outside the mutex: a slow provider must not stall
	// unrelated token validations. Concurrent duplicate fetches are harmless.
	discovery, err := fetchOIDCDiscovery(ctx, issuer, httpClient)
	if err != nil {
		logrus.WithError(err).Warn("MSC3861: OIDC discovery failed")
		return ""
	}

	updateDiscoveryCache(issuer, discovery)
	if discovery.IntrospectionEndpoint != "" {
		logrus.WithField("endpoint", discovery.IntrospectionEndpoint).Info("MSC3861: discovered introspection endpoint")
	}
	return discovery.IntrospectionEndpoint
}

// resolveIntrospectionEndpoint returns the introspection endpoint to use,
// trying config, then OIDC discovery, then the default fallback.
func resolveIntrospectionEndpoint(ctx context.Context, msc3861 *config.MSC3861Config, httpClient *http.Client) string {
	if msc3861.IntrospectionEndpoint != "" {
		return msc3861.IntrospectionEndpoint
	}

	if endpoint := discoverIntrospectionEndpoint(ctx, msc3861.Issuer, httpClient); endpoint != "" {
		return endpoint
	}

	return msc3861.IntrospectionEndpointOrDefault()
}

// discoverUserinfoEndpoint fetches the OIDC discovery document and returns the
// userinfo_endpoint, falling back to the configured or default issuer value.
// Results are cached with the same TTLs as the introspection endpoint.
func discoverUserinfoEndpoint(ctx context.Context, msc3861 *config.MSC3861Config, httpClient *http.Client) string {
	if msc3861.UserinfoEndpoint != "" {
		return msc3861.UserinfoEndpoint
	}

	if msc3861.Issuer == "" {
		return ""
	}
	issuer := strings.TrimSuffix(msc3861.Issuer, "/")

	discoveredEndpointsMu.Lock()
	cached := discoveredEndpoints[issuer]
	discoveredEndpointsMu.Unlock()
	if cached != nil && discoveryEntryFresh(cached.userinfoCachedAt, cached.userinfoEndpoint != "") {
		if cached.userinfoEndpoint != "" {
			return cached.userinfoEndpoint
		}
		return msc3861.UserinfoEndpointOrDefault()
	}

	// The HTTP fetch happens outside the mutex, see discoverIntrospectionEndpoint.
	discovery, err := fetchOIDCDiscovery(ctx, issuer, httpClient)
	if err != nil {
		logrus.WithError(err).Warn("MSC3861: OIDC discovery for userinfo failed")
		return msc3861.UserinfoEndpointOrDefault()
	}

	updateDiscoveryCache(issuer, discovery)
	if discovery.UserinfoEndpoint != "" {
		return discovery.UserinfoEndpoint
	}

	return msc3861.UserinfoEndpointOrDefault()
}

// errTokenExpired is returned when the OIDC provider rejects a token because
// it has expired, as opposed to being unknown or invalid for another reason.
// Callers use it to signal a soft logout so clients attempt a token refresh
// instead of destroying the session.
var errTokenExpired = fmt.Errorf("msc3861: access token expired")

// parseBearerChallengeParams parses the auth-param list of a Bearer
// WWW-Authenticate challenge (RFC 6750 section 3) into a lowercase-keyed map.
// It returns false if the header is not a Bearer challenge with parameters.
func parseBearerChallengeParams(header string) (map[string]string, bool) {
	scheme, rest, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return nil, false
	}
	params := make(map[string]string)
	for _, part := range strings.Split(rest, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		params[strings.ToLower(strings.TrimSpace(key))] = strings.Trim(strings.TrimSpace(value), `"`)
	}
	return params, true
}

// isExpiredBearerToken reports whether an RFC 6750 rejection indicates an
// expired (rather than unknown or malformed) token. The WWW-Authenticate
// challenge is authoritative when present: error="invalid_token" combined
// with an error_description mentioning expiry means the token expired. The
// response body is only consulted as a fallback when no challenge was sent.
func isExpiredBearerToken(wwwAuthenticate string, body []byte) bool {
	if wwwAuthenticate != "" {
		params, ok := parseBearerChallengeParams(wwwAuthenticate)
		if !ok {
			return false
		}
		if !strings.EqualFold(params["error"], "invalid_token") {
			return false
		}
		return strings.Contains(strings.ToLower(params["error_description"]), "expir")
	}
	haystack := strings.ToLower(string(body))
	return strings.Contains(haystack, "invalid_token") && strings.Contains(haystack, "expired")
}

// userinfoToken validates an access token by calling the OIDC UserInfo endpoint.
// This is used as a fallback when RFC 7662 token introspection is unavailable.
func userinfoToken(ctx context.Context, msc3861 *config.MSC3861Config, token string, httpClient *http.Client) (*IntrospectionResponse, error) {
	endpoint := discoverUserinfoEndpoint(ctx, msc3861, httpClient)
	if endpoint == "" {
		return nil, fmt.Errorf("msc3861: no userinfo endpoint available")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("msc3861: failed to create userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("msc3861: userinfo request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		// A 401 with an invalid_token error describing expiry (RFC 6750) means
		// the session is intact but the token needs refreshing.
		if resp.StatusCode == http.StatusUnauthorized && isExpiredBearerToken(resp.Header.Get("WWW-Authenticate"), body) {
			return nil, errTokenExpired
		}
		return nil, fmt.Errorf("msc3861: userinfo returned status %d: %s", resp.StatusCode, string(body))
	}

	var userinfo struct {
		Sub               string `json:"sub"`
		PreferredUsername string `json:"preferred_username"`
		Name              string `json:"name"`
		Nickname          string `json:"nickname"`
		Email             string `json:"email"`
		EmailVerified     bool   `json:"email_verified"`
		Username          string `json:"username"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&userinfo); err != nil {
		return nil, fmt.Errorf("msc3861: failed to decode userinfo response: %w", err)
	}
	if userinfo.Sub == "" {
		return nil, fmt.Errorf("msc3861: userinfo response missing sub")
	}

	username := userinfo.PreferredUsername
	if username == "" {
		username = userinfo.Name
	}
	if username == "" {
		username = userinfo.Nickname
	}
	if username == "" && userinfo.EmailVerified {
		// Same reasoning as in deriveLocalpart: an unverified address is
		// self-asserted, and feeding it into the username chain would smuggle
		// it past the email_verified gate.
		username = userinfo.Email
	}
	if username == "" {
		username = userinfo.Username
	}

	return &IntrospectionResponse{
		Active:        true,
		Sub:           userinfo.Sub,
		Username:      username,
		Email:         userinfo.Email,
		EmailVerified: userinfo.EmailVerified,
	}, nil
}

// introspectToken calls the OIDC introspection endpoint to validate an access
// token, returning a cached result when one is available.
func introspectToken(ctx context.Context, msc3861 *config.MSC3861Config, token string, httpClient *http.Client) (*IntrospectionResponse, error) {
	// Check cache first.
	cache := getIntrospectionCache()
	tokenHash := hashToken(token)
	if item := cache.Get(tokenHash); item != nil {
		cached := item.Value()
		return cached.response, cached.err
	}

	introspection, err := doIntrospectToken(ctx, msc3861, token, httpClient)
	if err != nil {
		// Cache failures briefly: clients polling with an invalid or expired
		// token would otherwise hit the provider on every single request.
		cache.Set(tokenHash, &cachedIntrospection{err: err}, introspectionErrorCacheTTL)
		return nil, err
	}

	cache.Set(tokenHash, &cachedIntrospection{response: introspection}, introspectionCacheTTL)
	return introspection, nil
}

// doIntrospectToken calls the OIDC introspection endpoint to validate an access token.
// If introspection is unavailable, it falls back to the OIDC UserInfo endpoint.
func doIntrospectToken(ctx context.Context, msc3861 *config.MSC3861Config, token string, httpClient *http.Client) (*IntrospectionResponse, error) {
	// Reject an unsupported auth method before any network work happens: it is
	// a configuration error, not something worth discovering after a request
	// has already been built and an endpoint discovered.
	authMethod := msc3861.ClientAuthMethod
	if authMethod == "" {
		authMethod = clientAuthMethodBasic
	}
	if authMethod != clientAuthMethodBasic && authMethod != clientAuthMethodPost {
		return nil, fmt.Errorf("msc3861: unsupported client_auth_method %q", authMethod)
	}

	endpoint := resolveIntrospectionEndpoint(ctx, msc3861, httpClient)

	// The form must be complete before the request is created. Patching
	// req.Body afterwards leaves req.GetBody returning the original,
	// credential-less body, and the http client replays GetBody on redirects
	// and HTTP/2 retries, which would silently drop the credentials.
	form := url.Values{"token": {token}}
	if authMethod == clientAuthMethodPost {
		form.Set("client_id", msc3861.ClientID)
		form.Set("client_secret", msc3861.ClientSecret)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("msc3861: failed to create introspection request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	if authMethod == clientAuthMethodBasic {
		// RFC 6749 section 2.3.1 nominally form-encodes these before base64,
		// but MAS, Keycloak and Hydra all compare the raw values, so encoding
		// them breaks every secret containing '+', '/', '=' or '%'.
		req.SetBasicAuth(msc3861.ClientID, msc3861.ClientSecret)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		logrus.WithError(err).Warn("MSC3861: introspection request failed, trying userinfo fallback")
		return userinfoToken(ctx, msc3861, token, httpClient)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fields := logrus.Fields{
			"status":   resp.StatusCode,
			"response": string(body),
		}
		// A 401/403 rejects our own client credentials, not the user's token.
		// The userinfo fallback below hides that at the cost of returning no
		// scope, so device IDs silently collapse to the "OIDC" default and
		// urn:synapse:admin:* is never seen. Say so at Error level rather than
		// letting a misconfigured secret look like a working deployment.
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			fields["client_id"] = msc3861.ClientID
			fields["client_auth_method"] = authMethod
			logrus.WithFields(fields).Error(
				"MSC3861: the OIDC provider rejected the configured client credentials; " +
					"falling back to userinfo, so scope-derived device IDs and admin scope will be lost",
			)
			return userinfoToken(ctx, msc3861, token, httpClient)
		}
		logrus.WithFields(fields).Warn("MSC3861: introspection returned non-200 status, trying userinfo fallback")
		return userinfoToken(ctx, msc3861, token, httpClient)
	}

	var introspection IntrospectionResponse
	if err := json.NewDecoder(resp.Body).Decode(&introspection); err != nil {
		logrus.WithError(err).Warn("MSC3861: failed to decode introspection response, trying userinfo fallback")
		return userinfoToken(ctx, msc3861, token, httpClient)
	}

	return &introspection, nil
}

// localpartTaken reports whether an account with the given localpart already
// exists on this server.
func (a *UserInternalAPI) localpartTaken(ctx context.Context, localpart string, serverName spec.ServerName) (bool, error) {
	_, err := a.DB.GetAccountByLocalpart(ctx, localpart, serverName)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

// provisionMSC3861Localpart determines the Matrix localpart for a previously
// unseen OIDC subject and persists the external ID mapping. It returns an
// empty localpart (and no error) when no safe localpart can be assigned, in
// which case the caller must refuse the login without linking any account.
func (a *UserInternalAPI) provisionMSC3861Localpart(ctx context.Context, introspection *IntrospectionResponse, issuer, sub string, serverName spec.ServerName) (string, error) {
	localpart := deriveLocalpart(introspection, string(serverName))
	if localpart == "" {
		logrus.WithField("sub", sub).Warn("MSC3861: introspection response has no email, no username, no sub, and no existing external ID mapping")
		return "", nil
	}

	// Validate before persisting the mapping: a stored localpart that fails
	// validation would lock the user out of every future login.
	if err := zinternal.ValidateUsername(localpart, serverName); err != nil {
		logrus.WithError(err).WithField("localpart", localpart).Warn("MSC3861: derived localpart is not valid")
		return "", nil
	}

	// Never attach an OIDC identity to a pre-existing account that merely
	// happens to share the derived localpart (e.g. a password-registered
	// account); fall back to the subject claim instead.
	taken, err := a.localpartTaken(ctx, localpart, serverName)
	if err != nil {
		return "", err
	}
	if taken {
		localpart = sanitizeLocalpart(introspection.Sub)
		if err := zinternal.ValidateUsername(localpart, serverName); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"localpart": localpart,
				"sub":       sub,
			}).Warn("MSC3861: subject-derived localpart is not valid")
			return "", nil
		}
		taken, err = a.localpartTaken(ctx, localpart, serverName)
		if err != nil {
			return "", err
		}
		if taken {
			logrus.WithFields(logrus.Fields{
				"localpart": localpart,
				"sub":       sub,
			}).Warn("MSC3861: derived and subject localparts both collide with existing accounts, refusing to link")
			return "", nil
		}
	}

	if err := a.DB.CreateExternalIDMapping(ctx, localpart, serverName, issuer, sub); err != nil {
		// A concurrent first login for the same subject may have won the
		// insert; adopt its mapping instead of failing the request.
		mapped, _, lookupErr := a.DB.GetLocalpartByExternalID(ctx, issuer, sub)
		if lookupErr == nil && mapped != "" {
			logrus.WithFields(logrus.Fields{
				"localpart": mapped,
				"sub":       sub,
			}).Info("MSC3861: adopting concurrently created external ID mapping")
			return mapped, nil
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"sub":       sub,
		}).Error("MSC3861: failed to create external ID mapping")
		return "", err
	}

	logrus.WithFields(logrus.Fields{
		"localpart": localpart,
		"sub":       sub,
	}).Info("MSC3861: created external ID mapping")
	return localpart, nil
}

// queryAccessTokenMSC3861 handles token validation when MSC3861 OIDC delegation is enabled.
func (a *UserInternalAPI) queryAccessTokenMSC3861(ctx context.Context, req *api.QueryAccessTokenRequest, res *api.QueryAccessTokenResponse) error {
	msc3861 := &a.Config.MSCs.MSC3861

	// 1. Check static admin token using a constant-time comparison. An empty
	// configured admin token never matches.
	if msc3861.AdminToken != "" && subtle.ConstantTimeCompare([]byte(req.AccessToken), []byte(msc3861.AdminToken)) == 1 {
		res.Device = &api.Device{
			ID:          "admin",
			UserID:      fmt.Sprintf("@admin:%s", a.Config.Matrix.ServerName),
			AccessToken: req.AccessToken,
			AccountType: api.AccountTypeAdmin,
		}
		return nil
	}

	// 2. Check appservice tokens (pass through unchanged).
	if req.AppServiceUserID != "" {
		appServiceDevice, err := a.queryAppServiceToken(ctx, req.AccessToken, req.AppServiceUserID)
		if err != nil || appServiceDevice != nil {
			if err != nil {
				res.Err = err.Error()
			}
			res.Device = appServiceDevice
			return nil
		}
	}

	// 3. Introspect the token with the OIDC provider.
	introspection, err := introspectToken(ctx, msc3861, req.AccessToken, a.HTTPClient)
	if err != nil {
		if errors.Is(err, errTokenExpired) {
			// The provider rejected the token as expired; tell the client it
			// can refresh instead of destroying the session.
			logrus.Warn("MSC3861: access token has expired (userinfo fallback)")
			res.SoftLogout = true
			return nil
		}
		logrus.WithError(err).Warn("MSC3861: token introspection failed")
		return nil
	}

	// 4. Validate token expiry before the active check: providers that answer
	// active:false for expired tokens must still trigger a soft logout so the
	// client refreshes instead of destroying the session.
	if introspection.Exp > 0 && time.Now().Unix() > introspection.Exp {
		logrus.WithField("exp", introspection.Exp).Warn("MSC3861: access token has expired")
		res.SoftLogout = true
		return nil
	}

	// 5. Inactive token -> return nil device (M_UNKNOWN_TOKEN).
	if !introspection.Active {
		return nil
	}

	// 6. Resolve user identity via external ID mapping.
	sub := introspection.Sub
	if sub == "" {
		logrus.Warn("MSC3861: introspection response has empty sub")
		return nil
	}

	issuer := strings.TrimSuffix(msc3861.Issuer, "/")
	serverName := a.Config.Matrix.ServerName

	// Look up the external ID in the mapping table.
	localpart, _, err := a.DB.GetLocalpartByExternalID(ctx, issuer, sub)
	if err != nil {
		logrus.WithError(err).WithField("sub", sub).Error("MSC3861: failed to look up external ID")
		return err
	}

	if localpart == "" {
		// No mapping exists yet. Derive one from the introspected claims and
		// persist it, refusing to link to colliding pre-existing accounts.
		localpart, err = a.provisionMSC3861Localpart(ctx, introspection, issuer, sub, serverName)
		if err != nil {
			return err
		}
		if localpart == "" {
			return nil
		}
	}

	// 7. Validate the resolved localpart before account provisioning.
	if err := zinternal.ValidateUsername(localpart, serverName); err != nil {
		logrus.WithError(err).WithField("localpart", localpart).Warn("MSC3861: localpart is not valid")
		return nil
	}

	// 8. Refuse tokens for deactivated accounts. PerformAccountCreation with
	// ConflictUpdate would otherwise silently re-attach the session to the
	// deactivated account. A missing account is fine; it is provisioned below.
	deactivated, err := a.DB.IsAccountDeactivated(ctx, localpart, serverName)
	switch {
	case err == nil && deactivated:
		logrus.WithField("localpart", localpart).Warn("MSC3861: refusing token for deactivated account")
		return nil
	case err == nil || errors.Is(err, sql.ErrNoRows):
	default:
		logrus.WithError(err).WithField("localpart", localpart).Error("MSC3861: failed to check account deactivation")
		return err
	}

	// 9. Determine account type from scope.
	accountType := api.AccountTypeUser
	if strings.Contains(introspection.Scope, "urn:synapse:admin:*") {
		accountType = api.AccountTypeAdmin
	}

	// 10. Auto-provision user account if it doesn't exist.
	var createRes api.PerformAccountCreationResponse
	if err := a.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
		AccountType: accountType,
		Localpart:   localpart,
		ServerName:  serverName,
		OnConflict:  api.ConflictUpdate,
	}, &createRes); err != nil {
		logrus.WithError(err).WithField("localpart", localpart).Error("MSC3861: failed to auto-provision user")
		return err
	}

	// 11. Extract device ID from scope and ensure a device row exists. An
	// existing row is never recreated: OIDC access tokens rotate often (MAS
	// defaults to five minutes) and deleting plus re-inserting the row on every
	// rotation would bump the session ID, break /send transaction deduplication
	// and wipe the display name. Only a genuinely missing row is created.
	deviceID := extractDeviceIDFromScope(introspection.Scope)
	if deviceID == "" {
		deviceID = defaultOIDCDeviceID
	}

	existingDevice, err := a.DB.GetDeviceByID(ctx, localpart, serverName, deviceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"device_id": deviceID,
		}).Error("MSC3861: failed to look up device")
		return err
	}
	if err == nil {
		device, bindErr := a.bindMSC3861DeviceToken(ctx, existingDevice, localpart, serverName, deviceID, req.AccessToken)
		if bindErr != nil {
			return bindErr
		}
		if device != nil {
			device.AccountType = accountType
			res.Device = device
			return nil
		}
	}

	var deviceRes api.PerformDeviceCreationResponse
	if err := a.PerformDeviceCreation(ctx, &api.PerformDeviceCreationRequest{
		Localpart:          localpart,
		ServerName:         serverName,
		AccessToken:        req.AccessToken,
		DeviceID:           &deviceID,
		NoDeviceListUpdate: true,
	}, &deviceRes); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"device_id": deviceID,
		}).Error("MSC3861: failed to create/update device")
		return err
	}

	if deviceRes.Device != nil {
		deviceRes.Device.AccountType = accountType
		res.Device = deviceRes.Device
	}

	return nil
}

// bindMSC3861DeviceToken makes an already existing device row usable with the
// presented access token, without ever recreating the row. It returns the
// device to hand back to the caller, or a nil device (and no error) when the
// row could not be bound, in which case the caller falls back to creating one.
func (a *UserInternalAPI) bindMSC3861DeviceToken(
	ctx context.Context, existingDevice *api.Device,
	localpart string, serverName spec.ServerName,
	deviceID, accessToken string,
) (*api.Device, error) {
	tokenDevice, err := a.DB.GetDeviceByAccessToken(ctx, accessToken)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"device_id": deviceID,
		}).Error("MSC3861: failed to look up device by access token")
		return nil, err

	case err == nil && tokenDevice.ID == deviceID && tokenDevice.UserID == existingDevice.UserID:
		// Device and token are unchanged; reuse the row as-is.
		tokenDevice.DisplayName = existingDevice.DisplayName
		return tokenDevice, nil

	case err == nil:
		// The token is still recorded against a *different* device row, e.g.
		// one provisioned before the provider started sending a device scope.
		// The access token is the primary key of the devices table, so that row
		// has to be revoked before the token can be bound to this device.
		if err := a.revokeMSC3861TokenDevice(ctx, tokenDevice); err != nil {
			return nil, err
		}
	}

	// The token rotated. Swap it into the existing row rather than deleting and
	// re-inserting, which would allocate a new session ID.
	if err := a.DB.UpdateDeviceAccessToken(ctx, localpart, serverName, deviceID, accessToken); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"device_id": deviceID,
		}).Error("MSC3861: failed to update device access token")
		return nil, err
	}

	// Re-read by token: only the by-token lookup returns the session ID that
	// /send transaction deduplication keys off.
	device, err := a.DB.GetDeviceByAccessToken(ctx, accessToken)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The row vanished underneath us (concurrent logout); let the
			// caller recreate it.
			return nil, nil
		}
		logrus.WithError(err).WithFields(logrus.Fields{
			"localpart": localpart,
			"device_id": deviceID,
		}).Error("MSC3861: failed to re-read device after access token update")
		return nil, err
	}
	device.DisplayName = existingDevice.DisplayName
	return device, nil
}

// revokeMSC3861TokenDevice deletes the device row that currently holds an
// access token so the token can be bound to a different device.
func (a *UserInternalAPI) revokeMSC3861TokenDevice(ctx context.Context, device *api.Device) error {
	staleLocalpart, staleServerName, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		logrus.WithError(err).WithField("user_id", device.UserID).Error("MSC3861: failed to split user ID of stale token device")
		return err
	}
	logrus.WithFields(logrus.Fields{
		"user_id":   device.UserID,
		"device_id": device.ID,
	}).Warn("MSC3861: access token was bound to a different device, revoking it")
	if err := a.DB.RemoveDevices(ctx, staleLocalpart, staleServerName, []string{device.ID}); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"user_id":   device.UserID,
			"device_id": device.ID,
		}).Error("MSC3861: failed to revoke device holding the presented access token")
		return err
	}
	return nil
}
