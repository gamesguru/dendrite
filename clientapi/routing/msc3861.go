// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
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

// Bounds the body read on the unauthenticated POST /register endpoint handled
// by the MSC3861 register shim (1 MiB).
const msc3861RegisterBodyMaxBytes = 1 << 20

// msc3861RegisterHandler returns a handler for POST /_matrix/client/v3/register
// when MSC3861 is enabled. Application-service registrations
// (m.login.application_service with an access token) are still allowed; all
// other registration attempts are rejected.
func msc3861RegisterHandler(
	cfg *config.ClientAPI,
	userAPI userapi.ClientUserAPI,
	rateLimits *httputil.RateLimits,
) http.Handler {
	return httputil.MakeExternalAPI("register", func(req *http.Request) util.JSONResponse {
		if req.Body == nil {
			req.Body = http.NoBody
		}
		// The register endpoint is unauthenticated, so the body read is
		// bounded to keep memory usage in check. One extra byte is read so an
		// over-long body can be told apart from one exactly at the limit.
		body, err := io.ReadAll(io.LimitReader(req.Body, msc3861RegisterBodyMaxBytes+1))
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.NotJSON("Unable to read request body"),
			}
		}
		req.Body.Close()
		if len(body) > msc3861RegisterBodyMaxBytes {
			return util.JSONResponse{
				Code: http.StatusRequestEntityTooLarge,
				JSON: spec.BadJSON("Request body too large."),
			}
		}
		// Restore the body so the real registration handler can read it.
		req.Body = io.NopCloser(bytes.NewReader(body))

		// Allow application-service registrations through.
		_, accessTokenErr := auth.ExtractAccessToken(req)
		if gjson.GetBytes(body, "type").String() == authtypes.LoginTypeApplicationService && accessTokenErr == nil {
			return Register(req, userAPI, cfg)
		}

		// Otherwise apply the usual rate limit and reject.
		if r := rateLimits.Limit(req, nil); r != nil {
			return *r
		}
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Registration is delegated to the OIDC provider via MSC3861."),
		}
	})
}

// AuthMetadataResponse is the RFC8414-style authorization server metadata
// returned by GET /_matrix/client/v1/auth_metadata (MSC2965).
type AuthMetadataResponse struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint"`
	RevocationEndpoint                string   `json:"revocation_endpoint"`
	UserinfoEndpoint                  string   `json:"userinfo_endpoint,omitempty"`
	EndSessionEndpoint                string   `json:"end_session_endpoint,omitempty"`
	DeviceAuthorizationEndpoint       string   `json:"device_authorization_endpoint,omitempty"`
	JWKSURI                           string   `json:"jwks_uri"`
	ResponseTypesSupported            []string `json:"response_types_supported"`
	GrantTypesSupported               []string `json:"grant_types_supported"`
	ResponseModesSupported            []string `json:"response_modes_supported"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported"`
	IDTokenSigningAlgValuesSupported  []string `json:"id_token_signing_alg_values_supported"`
	SubjectTypesSupported             []string `json:"subject_types_supported,omitempty"`
	ScopesSupported                   []string `json:"scopes_supported"`
	PromptValuesSupported             []string `json:"prompt_values_supported,omitempty"`
	AccountManagementURI              string   `json:"account_management_uri,omitempty"`
	AccountManagementActionsSupported []string `json:"account_management_actions_supported,omitempty"`
}

// oidcAuthMetadataDoc holds the subset of the OIDC discovery document that we
// need when building the Matrix auth_metadata response.
type oidcAuthMetadataDoc struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint"`
	RevocationEndpoint                string   `json:"revocation_endpoint"`
	EndSessionEndpoint                string   `json:"end_session_endpoint"`
	DeviceAuthorizationEndpoint       string   `json:"device_authorization_endpoint"`
	JWKSURI                           string   `json:"jwks_uri"`
	UserinfoEndpoint                  string   `json:"userinfo_endpoint"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported"`
	IDTokenSigningAlgValuesSupported  []string `json:"id_token_signing_alg_values_supported"`
	SubjectTypesSupported             []string `json:"subject_types_supported"`
	PromptValuesSupported             []string `json:"prompt_values_supported"`
}

const (
	oidcDiscoveryTimeout = 10 * time.Second
	// The auth metadata cache TTL mirrors the userapi discovery cache TTL so
	// the auth_metadata endpoint picks up provider changes within an hour.
	oidcAuthMetadataCacheTTL = time.Hour
)

// oidcAuthMetadataCacheEntry holds a cached discovery document.
type oidcAuthMetadataCacheEntry struct {
	doc       oidcAuthMetadataDoc
	expiresAt time.Time
}

var (
	oidcAuthMetadataCache   = map[string]oidcAuthMetadataCacheEntry{}
	oidcAuthMetadataCacheMu sync.RWMutex
)

// discoverOIDCAuthServerMetadata fetches the OIDC discovery document and returns
// the fields needed for the Matrix auth_metadata response. Results are cached
// for oidcAuthMetadataCacheTTL so that the auth_metadata endpoint does not
// need to perform an outbound request on every hit.
func discoverOIDCAuthServerMetadata(ctx context.Context, issuer string) oidcAuthMetadataDoc {
	return discoverOIDCAuthServerMetadataWithTTL(ctx, issuer, oidcAuthMetadataCacheTTL)
}

// discoverOIDCAuthServerMetadataWithTTL is the testable core of
// discoverOIDCAuthServerMetadata with an explicit cache TTL.
func discoverOIDCAuthServerMetadataWithTTL(ctx context.Context, issuer string, cacheTTL time.Duration) oidcAuthMetadataDoc {
	if issuer == "" {
		return oidcAuthMetadataDoc{}
	}

	// Normalise the configured issuer once, the same way the userapi discovery
	// path does. A trailing slash is not significant for an issuer URL, and
	// normalising here keeps the cache key, the discovery URL and the issuer
	// comparison below consistent: otherwise a configured "https://mas/" would
	// fetch successfully, fail the comparison, never be cached, and trigger a
	// fresh outbound request on every auth_metadata hit.
	issuer = strings.TrimSuffix(issuer, "/")

	oidcAuthMetadataCacheMu.RLock()
	cached, ok := oidcAuthMetadataCache[issuer]
	oidcAuthMetadataCacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.doc
	}

	queryCtx, cancel := context.WithTimeout(ctx, oidcDiscoveryTimeout)
	defer cancel()

	discoveryURL := issuer + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(queryCtx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		return oidcAuthMetadataDoc{}
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return oidcAuthMetadataDoc{}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return oidcAuthMetadataDoc{}
	}

	var doc oidcAuthMetadataDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return oidcAuthMetadataDoc{}
	}

	// A discovery document whose issuer does not match the configured issuer
	// is not trustworthy: neither cache nor use it. The caller still serves
	// the config-derived values in that case. Both sides are compared
	// normalised so that a trailing slash on either one is not a mismatch.
	if strings.TrimSuffix(doc.Issuer, "/") != issuer {
		logrus.WithFields(logrus.Fields{
			"configured_issuer": issuer,
			"discovered_issuer": doc.Issuer,
		}).Warn("MSC3861: discovered issuer does not match configured issuer")
		return oidcAuthMetadataDoc{}
	}

	oidcAuthMetadataCacheMu.Lock()
	oidcAuthMetadataCache[issuer] = oidcAuthMetadataCacheEntry{
		doc:       doc,
		expiresAt: time.Now().Add(cacheTTL),
	}
	oidcAuthMetadataCacheMu.Unlock()

	return doc
}

// authIssuerResponse is the JSON body for GET /_matrix/client/v1/auth_issuer.
type authIssuerResponse struct {
	Issuer string `json:"issuer"`
}

// msc3861AuthIssuer returns the OIDC issuer URL when MSC3861 is active.
func msc3861AuthIssuer(mscCfg *config.MSCs) util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusOK,
		Headers: map[string]string{
			"Cache-Control": "public, max-age=3600",
		},
		JSON: authIssuerResponse{Issuer: mscCfg.MSC3861.Issuer},
	}
}

// msc3861AuthMetadata returns the authorization server metadata when MSC3861 is active.
// The JWKS URI is rewritten to point at a homeserver-hosted proxy so that browser
// clients can fetch the keys without relying on the OIDC provider's CORS policy.
func msc3861AuthMetadata(req *http.Request, mscCfg *config.MSCs) util.JSONResponse {
	cfg := &mscCfg.MSC3861

	doc := discoverOIDCAuthServerMetadata(req.Context(), cfg.Issuer)

	registrationEndpoint := firstNonEmpty(cfg.RegistrationEndpoint, doc.RegistrationEndpoint, cfg.RegistrationEndpointOrDefault())
	userinfoEndpoint := firstNonEmpty(cfg.UserinfoEndpoint, doc.UserinfoEndpoint, cfg.UserinfoEndpointOrDefault())
	endSessionEndpoint := firstNonEmpty(cfg.EndSessionEndpoint, doc.EndSessionEndpoint)
	deviceAuthorizationEndpoint := firstNonEmpty(cfg.DeviceAuthorizationEndpoint, doc.DeviceAuthorizationEndpoint, cfg.DeviceAuthorizationEndpointOrDefault())

	grantTypes := []string{"authorization_code", "refresh_token"}
	if deviceAuthorizationEndpoint != "" {
		grantTypes = append(grantTypes, "urn:ietf:params:oauth:grant-type:device_code")
	}

	tokenEndpointAuthMethods := doc.TokenEndpointAuthMethodsSupported
	if len(tokenEndpointAuthMethods) == 0 {
		tokenEndpointAuthMethods = []string{"client_secret_basic", "client_secret_post", "none"}
	}

	idTokenSigningAlgs := doc.IDTokenSigningAlgValuesSupported
	if len(idTokenSigningAlgs) == 0 {
		idTokenSigningAlgs = []string{"RS256"}
	}

	subjectTypes := doc.SubjectTypesSupported
	if len(subjectTypes) == 0 {
		subjectTypes = []string{"public"}
	}

	var jwksProxyBase string
	if cfg.PublicBaseURL != "" {
		// Prefer the configured public base URL so that a forged Host header
		// cannot poison shared caches of the auth metadata document.
		jwksProxyBase = strings.TrimSuffix(cfg.PublicBaseURL, "/")
	} else {
		// Fall back to the incoming request's Host. This trusts the Host and
		// X-Forwarded-Proto headers, which a reverse proxy must sanitize;
		// set public_base_url to drop that assumption.
		jwksProxyBase = requestScheme(req) + "://" + req.Host
	}
	jwksProxyURL := jwksProxyBase + "/_matrix/client/v1/auth_metadata/jwks"

	return util.JSONResponse{
		Code: http.StatusOK,
		Headers: map[string]string{
			"Cache-Control": "public, max-age=3600",
		},
		JSON: AuthMetadataResponse{
			Issuer:                            cfg.Issuer,
			AuthorizationEndpoint:             firstNonEmpty(cfg.AuthorizationEndpoint, doc.AuthorizationEndpoint, cfg.AuthorizationEndpointOrDefault()),
			TokenEndpoint:                     firstNonEmpty(cfg.TokenEndpoint, doc.TokenEndpoint, cfg.TokenEndpointOrDefault()),
			RegistrationEndpoint:              registrationEndpoint,
			RevocationEndpoint:                firstNonEmpty(cfg.RevocationEndpoint, doc.RevocationEndpoint, cfg.RevocationEndpointOrDefault()),
			UserinfoEndpoint:                  userinfoEndpoint,
			EndSessionEndpoint:                endSessionEndpoint,
			DeviceAuthorizationEndpoint:       deviceAuthorizationEndpoint,
			JWKSURI:                           jwksProxyURL,
			ResponseTypesSupported:            []string{"code"},
			GrantTypesSupported:               grantTypes,
			ResponseModesSupported:            []string{"query", "fragment"},
			CodeChallengeMethodsSupported:     []string{"S256"},
			TokenEndpointAuthMethodsSupported: tokenEndpointAuthMethods,
			IDTokenSigningAlgValuesSupported:  idTokenSigningAlgs,
			SubjectTypesSupported:             subjectTypes,
			PromptValuesSupported:             doc.PromptValuesSupported,
			AccountManagementURI:              cfg.AccountManagementURL,
			AccountManagementActionsSupported: cfg.AccountManagementActions,
			ScopesSupported: []string{
				"openid",
				"offline_access",
				"urn:matrix:client:api:*",
			},
		},
	}
}

// jwksProxyCacheEntry holds a cached JWKS response.
type jwksProxyCacheEntry struct {
	body      []byte
	expiresAt time.Time
}

var (
	jwksProxyCache    = map[string]jwksProxyCacheEntry{}
	jwksProxyCacheMu  sync.RWMutex
	jwksProxyCacheTTL = 15 * time.Minute
)

// resolveJWKSUpstream returns the upstream JWKS URI to proxy, preferring the
// configured override, then OIDC discovery, then the default fallback.
func resolveJWKSUpstream(cfg *config.MSC3861Config, doc oidcAuthMetadataDoc) string {
	if cfg.JWKSURI != "" {
		return cfg.JWKSURI
	}
	if doc.JWKSURI != "" {
		return doc.JWKSURI
	}
	return cfg.JWKSURIOrDefault()
}

// jsonRaw is a json.Marshaler that writes the contained bytes verbatim.
type jsonRaw []byte

func (r jsonRaw) MarshalJSON() ([]byte, error) {
	return []byte(r), nil
}

const (
	// Bound on the upstream JWKS response body that we read (1 MiB).
	jwksMaxBodySize = 1 << 20
	// The max-age advertised when serving a stale cached JWKS after a failed
	// revalidation, so clients retry soon.
	jwksStaleMaxAgeSeconds = 60
)

// isValidJWKS reports whether body decodes as JSON with a top-level "keys"
// array, as required by RFC 7517.
func isValidJWKS(body []byte) bool {
	if !json.Valid(body) {
		return false
	}
	return gjson.GetBytes(body, "keys").IsArray()
}

// serveStaleJWKS serves an expired cache entry after a failed revalidation.
func serveStaleJWKS(upstream string) (util.JSONResponse, bool) {
	jwksProxyCacheMu.RLock()
	cached, ok := jwksProxyCache[upstream]
	jwksProxyCacheMu.RUnlock()
	if !ok {
		return util.JSONResponse{}, false
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Cache-Control": fmt.Sprintf("public, max-age=%d", jwksStaleMaxAgeSeconds),
		},
		JSON: jsonRaw(cached.body),
	}, true
}

// msc3861JWKSProxy proxies the OIDC provider's JWKS through the homeserver so
// that browser clients can fetch it without being blocked by CORS.
func msc3861JWKSProxy(req *http.Request, mscCfg *config.MSCs) util.JSONResponse {
	return jwksProxyWithTTL(req, mscCfg, jwksProxyCacheTTL)
}

// jwksProxyWithTTL is the testable core of msc3861JWKSProxy with an explicit
// cache TTL.
func jwksProxyWithTTL(req *http.Request, mscCfg *config.MSCs, cacheTTL time.Duration) util.JSONResponse {
	cfg := &mscCfg.MSC3861
	upstream := resolveJWKSUpstream(cfg, discoverOIDCAuthServerMetadata(req.Context(), cfg.Issuer))
	if upstream == "" {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "JWKS URI not configured or discoverable"},
		}
	}

	jwksProxyCacheMu.RLock()
	cached, ok := jwksProxyCache[upstream]
	jwksProxyCacheMu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return util.JSONResponse{
			Code: http.StatusOK,
			Headers: map[string]string{
				"Content-Type":  "application/json",
				"Cache-Control": fmt.Sprintf("public, max-age=%d", int(cacheTTL.Seconds())),
			},
			JSON: jsonRaw(cached.body),
		}
	}

	ctx, cancel := context.WithTimeout(req.Context(), oidcDiscoveryTimeout)
	defer cancel()

	discoveryReq, err := http.NewRequestWithContext(ctx, http.MethodGet, upstream, nil)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: "failed to build JWKS request"},
		}
	}
	discoveryReq.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(discoveryReq)
	if err != nil {
		logrus.WithError(err).Error("MSC3861: failed to fetch upstream JWKS")
		if stale, served := serveStaleJWKS(upstream); served {
			return stale
		}
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "failed to fetch upstream JWKS"},
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBodySize))
		logrus.WithFields(logrus.Fields{
			"status":   resp.StatusCode,
			"response": string(body),
		}).Error("MSC3861: upstream JWKS returned non-200 status")
		if stale, served := serveStaleJWKS(upstream); served {
			return stale
		}
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "upstream JWKS returned non-200 status"},
		}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBodySize))
	if err != nil {
		logrus.WithError(err).Error("MSC3861: failed to read upstream JWKS")
		if stale, served := serveStaleJWKS(upstream); served {
			return stale
		}
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "failed to read upstream JWKS"},
		}
	}

	// Sanity-check the payload before caching or serving it: an upstream
	// returning an error page must not be cached as the JWKS.
	if !isValidJWKS(body) {
		logrus.Error("MSC3861: upstream JWKS payload is not a valid JWKS document")
		if stale, served := serveStaleJWKS(upstream); served {
			return stale
		}
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.InternalServerError{Err: "upstream JWKS payload is not a valid JWKS document"},
		}
	}

	jwksProxyCacheMu.Lock()
	jwksProxyCache[upstream] = jwksProxyCacheEntry{
		body:      body,
		expiresAt: time.Now().Add(cacheTTL),
	}
	jwksProxyCacheMu.Unlock()

	return util.JSONResponse{
		Code: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Cache-Control": fmt.Sprintf("public, max-age=%d", int(cacheTTL.Seconds())),
		},
		JSON: jsonRaw(body),
	}
}

// firstNonEmpty returns the first non-empty string in the given list.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
