// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/userapi/api"
)

// IntrospectionResponse represents the response from an RFC 7662 token introspection endpoint.
type IntrospectionResponse struct {
	Active   bool   `json:"active"`
	Sub      string `json:"sub"`
	Scope    string `json:"scope"`
	Exp      int64  `json:"exp"`
	Username string `json:"username"`
}

// oidcDiscoveryResponse represents the relevant fields from an OIDC discovery document.
type oidcDiscoveryResponse struct {
	IntrospectionEndpoint string `json:"introspection_endpoint"`
}

// cachedIntrospection holds a cached introspection result.
type cachedIntrospection struct {
	response *IntrospectionResponse
	err      error
}

var (
	// IntrospectionCache caches token introspection results for 30 seconds.
	introspectionCache     *ttlcache.Cache[string, *cachedIntrospection]
	introspectionCacheOnce sync.Once

	// DiscoveredEndpoint caches the discovered introspection endpoint.
	discoveredEndpoint     string
	discoveredEndpointTime time.Time
	discoveryMu            sync.Mutex
)

const (
	introspectionCacheTTL     = 30 * time.Second
	introspectionCacheMaxSize = 10_000
	discoveryEndpointCacheTTL = 1 * time.Hour
	deviceScopePrefix         = "urn:matrix:org.matrix.msc2967.client:device:"
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
// encoded as urn:matrix:org.matrix.msc2967.client:device:DEVICEID.
func extractDeviceIDFromScope(scope string) string {
	for _, part := range strings.Fields(scope) {
		if strings.HasPrefix(part, deviceScopePrefix) {
			return strings.TrimPrefix(part, deviceScopePrefix)
		}
	}
	return ""
}

// discoverIntrospectionEndpoint fetches the OIDC discovery document from the issuer
// and returns the introspection_endpoint. Results are cached for 1 hour.
func discoverIntrospectionEndpoint(ctx context.Context, issuer string, httpClient *http.Client) string {
	discoveryMu.Lock()
	defer discoveryMu.Unlock()

	if discoveredEndpoint != "" && time.Since(discoveredEndpointTime) < discoveryEndpointCacheTTL {
		return discoveredEndpoint
	}

	discoveryURL := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryURL, nil)
	if err != nil {
		logrus.WithError(err).Warn("MSC3861: failed to create OIDC discovery request")
		return ""
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		logrus.WithError(err).Warn("MSC3861: OIDC discovery request failed")
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logrus.WithField("status", resp.StatusCode).Warn("MSC3861: OIDC discovery returned non-200 status")
		return ""
	}

	var discovery oidcDiscoveryResponse
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		logrus.WithError(err).Warn("MSC3861: failed to decode OIDC discovery response")
		return ""
	}

	if discovery.IntrospectionEndpoint != "" {
		discoveredEndpoint = discovery.IntrospectionEndpoint
		discoveredEndpointTime = time.Now()
		logrus.WithField("endpoint", discoveredEndpoint).Info("MSC3861: discovered introspection endpoint")
	}

	return discoveredEndpoint
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

	return strings.TrimSuffix(msc3861.Issuer, "/") + "/oauth2/introspect"
}

// introspectToken calls the OIDC introspection endpoint to validate an access token.
func introspectToken(ctx context.Context, msc3861 *config.MSC3861Config, token string, httpClient *http.Client) (*IntrospectionResponse, error) {
	// Check cache first.
	cache := getIntrospectionCache()
	tokenHash := hashToken(token)
	if item := cache.Get(tokenHash); item != nil {
		cached := item.Value()
		return cached.response, cached.err
	}

	endpoint := resolveIntrospectionEndpoint(ctx, msc3861, httpClient)

	form := url.Values{"token": {token}}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("msc3861: failed to create introspection request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	authMethod := msc3861.ClientAuthMethod
	if authMethod == "" {
		authMethod = "client_secret_basic"
	}

	switch authMethod {
	case "client_secret_basic":
		req.SetBasicAuth(url.QueryEscape(msc3861.ClientID), url.QueryEscape(msc3861.ClientSecret))
	case "client_secret_post":
		form.Set("client_id", msc3861.ClientID)
		form.Set("client_secret", msc3861.ClientSecret)
		req.Body = io.NopCloser(strings.NewReader(form.Encode()))
		req.ContentLength = int64(len(form.Encode()))
	default:
		return nil, fmt.Errorf("msc3861: unsupported client_auth_method %q", authMethod)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("msc3861: introspection request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("msc3861: introspection returned status %d: %s", resp.StatusCode, string(body))
	}

	var introspection IntrospectionResponse
	if err := json.NewDecoder(resp.Body).Decode(&introspection); err != nil {
		return nil, fmt.Errorf("msc3861: failed to decode introspection response: %w", err)
	}

	// Cache the result (only cache active tokens and inactive responses, not errors).
	cache.Set(tokenHash, &cachedIntrospection{response: &introspection}, introspectionCacheTTL)

	return &introspection, nil
}

// queryAccessTokenMSC3861 handles token validation when MSC3861 OIDC delegation is enabled.
func (a *UserInternalAPI) queryAccessTokenMSC3861(ctx context.Context, req *api.QueryAccessTokenRequest, res *api.QueryAccessTokenResponse) error {
	msc3861 := &a.Config.MSCs.MSC3861

	// 1. Check static admin token.
	if msc3861.AdminToken != "" && req.AccessToken == msc3861.AdminToken {
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
		logrus.WithError(err).Warn("MSC3861: token introspection failed")
		return nil
	}

	// 4. Inactive token -> return nil device (M_UNKNOWN_TOKEN).
	if !introspection.Active {
		return nil
	}

	// 5. Resolve user identity via external ID mapping.
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
		// No mapping exists yet. Use the username claim from the introspection response.
		localpart = introspection.Username
		if localpart == "" {
			logrus.WithField("sub", sub).Warn("MSC3861: introspection response has no username and no existing external ID mapping")
			return nil
		}

		// Create the external ID mapping.
		if err := a.DB.CreateExternalIDMapping(ctx, localpart, serverName, issuer, sub); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"localpart": localpart,
				"sub":       sub,
			}).Error("MSC3861: failed to create external ID mapping")
			return err
		}

		logrus.WithFields(logrus.Fields{
			"localpart": localpart,
			"sub":       sub,
		}).Info("MSC3861: created external ID mapping")
	}

	// 6. Determine account type from scope.
	accountType := api.AccountTypeUser
	if strings.Contains(introspection.Scope, "urn:synapse:admin:*") {
		accountType = api.AccountTypeAdmin
	}

	// 7. Auto-provision user account if it doesn't exist.
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

	// 8. Extract device ID from scope and create a real device.
	deviceID := extractDeviceIDFromScope(introspection.Scope)
	if deviceID == "" {
		deviceID = "OIDC"
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
