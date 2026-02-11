// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"codefloe.com/pat-s/gomatrixserverlib"
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
	DeviceID string `json:"device_id"`
}

// introspectToken calls the OIDC introspection endpoint to validate an access token.
func introspectToken(ctx context.Context, msc3861 *config.MSC3861Config, token string, httpClient *http.Client) (*IntrospectionResponse, error) {
	endpoint := msc3861.IntrospectionEndpoint
	if endpoint == "" {
		endpoint = strings.TrimSuffix(msc3861.Issuer, "/") + "/oauth2/introspect"
	}

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

	// 5. Parse the sub claim to extract localpart and domain.
	sub := introspection.Sub
	if sub == "" {
		logrus.Warn("MSC3861: introspection response has empty sub")
		return nil
	}

	localpart, domain, err := gomatrixserverlib.SplitID('@', sub)
	if err != nil {
		logrus.WithError(err).WithField("sub", sub).Warn("MSC3861: failed to parse sub as user ID")
		return nil
	}

	if !a.Config.Matrix.IsLocalServerName(domain) {
		logrus.WithField("sub", sub).Warn("MSC3861: introspected token belongs to non-local user")
		return nil
	}

	// 6. Auto-provision user if not found.
	accountType := api.AccountTypeUser
	if strings.Contains(introspection.Scope, "urn:synapse:admin:*") {
		accountType = api.AccountTypeAdmin
	}

	var createRes api.PerformAccountCreationResponse
	if err := a.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
		AccountType: accountType,
		Localpart:   localpart,
		ServerName:  domain,
		OnConflict:  api.ConflictUpdate,
	}, &createRes); err != nil {
		logrus.WithError(err).WithField("localpart", localpart).Error("MSC3861: failed to auto-provision user")
		return err
	}

	// 7. Build device from introspection data.
	deviceID := introspection.DeviceID
	if deviceID == "" {
		deviceID = "OIDC"
	}

	res.Device = &api.Device{
		ID:          deviceID,
		UserID:      fmt.Sprintf("@%s:%s", localpart, domain),
		AccessToken: req.AccessToken,
		AccountType: accountType,
	}

	return nil
}
