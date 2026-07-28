// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// verifyMASAdminToken checks the Authorization header for a valid MAS admin token.
// Returns a JSON error response if verification fails, or nil if successful.
func verifyMASAdminToken(req *http.Request, adminToken string) *util.JSONResponse {
	// Unreachable today: routing.go only registers the MAS admin routes when
	// an admin token is configured. Kept as defense in depth so that the
	// helper fails closed rather than comparing against the empty string if a
	// future caller ever registers these routes without that gate.
	if adminToken == "" {
		res := util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("MAS admin token not configured"),
		}
		return &res
	}

	auth := req.Header.Get("Authorization")
	if auth == "" {
		res := util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.MissingToken("Missing access token"),
		}
		return &res
	}

	const bearerPrefix = "Bearer "
	if len(auth) <= len(bearerPrefix) || auth[:len(bearerPrefix)] != bearerPrefix {
		res := util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: spec.MissingToken("Invalid Authorization header"),
		}
		return &res
	}

	token := auth[len(bearerPrefix):]
	if subtle.ConstantTimeCompare([]byte(token), []byte(adminToken)) != 1 {
		res := util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Invalid admin token"),
		}
		return &res
	}

	return nil
}

// extractUserIDLocalpart parses a full Matrix user ID (@user:server) and returns the localpart.
// Returns a JSON error response if the user ID is malformed or not local.
func extractUserIDLocalpart(userID string, cfg *config.ClientAPI) (string, spec.ServerName, *util.JSONResponse) {
	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		res := util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Invalid user ID: " + userID),
		}
		return "", "", &res
	}
	if !cfg.Matrix.IsLocalServerName(domain) {
		res := util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("User ID does not belong to this server"),
		}
		return "", "", &res
	}
	return localpart, domain, nil
}

// setupMASAdminRoutes registers the Synapse-compatible admin endpoints that MAS calls.
func setupMASAdminRoutes(
	synapseAdminRouter *httputil.Router,
	mscCfg *config.MSCs,
	cfg *config.ClientAPI,
	userAPI userapi.ClientUserAPI,
	rateLimits *httputil.RateLimits,
) {
	adminToken := mscCfg.MSC3861.AdminToken

	// GET /_synapse/admin/v1/username_available
	synapseAdminRouter.Handle("/admin/v1/username_available",
		httputil.MakeExternalAPI("mas_username_available", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASCheckUsernameAvailable(req, cfg, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	// PUT /_synapse/admin/v1/users/{userId}
	synapseAdminRouter.Handle("/admin/v1/users/{userId}",
		httputil.MakeExternalAPI("mas_create_or_update_user", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			switch req.Method {
			case http.MethodPut:
				return MASCreateOrUpdateUser(req, cfg, userAPI)
			case http.MethodGet:
				return MASGetUser(req, cfg, userAPI)
			default:
				return util.JSONResponse{
					Code: http.StatusMethodNotAllowed,
					JSON: spec.NotFound("unknown method"),
				}
			}
		}),
	).Methods(http.MethodPut, http.MethodGet, http.MethodOptions)

	// PUT/DELETE /_synapse/admin/v1/users/{userId}/devices/{deviceId}
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/devices/{deviceId}",
		httputil.MakeExternalAPI("mas_device", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			switch req.Method {
			case http.MethodPut:
				return MASCreateOrGetDevice(req, cfg, userAPI)
			case http.MethodDelete:
				return MASDeleteDevice(req, cfg, userAPI)
			default:
				return util.JSONResponse{
					Code: http.StatusMethodNotAllowed,
					JSON: spec.NotFound("unknown method"),
				}
			}
		}),
	).Methods(http.MethodPut, http.MethodDelete, http.MethodOptions)

	// DELETE /_synapse/admin/v1/users/{userId}/devices
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/devices",
		httputil.MakeExternalAPI("mas_delete_all_devices", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASDeleteAllDevices(req, cfg, userAPI)
		}),
	).Methods(http.MethodDelete, http.MethodOptions)

	// POST /_synapse/admin/v1/users/{userId}/_deactivate
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/_deactivate",
		httputil.MakeExternalAPI("mas_deactivate_user", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASDeactivateUser(req, cfg, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	// POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia",
		httputil.MakeExternalAPI("mas_allow_cross_signing_replacement", func(req *http.Request) util.JSONResponse {
			if r := rateLimits.Limit(req, nil); r != nil {
				return *r
			}
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASAllowCrossSigningReplacement(req, cfg)
		}),
	).Methods(http.MethodPost, http.MethodOptions)
}

// MASCheckUsernameAvailable checks if a username is available for registration.
// GET /_synapse/admin/v1/username_available?username=foo.
func MASCheckUsernameAvailable(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	username := req.URL.Query().Get("username")
	if username == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'username' query parameter"),
		}
	}

	var availRes userapi.QueryAccountAvailabilityResponse
	if err := userAPI.QueryAccountAvailability(req.Context(), &userapi.QueryAccountAvailabilityRequest{
		Localpart:  username,
		ServerName: cfg.Matrix.ServerName,
	}, &availRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to check username availability")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			Available bool `json:"available"`
		}{Available: availRes.Available},
	}
}

// masUserRequest is the JSON body for PUT /_synapse/admin/v1/users/{userId}.
type masUserRequest struct {
	DisplayName *string `json:"displayname,omitempty"`
	AvatarURL   *string `json:"avatar_url,omitempty"`
	Admin       *bool   `json:"admin,omitempty"`
	Deactivated *bool   `json:"deactivated,omitempty"`
}

// MASCreateOrUpdateUser creates or updates a user account.
// PUT /_synapse/admin/v1/users/{userId}.
func MASCreateOrUpdateUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]

	localpart, serverName, errRes := extractUserIDLocalpart(userID, cfg)
	if errRes != nil {
		return *errRes
	}

	var body masUserRequest
	if req.Body != nil {
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("Failed to parse request body: " + err.Error()),
			}
		}
	}

	accountType := userapi.AccountTypeUser
	if body.Admin != nil && *body.Admin {
		accountType = userapi.AccountTypeAdmin
	}

	// Create or update the account.
	var createRes userapi.PerformAccountCreationResponse
	if err := userAPI.PerformAccountCreation(req.Context(), &userapi.PerformAccountCreationRequest{
		AccountType: accountType,
		Localpart:   localpart,
		ServerName:  serverName,
		OnConflict:  userapi.ConflictUpdate,
	}, &createRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to create/update user")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Update display name if provided.
	if body.DisplayName != nil {
		if _, _, err := userAPI.SetDisplayName(req.Context(), localpart, serverName, *body.DisplayName); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to set display name")
		}
	}

	// Update avatar URL if provided.
	if body.AvatarURL != nil {
		if _, _, err := userAPI.SetAvatarURL(req.Context(), localpart, serverName, *body.AvatarURL); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to set avatar URL")
		}
	}

	// Handle deactivation.
	if body.Deactivated != nil && *body.Deactivated {
		var deactivateRes userapi.PerformAccountDeactivationResponse
		if err := userAPI.PerformAccountDeactivation(req.Context(), &userapi.PerformAccountDeactivationRequest{
			Localpart:  localpart,
			ServerName: serverName,
		}, &deactivateRes); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to deactivate user")
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			Name           string `json:"name"`
			DisplayName    string `json:"displayname,omitempty"`
			AvatarURL      string `json:"avatar_url,omitempty"`
			Admin          bool   `json:"admin"`
			Deactivated    bool   `json:"deactivated"`
			AccountCreated bool   `json:"account_created"`
		}{
			Name:           userID,
			Admin:          accountType == userapi.AccountTypeAdmin,
			Deactivated:    body.Deactivated != nil && *body.Deactivated,
			AccountCreated: createRes.AccountCreated,
		},
	}
}

// accountByLocalpartQuerier is implemented by the full user internal API. It
// is asserted at runtime so these handlers only depend on ClientUserAPI.
type accountByLocalpartQuerier interface {
	QueryAccountByLocalpart(ctx context.Context, req *userapi.QueryAccountByLocalpartRequest, res *userapi.QueryAccountByLocalpartResponse) error
}

// accountDeactivationQuerier is implemented by the full user internal API. It
// is asserted at runtime so these handlers only depend on ClientUserAPI.
type accountDeactivationQuerier interface {
	IsAccountDeactivated(ctx context.Context, localpart string, serverName spec.ServerName) (bool, error)
}

// MASGetUser retrieves information about a user account.
// GET /_synapse/admin/v1/users/{userId}.
func MASGetUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]

	localpart, serverName, errRes := extractUserIDLocalpart(userID, cfg)
	if errRes != nil {
		return *errRes
	}

	// The account record is authoritative for existence and account type.
	querier, ok := userAPI.(accountByLocalpartQuerier)
	if !ok {
		logrus.Error("MAS admin: user API does not support QueryAccountByLocalpart")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var accRes userapi.QueryAccountByLocalpartResponse
	err = querier.QueryAccountByLocalpart(req.Context(), &userapi.QueryAccountByLocalpartRequest{
		Localpart:  localpart,
		ServerName: serverName,
	}, &accRes)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not found"),
		}
	case err != nil:
		logrus.WithError(err).Error("MAS admin: failed to query account")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	case accRes.Account == nil:
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not found"),
		}
	}

	// The profile only carries cosmetic fields; a missing profile must not
	// hide an existing account.
	var displayName, avatarURL string
	profile, err := userAPI.QueryProfile(req.Context(), userID)
	if err != nil {
		logrus.WithError(err).WithField("user_id", userID).Warn("MAS admin: failed to query profile")
	} else {
		displayName = profile.DisplayName
		avatarURL = profile.AvatarURL
	}

	// The deactivated flag lives on a separate query; a failure here must not
	// hide the account either.
	var deactivated bool
	if dq, ok := userAPI.(accountDeactivationQuerier); ok {
		deactivated, err = dq.IsAccountDeactivated(req.Context(), localpart, serverName)
		if err != nil {
			logrus.WithError(err).WithField("user_id", userID).Warn("MAS admin: failed to query deactivation state")
			deactivated = false
		}
	} else {
		logrus.Error("MAS admin: user API does not support IsAccountDeactivated")
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			Name        string `json:"name"`
			DisplayName string `json:"displayname,omitempty"`
			AvatarURL   string `json:"avatar_url,omitempty"`
			Admin       bool   `json:"admin"`
			Deactivated bool   `json:"deactivated"`
		}{
			Name:        userID,
			DisplayName: displayName,
			AvatarURL:   avatarURL,
			Admin:       accRes.Account.AccountType == userapi.AccountTypeAdmin,
			Deactivated: deactivated,
		},
	}
}

// masDeviceRequest is the JSON body for PUT /_synapse/admin/v1/users/{userId}/devices/{deviceId}.
type masDeviceRequest struct {
	DisplayName *string `json:"display_name,omitempty"`
}

// MASCreateOrGetDevice creates or retrieves a device.
// PUT /_synapse/admin/v1/users/{userId}/devices/{deviceId}.
func MASCreateOrGetDevice(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]
	deviceID := vars["deviceId"]

	localpart, serverName, errRes := extractUserIDLocalpart(userID, cfg)
	if errRes != nil {
		return *errRes
	}

	var body masDeviceRequest
	if req.Body != nil {
		if decodeErr := json.NewDecoder(req.Body).Decode(&body); decodeErr != nil {
			// Ignore decode errors -- body is optional.
			body = masDeviceRequest{}
		}
	}

	// Look up the device first: PerformDeviceCreation deletes and re-inserts
	// the device row, which would clobber a real access token already stored
	// by the introspection path. Only create the device when it is missing.
	var queryRes userapi.QueryDevicesResponse
	if err := userAPI.QueryDevices(req.Context(), &userapi.QueryDevicesRequest{UserID: userID}, &queryRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to query devices")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	for _, existing := range queryRes.Devices {
		if existing.ID == deviceID {
			return util.JSONResponse{
				Code: http.StatusOK,
				JSON: struct {
					DeviceID    string `json:"device_id"`
					DisplayName string `json:"display_name,omitempty"`
				}{
					DeviceID:    existing.ID,
					DisplayName: existing.DisplayName,
				},
			}
		}
	}

	// Generate a placeholder access token since the DB requires unique tokens.
	// MAS will supply the real token during OIDC login flows.
	placeholderToken, err := generatePlaceholderToken()
	if err != nil {
		logrus.WithError(err).Error("MAS admin: failed to generate placeholder access token")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var deviceRes userapi.PerformDeviceCreationResponse
	if err := userAPI.PerformDeviceCreation(req.Context(), &userapi.PerformDeviceCreationRequest{
		Localpart:          localpart,
		ServerName:         serverName,
		AccessToken:        placeholderToken,
		DeviceID:           &deviceID,
		DeviceDisplayName:  body.DisplayName,
		NoDeviceListUpdate: true,
	}, &deviceRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to create device")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			DeviceID    string `json:"device_id"`
			DisplayName string `json:"display_name,omitempty"`
		}{
			DeviceID:    deviceID,
			DisplayName: deviceRes.Device.DisplayName,
		},
	}
}

// MASDeleteDevice deletes a specific device.
// DELETE /_synapse/admin/v1/users/{userId}/devices/{deviceId}.
func MASDeleteDevice(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]
	deviceID := vars["deviceId"]

	if _, _, errRes := extractUserIDLocalpart(userID, cfg); errRes != nil {
		return *errRes
	}

	var deleteRes userapi.PerformDeviceDeletionResponse
	if err := userAPI.PerformDeviceDeletion(req.Context(), &userapi.PerformDeviceDeletionRequest{
		UserID:    userID,
		DeviceIDs: []string{deviceID},
	}, &deleteRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to delete device")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// MASDeleteAllDevices deletes all devices for a user.
// DELETE /_synapse/admin/v1/users/{userId}/devices.
func MASDeleteAllDevices(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]

	if _, _, errRes := extractUserIDLocalpart(userID, cfg); errRes != nil {
		return *errRes
	}

	// Empty DeviceIDs means "delete all".
	var deleteRes userapi.PerformDeviceDeletionResponse
	if err := userAPI.PerformDeviceDeletion(req.Context(), &userapi.PerformDeviceDeletionRequest{
		UserID:    userID,
		DeviceIDs: []string{},
	}, &deleteRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to delete all devices")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// MASDeactivateUser deactivates a user account.
// POST /_synapse/admin/v1/users/{userId}/_deactivate.
func MASDeactivateUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]

	localpart, serverName, errRes := extractUserIDLocalpart(userID, cfg)
	if errRes != nil {
		return *errRes
	}

	var deactivateRes userapi.PerformAccountDeactivationResponse
	if err := userAPI.PerformAccountDeactivation(req.Context(), &userapi.PerformAccountDeactivationRequest{
		Localpart:  localpart,
		ServerName: serverName,
	}, &deactivateRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to deactivate user")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			IDServerUnbindResult string `json:"id_server_unbind_result"`
		}{
			IDServerUnbindResult: "no-support",
		},
	}
}

// masCrossSigningConfirmationTTL is how long a cross-signing replacement
// confirmation recorded via the MAS admin API remains valid.
const masCrossSigningConfirmationTTL = 10 * time.Minute

var (
	masCrossSigningConfirmationsMu sync.Mutex
	// Expiry of the recorded cross-signing reset confirmation, per user ID.
	masCrossSigningConfirmations = map[string]time.Time{}
)

// recordMASCrossSigningConfirmation records that the OIDC provider confirmed
// the user completed the cross-signing reset web flow.
func recordMASCrossSigningConfirmation(userID string) {
	masCrossSigningConfirmationsMu.Lock()
	defer masCrossSigningConfirmationsMu.Unlock()
	masCrossSigningConfirmations[userID] = time.Now().Add(masCrossSigningConfirmationTTL)
}

// consumeMASCrossSigningConfirmation returns true once if an unexpired
// confirmation exists for the user, consuming it in the process.
func consumeMASCrossSigningConfirmation(userID string) bool {
	masCrossSigningConfirmationsMu.Lock()
	defer masCrossSigningConfirmationsMu.Unlock()
	expiresAt, ok := masCrossSigningConfirmations[userID]
	if !ok || time.Now().After(expiresAt) {
		delete(masCrossSigningConfirmations, userID)
		return false
	}
	delete(masCrossSigningConfirmations, userID)
	return true
}

// MASAllowCrossSigningReplacement records that the user confirmed a
// cross-signing key replacement in the OIDC provider's account management UI.
// The next m.oauth UIA resubmission for this user consumes the confirmation.
// POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia.
func MASAllowCrossSigningReplacement(req *http.Request, cfg *config.ClientAPI) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	userID := vars["userId"]

	if _, _, errRes := extractUserIDLocalpart(userID, cfg); errRes != nil {
		return *errRes
	}

	recordMASCrossSigningConfirmation(userID)

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			UpdatedAt int64 `json:"updated_at"`
		}{
			UpdatedAt: time.Now().UnixMilli(),
		},
	}
}

// generatePlaceholderToken creates a unique placeholder access token for
// admin-created devices. A failure to read from the system CSPRNG is fatal for
// the request rather than something to work around: the placeholder is stored
// as a device access token, so any guessable fallback (a timestamp, say) would
// be a usable credential for that device until MAS overwrites it.
func generatePlaceholderToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "mas_" + hex.EncodeToString(b), nil
}
