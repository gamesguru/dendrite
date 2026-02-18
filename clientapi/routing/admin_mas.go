// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
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
	if token != adminToken {
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
) {
	adminToken := mscCfg.MSC3861.AdminToken

	// GET /_synapse/admin/v1/username_available
	synapseAdminRouter.Handle("/admin/v1/username_available",
		httputil.MakeExternalAPI("mas_username_available", func(req *http.Request) util.JSONResponse {
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASCheckUsernameAvailable(req, cfg, userAPI)
		}),
	).Methods(http.MethodGet, http.MethodOptions)

	// PUT /_synapse/admin/v1/users/{userId}
	synapseAdminRouter.Handle("/admin/v1/users/{userId}",
		httputil.MakeExternalAPI("mas_create_or_update_user", func(req *http.Request) util.JSONResponse {
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
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASDeleteAllDevices(req, cfg, userAPI)
		}),
	).Methods(http.MethodDelete, http.MethodOptions)

	// POST /_synapse/admin/v1/users/{userId}/_deactivate
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/_deactivate",
		httputil.MakeExternalAPI("mas_deactivate_user", func(req *http.Request) util.JSONResponse {
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASDeactivateUser(req, cfg, userAPI)
		}),
	).Methods(http.MethodPost, http.MethodOptions)

	// POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia
	synapseAdminRouter.Handle("/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia",
		httputil.MakeExternalAPI("mas_allow_cross_signing_replacement", func(req *http.Request) util.JSONResponse {
			if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
				return *errRes
			}
			return MASAllowCrossSigningReplacement(req)
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

	profile, err := userAPI.QueryProfile(req.Context(), userID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not found"),
		}
	}

	// Check if account exists and get its type.
	var availRes userapi.QueryAccountAvailabilityResponse
	if err := userAPI.QueryAccountAvailability(req.Context(), &userapi.QueryAccountAvailabilityRequest{
		Localpart:  localpart,
		ServerName: serverName,
	}, &availRes); err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
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
			DisplayName: profile.DisplayName,
			AvatarURL:   profile.AvatarURL,
			// We can't directly determine admin status or deactivation from the profile query;
			// returning defaults. MAS uses this for informational purposes.
			Admin:       false,
			Deactivated: false,
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

	// Generate a placeholder access token since the DB requires unique tokens.
	// MAS will supply the real token during OIDC login flows.
	placeholderToken := generatePlaceholderToken()

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

// MASAllowCrossSigningReplacement is a stub that returns success.
// POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia.
func MASAllowCrossSigningReplacement(req *http.Request) util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			UpdatedAt int64 `json:"updated_at"`
		}{
			UpdatedAt: time.Now().UnixMilli(),
		},
	}
}

// generatePlaceholderToken creates a unique placeholder access token for admin-created devices.
func generatePlaceholderToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// Fallback should never happen, but just in case.
		return "mas_placeholder_" + time.Now().Format("20060102150405.000000000")
	}
	return "mas_" + hex.EncodeToString(b)
}
