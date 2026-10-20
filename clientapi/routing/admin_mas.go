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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	clienthttputil "codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/clientapi/userutil"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// This file implements the modern MAS provisioning API served under
// /_synapse/mas, which MAS calls when configured with
// matrix.kind: synapse (or synapse_modern / synapse_read_only).
// The contract mirrors Synapse's synapse/rest/synapse/mas handlers:
// every endpoint authenticates with the shared secret as a bearer token,
// returns Matrix error JSON (errcode) on failure and {} on success, and
// reports unknown users with 404 M_NOT_FOUND.

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

// masRoute wraps a MAS admin endpoint handler with per-caller rate limiting
// and bearer-token auth. The limiter runs first so that guessing the admin
// token is throttled like any other unauthenticated request.
func masRoute(name, adminToken string, rateLimits *httputil.RateLimits, h func(*http.Request) util.JSONResponse) http.Handler {
	return httputil.MakeExternalAPI("mas_"+name, func(req *http.Request) util.JSONResponse {
		if r := rateLimits.Limit(req, nil); r != nil {
			return *r
		}
		if errRes := verifyMASAdminToken(req, adminToken); errRes != nil {
			return *errRes
		}
		return h(req)
	})
}

// setupMASAdminRoutes registers the /_synapse/mas provisioning endpoints that
// MAS calls with its shared secret as a bearer token.
func setupMASAdminRoutes(
	synapseAdminRouter *httputil.Router,
	mscCfg *config.MSCs,
	cfg *config.ClientAPI,
	userAPI userapi.ClientUserAPI,
	rateLimits *httputil.RateLimits,
) {
	adminToken := mscCfg.MSC3861.AdminToken

	handle := func(path, name string, h masHandler, methods ...string) {
		synapseAdminRouter.Handle(path, masRoute(name, adminToken, rateLimits, func(req *http.Request) util.JSONResponse {
			return h(req, cfg, userAPI)
		})).Methods(methods...)
	}

	handle("/mas/query_user", "query_user", MASQueryUser, http.MethodGet, http.MethodOptions)
	handle("/mas/provision_user", "provision_user", MASProvisionUser, http.MethodPost, http.MethodOptions)
	handle("/mas/is_localpart_available", "is_localpart_available", MASIsLocalpartAvailable, http.MethodGet, http.MethodOptions)
	handle("/mas/upsert_device", "upsert_device", MASUpsertDevice, http.MethodPost, http.MethodOptions)
	handle("/mas/update_device_display_name", "update_device_display_name", MASUpdateDeviceDisplayName, http.MethodPost, http.MethodOptions)
	handle("/mas/delete_device", "delete_device", MASDeleteDevice, http.MethodPost, http.MethodOptions)
	handle("/mas/sync_devices", "sync_devices", MASSyncDevices, http.MethodPost, http.MethodOptions)
	handle("/mas/delete_user", "delete_user", MASDeleteUser, http.MethodPost, http.MethodOptions)
	handle("/mas/reactivate_user", "reactivate_user", MASReactivateUser, http.MethodPost, http.MethodOptions)
	handle("/mas/set_displayname", "set_displayname", MASSetDisplayName, http.MethodPost, http.MethodOptions)
	handle("/mas/unset_displayname", "unset_displayname", MASUnsetDisplayName, http.MethodPost, http.MethodOptions)
	handle("/mas/allow_cross_signing_reset", "allow_cross_signing_reset", MASAllowCrossSigningReset, http.MethodPost, http.MethodOptions)
}

// masHandler is the shared signature of every /_synapse/mas endpoint.
type masHandler func(*http.Request, *config.ClientAPI, userapi.ClientUserAPI) util.JSONResponse

// masRequireLocalpart returns a 400 response when the localpart is empty.
func masRequireLocalpart(localpart string) *util.JSONResponse {
	if localpart == "" {
		res := util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'localpart'"),
		}
		return &res
	}
	return nil
}

// masLookupAccount validates the localpart and verifies that an account
// exists for it, returning a 404/400/500 response when validation fails or
// the account is unknown.
func masLookupAccount(ctx context.Context, userAPI userapi.ClientUserAPI, cfg *config.ClientAPI, localpart string) *util.JSONResponse {
	if err := internal.ValidateUsername(localpart, cfg.Matrix.ServerName); err != nil {
		return internal.UsernameResponse(err)
	}

	var accRes userapi.QueryAccountByLocalpartResponse
	err := userAPI.QueryAccountByLocalpart(ctx, &userapi.QueryAccountByLocalpartRequest{
		Localpart:  localpart,
		ServerName: cfg.Matrix.ServerName,
	}, &accRes)
	switch {
	case errors.Is(err, sql.ErrNoRows), err == nil && accRes.Account == nil:
		res := util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not found"),
		}
		return &res
	case err != nil:
		logrus.WithError(err).Error("MAS admin: failed to query account")
		res := util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
		return &res
	}
	return nil
}

// masQueryUserResponse is the response body for GET /_synapse/mas/query_user.
// Display_name and avatar_url serialize as null when unset, matching Synapse.
type masQueryUserResponse struct {
	UserID        string  `json:"user_id"`
	DisplayName   *string `json:"display_name"`
	AvatarURL     *string `json:"avatar_url"`
	IsSuspended   bool    `json:"is_suspended"`
	IsDeactivated bool    `json:"is_deactivated"`
}

// MASQueryUser returns account and profile information for a localpart.
// GET /_synapse/mas/query_user?localpart=alice.
func MASQueryUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	localpart := req.URL.Query().Get("localpart")
	if errRes := masRequireLocalpart(localpart); errRes != nil {
		return *errRes
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, localpart); errRes != nil {
		return *errRes
	}

	userID := userutil.MakeUserID(localpart, cfg.Matrix.ServerName)

	// The profile only carries cosmetic fields; a missing profile must not
	// hide an existing account.
	var displayName, avatarURL *string
	profile, err := userAPI.QueryProfile(req.Context(), userID)
	if err != nil {
		logrus.WithError(err).WithField("user_id", userID).Warn("MAS admin: failed to query profile")
	} else if profile != nil {
		if profile.DisplayName != "" {
			displayName = &profile.DisplayName
		}
		if profile.AvatarURL != "" {
			avatarURL = &profile.AvatarURL
		}
	}

	// The deactivated flag lives on a separate query; a failure here must not
	// hide the account either.
	deactivated, err := userAPI.IsAccountDeactivated(req.Context(), localpart, cfg.Matrix.ServerName)
	if err != nil {
		logrus.WithError(err).WithField("user_id", userID).Warn("MAS admin: failed to query deactivation state")
		deactivated = false
	}

	// Zendrite has no suspension concept; suspended users are already blocked
	// at token introspection time on the MAS side.
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: masQueryUserResponse{
			UserID:        userID,
			DisplayName:   displayName,
			AvatarURL:     avatarURL,
			IsSuspended:   false,
			IsDeactivated: deactivated,
		},
	}
}

// masProvisionUserRequest is the JSON body for POST /_synapse/mas/provision_user.
type masProvisionUserRequest struct {
	Localpart        string   `json:"localpart"`
	SetDisplayname   *string  `json:"set_displayname,omitempty"`
	UnsetDisplayname bool     `json:"unset_displayname"`
	SetAvatarURL     *string  `json:"set_avatar_url,omitempty"`
	UnsetAvatarURL   bool     `json:"unset_avatar_url"`
	SetEmails        []string `json:"set_emails,omitempty"`
	UnsetEmails      bool     `json:"unset_emails"`
	Locked           *bool    `json:"locked,omitempty"`
}

// MASProvisionUser creates a new account or updates the profile of an
// existing one. It responds 201 when the account was created and 200 when it
// already existed, which is how MAS distinguishes the two outcomes.
// POST /_synapse/mas/provision_user.
func MASProvisionUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masProvisionUserRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}

	if body.SetDisplayname != nil && body.UnsetDisplayname {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Cannot specify both set_displayname and unset_displayname"),
		}
	}
	if body.SetAvatarURL != nil && body.UnsetAvatarURL {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Cannot specify both set_avatar_url and unset_avatar_url"),
		}
	}
	if body.SetEmails != nil && body.UnsetEmails {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Cannot specify both set_emails and unset_emails"),
		}
	}

	// MAS only consults is_localpart_available for brand new registrations,
	// so the namespace rules are enforced here as well for users it already
	// knows about.
	if errRes := validateLocalpartNamespace(cfg, body.Localpart, cfg.Matrix.ServerName); errRes != nil {
		return *errRes
	}
	if errRes := callGuestLocalpartReserved(body.Localpart); errRes != nil {
		return *errRes
	}

	// Emails and the locked flag are owned by MAS under delegated auth:
	// Zendrite's 3PID endpoints are disabled with MSC3861 and token
	// introspection already rejects locked users. Log for observability.
	if body.SetEmails != nil || body.UnsetEmails {
		logrus.WithField("localpart", body.Localpart).Debug("MAS admin: ignoring email change request; MAS owns 3PIDs under delegated auth")
	}
	if body.Locked != nil {
		logrus.WithField("localpart", body.Localpart).Debug("MAS admin: ignoring lock change request; Zendrite has no suspension support")
	}

	// Create or update the account. ConflictUpdate makes this idempotent and
	// race-tolerant: AccountCreated reports which of the two happened.
	var createRes userapi.PerformAccountCreationResponse
	if err := userAPI.PerformAccountCreation(req.Context(), &userapi.PerformAccountCreationRequest{
		AccountType: userapi.AccountTypeUser,
		Localpart:   body.Localpart,
		ServerName:  cfg.Matrix.ServerName,
		OnConflict:  userapi.ConflictUpdate,
	}, &createRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to create/update user")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Apply the profile fields. On creation only the set variants are
	// meaningful; on update the unset variants clear the stored value.
	if body.SetDisplayname != nil {
		if _, _, err := userAPI.SetDisplayName(req.Context(), body.Localpart, cfg.Matrix.ServerName, *body.SetDisplayname); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to set display name")
		}
	} else if body.UnsetDisplayname && !createRes.AccountCreated {
		if _, _, err := userAPI.SetDisplayName(req.Context(), body.Localpart, cfg.Matrix.ServerName, ""); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to unset display name")
		}
	}
	if body.SetAvatarURL != nil {
		if _, _, err := userAPI.SetAvatarURL(req.Context(), body.Localpart, cfg.Matrix.ServerName, *body.SetAvatarURL); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to set avatar URL")
		}
	} else if body.UnsetAvatarURL && !createRes.AccountCreated {
		if _, _, err := userAPI.SetAvatarURL(req.Context(), body.Localpart, cfg.Matrix.ServerName, ""); err != nil {
			logrus.WithError(err).Error("MAS admin: failed to unset avatar URL")
		}
	}

	status := http.StatusOK
	if createRes.AccountCreated {
		status = http.StatusCreated
	}
	return util.JSONResponse{
		Code: status,
		JSON: struct{}{},
	}
}

// MASIsLocalpartAvailable reports whether a localpart can be registered. MAS
// reads only the errcode: any 2xx means available, M_USER_IN_USE /
// M_INVALID_USERNAME / M_EXCLUSIVE mean taken or invalid. The localpart is
// checked exactly as sent, like provision_user validates it, so a name
// reported as available is guaranteed to provision.
// GET /_synapse/mas/is_localpart_available?localpart=alice.
func MASIsLocalpartAvailable(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	localpart := req.URL.Query().Get("localpart")
	if errRes := masRequireLocalpart(localpart); errRes != nil {
		return *errRes
	}

	if errRes := checkLocalpartRegistrable(req.Context(), cfg, userAPI, localpart, cfg.Matrix.ServerName); errRes != nil {
		if errRes.Code == http.StatusInternalServerError {
			logrus.WithField("localpart", localpart).Error("MAS admin: failed to check localpart availability")
		}
		return *errRes
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// masUpsertDeviceRequest is the JSON body for POST /_synapse/mas/upsert_device.
type masUpsertDeviceRequest struct {
	Localpart   string  `json:"localpart"`
	DeviceID    string  `json:"device_id"`
	DisplayName *string `json:"display_name,omitempty"`
}

// masDeviceResponse is the JSON body for device responses carrying the
// device id and optional display name.
type masDeviceResponse struct {
	DeviceID    string  `json:"device_id"`
	DisplayName *string `json:"display_name"`
}

// MASUpsertDevice creates a device (201) or updates the display name of an
// existing one (200).
// POST /_synapse/mas/upsert_device.
func MASUpsertDevice(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masUpsertDeviceRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if body.DeviceID == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'device_id'"),
		}
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}
	userID := userutil.MakeUserID(body.Localpart, cfg.Matrix.ServerName)

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
		if existing.ID == body.DeviceID {
			if body.DisplayName != nil && existing.DisplayName != *body.DisplayName {
				var updateRes userapi.PerformDeviceUpdateResponse
				if err := userAPI.PerformDeviceUpdate(req.Context(), &userapi.PerformDeviceUpdateRequest{
					RequestingUserID: userID,
					DeviceID:         body.DeviceID,
					DisplayName:      body.DisplayName,
				}, &updateRes); err != nil {
					logrus.WithError(err).Error("MAS admin: failed to update device display name")
					return util.JSONResponse{
						Code: http.StatusInternalServerError,
						JSON: spec.InternalServerError{},
					}
				}
			}
			return util.JSONResponse{
				Code: http.StatusOK,
				JSON: masDeviceResponse{DeviceID: body.DeviceID, DisplayName: body.DisplayName},
			}
		}
	}

	if err := masCreateDevice(req.Context(), userAPI, cfg, body.Localpart, body.DeviceID, body.DisplayName); err != nil {
		logrus.WithError(err).WithField("device_id", body.DeviceID).Error("MAS admin: failed to create device")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusCreated,
		JSON: masDeviceResponse{DeviceID: body.DeviceID, DisplayName: body.DisplayName},
	}
}

// masDeviceDisplayNameRequest is the JSON body for
// POST /_synapse/mas/update_device_display_name.
type masDeviceDisplayNameRequest struct {
	Localpart   string `json:"localpart"`
	DeviceID    string `json:"device_id"`
	DisplayName string `json:"display_name"`
}

// MASUpdateDeviceDisplayName updates the display name of a device.
// POST /_synapse/mas/update_device_display_name.
func MASUpdateDeviceDisplayName(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masDeviceDisplayNameRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if body.DeviceID == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'device_id'"),
		}
	}
	if body.DisplayName == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'display_name'"),
		}
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}

	var updateRes userapi.PerformDeviceUpdateResponse
	if err := userAPI.PerformDeviceUpdate(req.Context(), &userapi.PerformDeviceUpdateRequest{
		RequestingUserID: userutil.MakeUserID(body.Localpart, cfg.Matrix.ServerName),
		DeviceID:         body.DeviceID,
		DisplayName:      &body.DisplayName,
	}, &updateRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to update device display name")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !updateRes.DeviceExists {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Device not found"),
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// masDeviceIDRequest is the JSON body for endpoints taking a localpart and a
// single device ID.
type masDeviceIDRequest struct {
	Localpart string `json:"localpart"`
	DeviceID  string `json:"device_id"`
}

// MASDeleteDevice deletes a single device and responds 204. The Go HTTP server never
// writes a body for that status, so the JSON wrapper's payload is dropped
// on the wire.
// POST /_synapse/mas/delete_device.
func MASDeleteDevice(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masDeviceIDRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if body.DeviceID == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'device_id'"),
		}
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}

	var deleteRes userapi.PerformDeviceDeletionResponse
	if err := userAPI.PerformDeviceDeletion(req.Context(), &userapi.PerformDeviceDeletionRequest{
		UserID:    userutil.MakeUserID(body.Localpart, cfg.Matrix.ServerName),
		DeviceIDs: []string{body.DeviceID},
	}, &deleteRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to delete device")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusNoContent,
		JSON: struct{}{},
	}
}

// masSyncDevicesRequest is the JSON body for POST /_synapse/mas/sync_devices.
type masSyncDevicesRequest struct {
	Localpart string   `json:"localpart"`
	Devices   []string `json:"devices"`
}

// MASSyncDevices reconciles the stored device list with the set of device IDs
// MAS reports: devices absent from the set are deleted, missing ones created.
// The dehydrated device (MSC3814) and the fallback OIDC device have no MAS
// session and are never in the target set, so both are excluded from
// deletion.
// POST /_synapse/mas/sync_devices.
func MASSyncDevices(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masSyncDevicesRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if body.Devices == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'devices'"),
		}
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}
	userID := userutil.MakeUserID(body.Localpart, cfg.Matrix.ServerName)

	var queryRes userapi.QueryDevicesResponse
	if err := userAPI.QueryDevices(req.Context(), &userapi.QueryDevicesRequest{UserID: userID}, &queryRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to query devices")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// The dehydrated device is managed through its own MSC3814 API and must
	// survive the reconciliation.
	dehydratedID := ""
	var dehydRes userapi.QueryDehydratedDeviceResponse
	if err := userAPI.QueryDehydratedDevice(req.Context(), &userapi.QueryDehydratedDeviceRequest{UserID: userID}, &dehydRes); err == nil && dehydRes.Found {
		dehydratedID = dehydRes.DeviceID
	}

	target := make(map[string]struct{}, len(body.Devices))
	for _, id := range body.Devices {
		target[id] = struct{}{}
	}
	current := make(map[string]struct{}, len(queryRes.Devices))
	for _, d := range queryRes.Devices {
		current[d.ID] = struct{}{}
	}

	// The fallback device that binds tokens without a device scope has no
	// MAS session either and must survive as well.
	var toDelete []string
	for id := range current {
		if _, ok := target[id]; !ok && id != dehydratedID && id != userapi.MSC3861FallbackDeviceID {
			toDelete = append(toDelete, id)
		}
	}
	var toAdd []string
	for id := range target {
		if _, ok := current[id]; !ok {
			toAdd = append(toAdd, id)
		}
	}

	if len(toDelete) > 0 {
		var deleteRes userapi.PerformDeviceDeletionResponse
		if err := userAPI.PerformDeviceDeletion(req.Context(), &userapi.PerformDeviceDeletionRequest{
			UserID:    userID,
			DeviceIDs: toDelete,
		}, &deleteRes); err != nil {
			logrus.WithError(err).WithField("user_id", userID).Error("MAS admin: failed to delete devices during sync")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	for _, id := range toAdd {
		if err := masCreateDevice(req.Context(), userAPI, cfg, body.Localpart, id, nil); err != nil {
			logrus.WithError(err).WithField("device_id", id).Error("MAS admin: failed to create device during sync")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// masDeleteUserRequest is the JSON body for POST /_synapse/mas/delete_user.
type masDeleteUserRequest struct {
	Localpart string `json:"localpart"`
	Erase     bool   `json:"erase"`
}

// MASDeleteUser deactivates a user account. With erase, the user's events
// are served redacted to anyone who was not in the room when they were sent,
// and the user's account data and 3PIDs are deleted (MSC4025).
// POST /_synapse/mas/delete_user.
func MASDeleteUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masDeleteUserRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}

	var deactivateRes userapi.PerformAccountDeactivationResponse
	if err := userAPI.PerformAccountDeactivation(req.Context(), &userapi.PerformAccountDeactivationRequest{
		Localpart:  body.Localpart,
		ServerName: cfg.Matrix.ServerName,
		Erase:      body.Erase,
	}, &deactivateRes); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to deactivate user")
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

// masLocalpartOnlyRequest is the JSON body for endpoints taking only a localpart.
type masLocalpartOnlyRequest struct {
	Localpart string `json:"localpart"`
}

// MASReactivateUser re-enables login for a deactivated account.
// POST /_synapse/mas/reactivate_user.
func MASReactivateUser(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masLocalpartOnlyRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}

	if err := internal.ValidateUsername(body.Localpart, cfg.Matrix.ServerName); err != nil {
		return *internal.UsernameResponse(err)
	}

	// The update itself reports a missing account, so no lookup is needed.
	var reactivateRes userapi.PerformAccountReactivationResponse
	err := userAPI.PerformAccountReactivation(req.Context(), &userapi.PerformAccountReactivationRequest{
		Localpart:  body.Localpart,
		ServerName: cfg.Matrix.ServerName,
	}, &reactivateRes)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("User not found"),
		}
	case err != nil:
		logrus.WithError(err).Error("MAS admin: failed to reactivate user")
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

// masSetDisplayNameRequest is the JSON body for POST /_synapse/mas/set_displayname.
type masSetDisplayNameRequest struct {
	Localpart   string `json:"localpart"`
	Displayname string `json:"displayname"`
}

// MASSetDisplayName sets the display name of a user.
// POST /_synapse/mas/set_displayname.
func MASSetDisplayName(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masSetDisplayNameRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if body.Displayname == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Missing 'displayname'"),
		}
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}

	if _, _, err := userAPI.SetDisplayName(req.Context(), body.Localpart, cfg.Matrix.ServerName, body.Displayname); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to set display name")
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

// MASUnsetDisplayName clears the display name of a user.
// POST /_synapse/mas/unset_displayname.
func MASUnsetDisplayName(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI) util.JSONResponse {
	var body masLocalpartOnlyRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}

	if errRes := masLookupAccount(req.Context(), userAPI, cfg, body.Localpart); errRes != nil {
		return *errRes
	}

	if _, _, err := userAPI.SetDisplayName(req.Context(), body.Localpart, cfg.Matrix.ServerName, ""); err != nil {
		logrus.WithError(err).Error("MAS admin: failed to unset display name")
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

// masCrossSigningConfirmationTTL is how long a cross-signing replacement
// confirmation recorded via the MAS provisioning API remains valid.
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

// MASAllowCrossSigningReset records that the user confirmed a cross-signing
// key replacement in the OIDC provider's account management UI. The next
// m.oauth UIA resubmission for this user consumes the confirmation.
// Unlike the other endpoints it does not verify the account exists: the
// confirmation is only ever consumed when the named user uploads
// cross-signing keys with an authenticated device, and the endpoint is
// already restricted to the MAS admin token.
// POST /_synapse/mas/allow_cross_signing_reset.
func MASAllowCrossSigningReset(req *http.Request, cfg *config.ClientAPI, _ userapi.ClientUserAPI) util.JSONResponse {
	var body masLocalpartOnlyRequest
	if errRes := clienthttputil.UnmarshalJSONRequest(req, &body); errRes != nil {
		return *errRes
	}
	if errRes := masRequireLocalpart(body.Localpart); errRes != nil {
		return *errRes
	}
	if err := internal.ValidateUsername(body.Localpart, cfg.Matrix.ServerName); err != nil {
		return *internal.UsernameResponse(err)
	}

	recordMASCrossSigningConfirmation(userutil.MakeUserID(body.Localpart, cfg.Matrix.ServerName))

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// masCreateDevice creates a device row for a MAS-provisioned session. The
// row carries a placeholder access token because the database requires one;
// the introspection path swaps in the real token on the client's first
// request.
func masCreateDevice(ctx context.Context, userAPI userapi.ClientUserAPI, cfg *config.ClientAPI, localpart, deviceID string, displayName *string) error {
	placeholderToken, err := generatePlaceholderToken()
	if err != nil {
		return fmt.Errorf("failed to generate placeholder access token: %w", err)
	}
	var deviceRes userapi.PerformDeviceCreationResponse
	return userAPI.PerformDeviceCreation(ctx, &userapi.PerformDeviceCreationRequest{
		Localpart:          localpart,
		ServerName:         cfg.Matrix.ServerName,
		AccessToken:        placeholderToken,
		DeviceID:           &deviceID,
		DeviceDisplayName:  displayName,
		NoDeviceListUpdate: true,
	}, &deviceRes)
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
