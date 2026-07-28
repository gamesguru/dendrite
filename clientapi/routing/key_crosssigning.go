// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

type crossSigningRequest struct {
	api.PerformUploadDeviceKeysRequest
	Auth newPasswordAuth `json:"auth"`
}

type UploadKeysAPI interface {
	QueryKeys(ctx context.Context, req *api.QueryKeysRequest, res *api.QueryKeysResponse)
	api.UploadDeviceKeysAPI
}

func UploadCrossSigningDeviceKeys(
	req *http.Request,
	keyserverAPI UploadKeysAPI, device *api.Device,
	accountAPI auth.GetAccountByPassword, cfg *config.ClientAPI,
	mscCfg *config.MSCs,
) util.JSONResponse {
	uploadReq := &crossSigningRequest{}
	uploadRes := &api.PerformUploadDeviceKeysResponse{}

	resErr := httputil.UnmarshalJSONRequest(req, &uploadReq)
	if resErr != nil {
		return *resErr
	}

	// Query existing keys to determine if UIA is required
	keyResp := api.QueryKeysResponse{}
	keyserverAPI.QueryKeys(req.Context(), &api.QueryKeysRequest{
		UserID:        device.UserID,
		UserToDevices: map[string][]string{device.UserID: {device.ID}},
		Timeout:       time.Second * 10, //nolint:mnd
	}, &keyResp)

	if keyResp.Error != nil {
		logrus.WithError(keyResp.Error).Error("Failed to query keys")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(keyResp.Error.Error()),
		}
	}

	existingMasterKey, hasMasterKey := keyResp.MasterKeys[device.UserID]
	requireUIA := false
	if hasMasterKey {
		// If we have a master key, check if any of the existing keys differ. If they do,
		// we need to re-authenticate the user.
		requireUIA = keysDiffer(existingMasterKey, keyResp, uploadReq, device.UserID)
	}

	if requireUIA {
		sessionID := uploadReq.Auth.Session
		if sessionID == "" {
			sessionID = util.RandomString(sessionIDLength)
		}
		if mscCfg.Enabled("msc3861") {
			// With delegated authentication there is no password to re-check.
			// The OAuth 2.0 API uses the m.oauth UIA type instead: the client
			// opens the params URL so the user can confirm the action in the
			// account management web UI, and the OIDC provider reports that
			// confirmation back through the MAS admin API. That callback is the
			// only evidence that a human approved the replacement -- a
			// resubmitted session proves nothing beyond possession of the
			// access token, which is exactly what UIA on this endpoint has to
			// guard against. The stage is therefore completed by the provider
			// confirmation alone whenever the provider is able to send one, and
			// only falls back to the weaker resubmission check when no admin
			// token is configured and that callback cannot happen.
			completed := slices.Contains(sessions.getCompletedStages(sessionID), authtypes.LoginTypeOAuth)
			if !completed && completeOAuthCrossSigningStage(sessionID, device.UserID, mscCfg.MSC3861.AdminToken != "") {
				sessions.addCompletedSessionStage(sessionID, authtypes.LoginTypeOAuth)
				completed = true
			}
			if !completed {
				// Always generate the session ID server-side. A client-supplied
				// session that we never issued (or that was issued for another
				// user) gets a fresh challenge with a new session ID rather
				// than being honored. The stage is only recorded as completed
				// once the checks above pass.
				sessionID = util.RandomString(sessionIDLength)
				sessions.addOAuthSession(sessionID, device.UserID)
				return util.JSONResponse{
					Code: http.StatusUnauthorized,
					JSON: newUserInteractiveResponse(
						sessionID,
						[]authtypes.Flow{
							{
								Stages: []authtypes.LoginType{authtypes.LoginTypeOAuth},
							},
						},
						map[string]any{
							"m.oauth": map[string]any{
								"url": msc3861OAuthUIAURL(mscCfg.MSC3861.AccountManagementURL, device.ID),
							},
						},
					),
				}
			}
		} else {
			if uploadReq.Auth.Type != authtypes.LoginTypePassword {
				return util.JSONResponse{
					Code: http.StatusUnauthorized,
					JSON: newUserInteractiveResponse(
						sessionID,
						[]authtypes.Flow{
							{
								Stages: []authtypes.LoginType{authtypes.LoginTypePassword},
							},
						},
						nil,
					),
				}
			}
			typePassword := auth.LoginTypePassword{
				GetAccountByPassword: accountAPI,
				Config:               cfg,
			}
			if _, authErr := typePassword.Login(req.Context(), &uploadReq.Auth.PasswordRequest); authErr != nil {
				return *authErr
			}
			sessions.addCompletedSessionStage(sessionID, authtypes.LoginTypePassword)
		}
	}

	uploadReq.UserID = device.UserID
	keyserverAPI.PerformUploadDeviceKeys(req.Context(), &uploadReq.PerformUploadDeviceKeysRequest, uploadRes)

	if err := uploadRes.Error; err != nil {
		switch {
		case err.IsInvalidSignature:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidSignature(err.Error()),
			}
		case err.IsMissingParam:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MissingParam(err.Error()),
			}
		case err.IsInvalidParam:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		default:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown(err.Error()),
			}
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// unconfirmedCrossSigningWarning makes sure the fallback below is reported once
// per process rather than on every cross-signing key replacement.
var unconfirmedCrossSigningWarning sync.Once

// completeOAuthCrossSigningStage reports whether the m.oauth UIA stage for a
// cross-signing key replacement may be completed, consuming the signal that
// completes it.
//
// When an admin token is configured the MAS admin routes are registered, so the
// OIDC provider is able to call
// POST /_synapse/admin/v1/users/{userId}/_allow_cross_signing_replacement_without_uia
// once the user confirmed the reset in the account management web UI. In that
// configuration the provider confirmation is the only thing that completes the
// stage: resubmitting a session this server issued is not sufficient, since any
// holder of the access token can do that.
//
// Without an admin token there is no callback channel, so the stage falls back
// to a resubmission of a session that this server issued for this user. That is
// a weaker check -- the access token is then the only guard -- so it is logged.
func completeOAuthCrossSigningStage(sessionID, userID string, providerCanConfirm bool) bool {
	if providerCanConfirm {
		return consumeMASCrossSigningConfirmation(userID)
	}
	if !sessions.completeOAuthSession(sessionID, userID) {
		return false
	}
	unconfirmedCrossSigningWarning.Do(func() {
		logrus.Warn("MSC3861: completing cross-signing key replacements without provider confirmation, " +
			"set msc3861.admin_token so the OIDC provider can confirm the reset via the MAS admin API")
	})
	return true
}

// msc3861OAuthUIAURL builds the URL for the m.oauth UIA params, pointing at the
// account management web UI with the cross-signing reset action, as described
// in the OAuth 2.0 API account management section.
func msc3861OAuthUIAURL(accountManagementURL, deviceID string) string {
	u, err := url.Parse(accountManagementURL)
	if err != nil {
		return accountManagementURL
	}
	q := u.Query()
	q.Set("action", "org.matrix.cross_signing_reset")
	if deviceID != "" {
		q.Set("device_id", deviceID)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func keysDiffer(existingMasterKey fclient.CrossSigningKey, keyResp api.QueryKeysResponse, uploadReq *crossSigningRequest, userID string) bool {
	masterKeyEqual := existingMasterKey.Equal(&uploadReq.MasterKey)
	if !masterKeyEqual {
		return true
	}
	existingSelfSigningKey := keyResp.SelfSigningKeys[userID]
	selfSigningEqual := existingSelfSigningKey.Equal(&uploadReq.SelfSigningKey)
	if !selfSigningEqual {
		return true
	}
	existingUserSigningKey := keyResp.UserSigningKeys[userID]
	userSigningEqual := existingUserSigningKey.Equal(&uploadReq.UserSigningKey)
	return !userSigningEqual
}

func UploadCrossSigningDeviceSignatures(req *http.Request, keyserverAPI api.ClientKeyAPI, device *api.Device) util.JSONResponse {
	uploadReq := &api.PerformUploadDeviceSignaturesRequest{}
	uploadRes := &api.PerformUploadDeviceSignaturesResponse{}

	if err := httputil.UnmarshalJSONRequest(req, &uploadReq.Signatures); err != nil {
		return *err
	}

	uploadReq.UserID = device.UserID
	keyserverAPI.PerformUploadDeviceSignatures(req.Context(), uploadReq, uploadRes)

	if err := uploadRes.Error; err != nil {
		switch {
		case err.IsInvalidSignature:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidSignature(err.Error()),
			}
		case err.IsMissingParam:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MissingParam(err.Error()),
			}
		case err.IsInvalidParam:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		default:
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown(err.Error()),
			}
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
