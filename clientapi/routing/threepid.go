// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/clientapi/threepid"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
	userdb "codefloe.com/pat-s/zendrite/userapi/storage"
)

type reqTokenResponse struct {
	SID string `json:"sid"`
}

type ThreePIDsResponse struct {
	ThreePIDs []authtypes.ThreePID `json:"threepids"`
}

// RequestEmailToken implements:
//
//	POST /account/3pid/email/requestToken
//	POST /register/email/requestToken
func RequestEmailToken(req *http.Request, threePIDAPI api.ClientUserAPI, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	var body threepid.EmailAssociationRequest
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}
	body.Email = authtypes.CanonicalThreePIDAddress("email", body.Email)
	if body.IDServer == "" {
		// The homeserver authenticates with its own token to the server it
		// is configured with.
		body.IDServer = cfg.EmailValidationServer
		body.IDAccessToken = ""
		if body.IDServer == "" {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MatrixError{
					ErrCode: "M_THREEPID_MEDIUM_NOT_SUPPORTED",
					Err:     "Email verification is not configured on this homeserver",
				},
			}
		}
	}

	var resp reqTokenResponse
	var err error

	// Check if the 3PID is already in use locally
	res := &api.QueryLocalpartForThreePIDResponse{}
	err = threePIDAPI.QueryLocalpartForThreePID(req.Context(), &api.QueryLocalpartForThreePIDRequest{
		ThreePID: body.Email,
		Medium:   "email",
	}, res)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("threePIDAPI.QueryLocalpartForThreePID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Internal server error"),
		}
	}

	if len(res.Localpart) > 0 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{
				ErrCode: spec.ErrorThreePIDInUse,
				Err:     userdb.Err3PIDInUse.Error(),
			},
		}
	}

	auth := threepid.IdentityAuth{Token: body.IDAccessToken, UserAPI: threePIDAPI, Config: cfg, Client: client}
	resp.SID, err = threepid.CreateSession(req.Context(), body, auth)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("threepid.CreateSession failed")
		return threePIDError(err, body.IDServer)
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}

// CheckAndSave3PIDAssociation implements POST /account/3pid.
func CheckAndSave3PIDAssociation(
	req *http.Request, threePIDAPI api.ClientUserAPI, device *api.Device,
	cfg *config.ClientAPI, client *fclient.Client,
) util.JSONResponse {
	var body threepid.EmailAssociationCheckRequest
	if reqErr := httputil.UnmarshalJSONRequest(req, &body); reqErr != nil {
		return *reqErr
	}

	return save3PIDAssociation(req, threePIDAPI, device, cfg, client, body)
}

func save3PIDAssociation(req *http.Request, threePIDAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client, body threepid.EmailAssociationCheckRequest) util.JSONResponse {
	if body.Creds.SID == "" || body.Creds.Secret == "" || body.Creds.IDServer == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("sid, client_secret and id_server are required")}
	}

	// Check if the association has been validated
	auth := threepid.IdentityAuth{Token: body.Creds.IDAccessToken, UserAPI: threePIDAPI, Config: cfg, Client: client}
	verified, address, medium, err := threepid.CheckAssociation(req.Context(), body.Creds, auth)
	{
		var errCase0 threepid.ErrNotTrusted
		switch {
		case err == nil:
		case errors.As(err, &errCase0):
			util.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAssociation failed")
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.NotTrusted(body.Creds.IDServer)}
		default:
			util.GetLogger(req.Context()).WithError(err).Error("threepid.CheckAssociation failed")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.Unknown("Internal server error")}
		}
	}

	if !verified {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{
				ErrCode: spec.ErrorThreePIDAuthFailed,
				Err:     "Failed to auth 3pid",
			},
		}
	}

	// Save the association in the database
	localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Internal server error"),
		}
	}

	if err = threePIDAPI.PerformSaveThreePIDAssociation(req.Context(), &api.PerformSaveThreePIDAssociationRequest{
		ThreePID:   address,
		Localpart:  localpart,
		ServerName: domain,
		Medium:     medium,
	}, &struct{}{}); err != nil {
		if errors.Is(err, userdb.Err3PIDInUse) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MatrixError{ErrCode: spec.ErrorThreePIDInUse, Err: "3PID is already in use"}}
		}
		util.GetLogger(req.Context()).WithError(err).Error("threePIDAPI.PerformSaveThreePIDAssociation failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Internal server error"),
		}
	}

	if body.Bind {
		// Publish the association on the identity server if requested
		if _, err = threepid.Bind(req.Context(), body.Creds, device.UserID, auth); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("threepid.Bind failed")
			return threePIDError(err, body.Creds.IDServer)
		}
		if err = saveBinding(req, threePIDAPI, device.UserID, medium, address, body.Creds.IDServer, true); err != nil {
			return util.ErrorResponse(err)
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// GetAssociated3PIDs implements GET /account/3pid.
func GetAssociated3PIDs(
	req *http.Request, threepidAPI api.ClientUserAPI, device *api.Device,
) util.JSONResponse {
	localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Internal server error"),
		}
	}

	res := &api.QueryThreePIDsForLocalpartResponse{}
	err = threepidAPI.QueryThreePIDsForLocalpart(req.Context(), &api.QueryThreePIDsForLocalpartRequest{
		Localpart:  localpart,
		ServerName: domain,
	}, res)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("threepidAPI.QueryThreePIDsForLocalpart failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Internal server error"),
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: ThreePIDsResponse{res.ThreePIDs},
	}
}

// Add3PID implements POST /account/3pid/add with interactive authentication.
func Add3PID(req *http.Request, userAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client, uia *auth.UserInteractive) util.JSONResponse {
	var body threepid.Credentials
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("Cannot read request")}
	}
	if res := httputil.UnmarshalJSON(bodyBytes, &body); res != nil {
		return *res
	}
	if body.SID == "" || body.Secret == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("sid and client_secret are required")}
	}
	login, res := uia.Verify(req.Context(), bodyBytes, device)
	if res != nil {
		return *res
	}
	if login.Username() != device.UserID {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("Authentication does not match the current user")}
	}
	// Only accept sessions issued through our own requestToken endpoint.
	body.IDServer = threepid.SessionServer(body.SID, body.Secret)
	if body.IDServer == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MatrixError{ErrCode: spec.ErrorThreePIDAuthFailed, Err: "Unknown or expired validation session"}}
	}
	return save3PIDAssociation(req, userAPI, device, cfg, client, threepid.EmailAssociationCheckRequest{Creds: body})
}

const threePIDBindingPrefix = "org.zendrite.threepid_binding."

func bindingDataType(medium, address string) string {
	address = authtypes.CanonicalThreePIDAddress(medium, address)
	return fmt.Sprintf("%s%x", threePIDBindingPrefix, sha256.Sum256([]byte(medium+"\x00"+address)))
}

func threePIDError(err error, server string) util.JSONResponse {
	var untrusted threepid.ErrNotTrusted
	var remote *threepid.IdentityServerError
	switch {
	case errors.As(err, &untrusted):
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.NotTrusted(server)}
	case errors.As(err, &remote):
		return util.JSONResponse{Code: remote.Code, JSON: remote.Body}
	default:
		return util.JSONResponse{Code: http.StatusBadGateway, JSON: spec.Unknown("Identity server request failed")}
	}
}

// Bind3PID publishes a binding without modifying the local account association.
func Bind3PID(req *http.Request, userAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	var body threepid.Credentials
	if res := httputil.UnmarshalJSONRequest(req, &body); res != nil {
		return *res
	}
	if body.SID == "" || body.Secret == "" || body.IDServer == "" || body.IDAccessToken == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("sid, client_secret, id_server and id_access_token are required")}
	}
	auth := threepid.IdentityAuth{Token: body.IDAccessToken, UserAPI: userAPI, Config: cfg, Client: client}
	result, err := threepid.Bind(req.Context(), body, device.UserID, auth)
	if err != nil {
		return threePIDError(err, body.IDServer)
	}
	if err = saveBinding(req, userAPI, device.UserID, result.Medium, result.Address, body.IDServer, true); err != nil {
		return util.ErrorResponse(err)
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}

// Serialize updates so simultaneous binds to different identity servers are retained.
var threePIDBindingMutex sync.Mutex

func bindingServers(req *http.Request, userAPI api.ClientUserAPI, userID, medium, address string) (map[string]bool, error) {
	var data api.QueryAccountDataResponse
	key := bindingDataType(medium, address)
	if err := userAPI.QueryAccountData(req.Context(), &api.QueryAccountDataRequest{UserID: userID, DataType: key}, &data); err != nil {
		return nil, err
	}
	servers := make(map[string]bool)
	if raw := data.GlobalAccountData[key]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &servers); err != nil {
			return nil, err
		}
	}
	return servers, nil
}

func saveBinding(req *http.Request, userAPI api.ClientUserAPI, userID, medium, address, server string, bound bool) error {
	threePIDBindingMutex.Lock()
	defer threePIDBindingMutex.Unlock()
	servers, err := bindingServers(req, userAPI, userID, medium, address)
	if err != nil {
		return err
	}
	if bound {
		servers[server] = true
	} else {
		delete(servers, server)
	}
	data, err := json.Marshal(servers)
	if err != nil {
		return err
	}
	return userAPI.InputAccountData(req.Context(), &api.InputAccountDataRequest{UserID: userID, DataType: bindingDataType(medium, address), AccountData: data}, &api.InputAccountDataResponse{})
}

// Unbind3PID removes the identity-server binding while retaining the local 3PID.
func Unbind3PID(req *http.Request, userAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	return remove3PID(req, userAPI, device, cfg, client, false)
}

// Forget3PID removes only the authenticated user's local association and attempts an unbind.
func Forget3PID(req *http.Request, userAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	return remove3PID(req, userAPI, device, cfg, client, true)
}

func remove3PID(req *http.Request, userAPI api.ClientUserAPI, device *api.Device, cfg *config.ClientAPI, client *fclient.Client, forget bool) util.JSONResponse {
	var body struct {
		Address  string `json:"address"`
		Medium   string `json:"medium"`
		IDServer string `json:"id_server"`
	}
	if res := httputil.UnmarshalJSONRequest(req, &body); res != nil {
		return *res
	}
	if body.Address == "" || body.Medium == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("address and medium are required")}
	}
	if body.Medium != "email" && body.Medium != "msisdn" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Unsupported medium")}
	}
	body.Address = authtypes.CanonicalThreePIDAddress(body.Medium, body.Address)
	servers := map[string]bool{body.IDServer: true}
	if body.IDServer == "" {
		var err error
		servers, err = bindingServers(req, userAPI, device.UserID, body.Medium, body.Address)
		if err != nil {
			return util.ErrorResponse(err)
		}
	}
	result := "success"
	if len(servers) == 0 {
		result = "no-support"
	}
	for server := range servers {
		unbound, err := threepid.Unbind(req.Context(), server, device.UserID, body.Medium, body.Address, cfg, client)
		if err != nil {
			return threePIDError(err, server)
		}
		if unbound == "success" {
			if err = saveBinding(req, userAPI, device.UserID, body.Medium, body.Address, server, false); err != nil {
				return util.ErrorResponse(err)
			}
		} else {
			result = "no-support"
		}
	}

	if forget {
		localpart, domain, err := gomatrixserverlib.SplitID('@', device.UserID)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if err = userAPI.PerformForgetThreePID(req.Context(), &api.PerformForgetThreePIDRequest{ThreePID: body.Address, Medium: body.Medium, Localpart: localpart, ServerName: domain}, &struct{}{}); err != nil {
			return util.ErrorResponse(err)
		}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: map[string]string{"id_server_unbind_result": result}}
}
