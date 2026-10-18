package routing

import (
	"encoding/json"
	"io"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/threepid"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// deactivateRequest holds the parameters of POST /account/deactivate besides
// the interactive auth data.
type deactivateRequest struct {
	IDServer string `json:"id_server"`
	// Erase asks for the user's content to be erased (MSC4025).
	Erase bool `json:"erase"`
}

// Deactivate handles POST requests to /account/deactivate.
func Deactivate(
	req *http.Request,
	userInteractiveAuth *auth.UserInteractive,
	accountAPI api.ClientUserAPI,
	deviceAPI *api.Device,
	cfg *config.ClientAPI,
	client *fclient.Client,
) util.JSONResponse {
	ctx := req.Context()
	defer req.Body.Close()
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be read: " + err.Error()),
		}
	}

	login, errRes := userInteractiveAuth.Verify(ctx, bodyBytes, deviceAPI)
	if errRes != nil {
		return *errRes
	}

	var body deactivateRequest
	if err = json.Unmarshal(bodyBytes, &body); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON: " + err.Error()),
		}
	}

	localpart, serverName, err := gomatrixserverlib.SplitID('@', login.Username())
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Unbind before deactivating: erasure deletes the account data that
	// records which identity servers hold a binding.
	unbindResult := unbindAll3PIDs(req, accountAPI, login.Username(), localpart, serverName, body.IDServer, cfg, client)

	var res api.PerformAccountDeactivationResponse
	err = accountAPI.PerformAccountDeactivation(ctx, &api.PerformAccountDeactivationRequest{
		Localpart:  localpart,
		ServerName: serverName,
		Erase:      body.Erase,
	}, &res)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("userAPI.PerformAccountDeactivation failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]string{"id_server_unbind_result": unbindResult},
	}
}

// unbindAll3PIDs removes the identity server bindings of the user's 3PIDs,
// from idServer if given, or else from every server recorded as holding a
// binding. It returns "success" when every binding is gone, including when
// there are none, and "no-support" when one could not be removed. Failures
// do not stop the deactivation.
func unbindAll3PIDs(
	req *http.Request, userAPI api.ClientUserAPI,
	userID, localpart string, serverName spec.ServerName, idServer string,
	cfg *config.ClientAPI, client *fclient.Client,
) string {
	logger := util.GetLogger(req.Context())
	var threepids api.QueryThreePIDsForLocalpartResponse
	if err := userAPI.QueryThreePIDsForLocalpart(req.Context(), &api.QueryThreePIDsForLocalpartRequest{
		Localpart: localpart, ServerName: serverName,
	}, &threepids); err != nil {
		logger.WithError(err).Error("deactivate: failed to list 3PIDs")
		return "no-support"
	}
	result := "success"
	for _, tp := range threepids.ThreePIDs {
		servers := map[string]bool{idServer: true}
		if idServer == "" {
			var err error
			if servers, err = bindingServers(req, userAPI, userID, tp.Medium, tp.Address); err != nil {
				logger.WithError(err).Error("deactivate: failed to read 3PID bindings")
				result = "no-support"
				continue
			}
		}
		for server := range servers {
			unbound, err := threepid.Unbind(req.Context(), server, userID, tp.Medium, tp.Address, cfg, client)
			if err != nil || unbound != "success" {
				logger.WithError(err).WithField("id_server", server).Warn("deactivate: failed to unbind 3PID")
				result = "no-support"
				continue
			}
			if err = saveBinding(req, userAPI, userID, tp.Medium, tp.Address, server, false); err != nil {
				logger.WithError(err).Error("deactivate: failed to record 3PID unbinding")
			}
		}
	}
	return result
}
