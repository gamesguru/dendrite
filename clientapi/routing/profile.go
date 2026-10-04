// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	appserviceAPI "codefloe.com/pat-s/zendrite/appservice/api"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// maxProfileKeyLength is the longest profile field name, in bytes (MSC4133).
const maxProfileKeyLength = 255

// profileFieldTimeZone is the profile field holding the user's IANA time zone.
const profileFieldTimeZone = "m.tz"

// customProfileKey is the grammar of custom profile field names in the
// specification: a namespaced identifier such as "m.tz" or
// "org.example.job_title".
var customProfileKey = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

// GetProfile implements GET /profile/{userID}.
func GetProfile(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) util.JSONResponse {
	profile, errRes := lookupProfile(req, profileAPI, cfg, userID, asAPI, federation)
	if errRes != nil {
		return *errRes
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: profile.ClientFields(),
	}
}

// GetProfileField implements GET /profile/{userID}/{keyName}.
func GetProfileField(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID, key string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) util.JSONResponse {
	if len(key) > maxProfileKeyLength {
		return keyTooLarge()
	}
	profile, errRes := lookupProfile(req, profileAPI, cfg, userID, asAPI, federation)
	if errRes != nil {
		return *errRes
	}

	switch key {
	case authtypes.ProfileFieldDisplayName:
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: eventutil.UserProfile{DisplayName: profile.DisplayName},
		}
	case authtypes.ProfileFieldAvatarURL:
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: eventutil.UserProfile{AvatarURL: profile.AvatarURL},
		}
	}
	value, ok := profile.Fields[key]
	if !ok {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The requested profile key does not exist."),
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]json.RawMessage{key: value},
	}
}

// lookupProfile wraps getProfile, turning its errors into responses.
func lookupProfile(
	req *http.Request, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) (*authtypes.Profile, *util.JSONResponse) {
	profile, err := getProfile(req.Context(), profileAPI, cfg, userID, asAPI, federation)
	if err != nil {
		if errors.Is(err, appserviceAPI.ErrProfileNotExists) {
			return nil, &util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("The user does not exist or does not have a profile"),
			}
		}

		util.GetLogger(req.Context()).WithError(err).Error("getProfile failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return profile, nil
}

// SetProfileField implements PUT /profile/{userID}/{keyName}.
func SetProfileField(
	req *http.Request, profileAPI userapi.ProfileAPI,
	device *userapi.Device, userID, key string, cfg *config.ClientAPI, rsAPI api.ClientRoomserverAPI,
) util.JSONResponse {
	localpart, domain, errRes := authorizeProfileChange(req, device, userID, key, cfg)
	if errRes != nil {
		return *errRes
	}
	if !isValidProfileKey(key) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Invalid profile key."),
		}
	}

	var body map[string]json.RawMessage
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	value, ok := body[key]
	if !ok {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam(fmt.Sprintf("A required parameter is missing: %s.", key)),
		}
	}

	switch key {
	case authtypes.ProfileFieldDisplayName, authtypes.ProfileFieldAvatarURL:
		// A null value clears the field, as it always has.
		var name *string
		if err := json.Unmarshal(value, &name); err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON(fmt.Sprintf("%s must be a string.", key)),
			}
		}
		var newValue string
		if name != nil {
			newValue = *name
		}
		return setProfileName(req, profileAPI, device, userID, localpart, domain, key, newValue, rsAPI)
	case profileFieldTimeZone:
		var tz string
		if err := json.Unmarshal(value, &tz); err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON(fmt.Sprintf("%s must be a string.", key)),
			}
		}
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(err.Error()),
		}
	}
	err := profileAPI.SetProfileField(req.Context(), localpart, domain, key, compact.Bytes())
	switch {
	case errors.Is(err, userapi.ErrProfileTooLarge):
		return profileTooLarge()
	case err != nil:
		util.GetLogger(req.Context()).WithError(err).Error("profileAPI.SetProfileField failed")
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

// DeleteProfileField implements DELETE /profile/{userID}/{keyName}.
// Deleting the display name or avatar URL clears it.
func DeleteProfileField(
	req *http.Request, profileAPI userapi.ProfileAPI,
	device *userapi.Device, userID, key string, cfg *config.ClientAPI, rsAPI api.ClientRoomserverAPI,
) util.JSONResponse {
	localpart, domain, errRes := authorizeProfileChange(req, device, userID, key, cfg)
	if errRes != nil {
		return *errRes
	}

	switch key {
	case authtypes.ProfileFieldDisplayName, authtypes.ProfileFieldAvatarURL:
		return setProfileName(req, profileAPI, device, userID, localpart, domain, key, "", rsAPI)
	}

	if err := profileAPI.DeleteProfileField(req.Context(), localpart, domain, key); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("profileAPI.DeleteProfileField failed")
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

// authorizeProfileChange checks that the device may change the given field of
// the user's profile, and returns the user's localpart and domain.
func authorizeProfileChange(
	req *http.Request, device *userapi.Device, userID, key string, cfg *config.ClientAPI,
) (string, spec.ServerName, *util.JSONResponse) {
	if userID != device.UserID {
		return "", "", &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}
	// Guests may only change their display name.
	if device.AccountType == userapi.AccountTypeGuest && key != authtypes.ProfileFieldDisplayName {
		return "", "", &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.GuestAccessForbidden("Guest access not allowed"),
		}
	}
	if len(key) > maxProfileKeyLength {
		res := keyTooLarge()
		return "", "", &res
	}

	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return "", "", &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !cfg.Matrix.IsLocalServerName(domain) {
		return "", "", &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not belong to a locally configured domain"),
		}
	}
	return localpart, domain, nil
}

// isValidProfileKey reports whether a profile field may be set under key.
func isValidProfileKey(key string) bool {
	switch key {
	case authtypes.ProfileFieldDisplayName, authtypes.ProfileFieldAvatarURL:
		return true
	}
	return customProfileKey.MatchString(key)
}

func keyTooLarge() util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusBadRequest,
		JSON: spec.MatrixError{
			ErrCode: "M_KEY_TOO_LARGE",
			Err:     fmt.Sprintf("The key name exceeds the maximum allowed length of %d bytes.", maxProfileKeyLength),
		},
	}
}

func profileTooLarge() util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusBadRequest,
		JSON: spec.MatrixError{
			ErrCode: "M_PROFILE_TOO_LARGE",
			Err:     fmt.Sprintf("The profile data exceeds the maximum allowed size of %d bytes.", authtypes.MaxProfileSize),
		},
	}
}

// setProfileName sets the display name or avatar URL, which unlike other
// profile fields are copied into the user's membership events.
func setProfileName(
	req *http.Request, profileAPI userapi.ProfileAPI, device *userapi.Device,
	userID, localpart string, domain spec.ServerName, key, value string,
	rsAPI api.ClientRoomserverAPI,
) util.JSONResponse {
	evTime, err := httputil.ParseTSParam(req)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	setName := profileAPI.SetDisplayName
	if key == authtypes.ProfileFieldAvatarURL {
		setName = profileAPI.SetAvatarURL
	}
	profile, changed, err := setName(req.Context(), localpart, domain, value)
	switch {
	case errors.Is(err, userapi.ErrProfileTooLarge):
		return profileTooLarge()
	case err != nil:
		util.GetLogger(req.Context()).WithError(err).WithField("field", key).Error("failed to set profile field")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	// No need to build new membership events, since nothing changed
	if !changed {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}

	response, err := updateProfile(req.Context(), rsAPI, device, profile, userID, evTime)
	if err != nil {
		return response
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

func updateProfile(
	ctx context.Context, rsAPI api.ClientRoomserverAPI, device *userapi.Device,
	profile *authtypes.Profile,
	userID string, evTime time.Time,
) (util.JSONResponse, error) {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	rooms, err := rsAPI.QueryRoomsForUser(ctx, *deviceUserID, "join")
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryRoomsForUser failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	roomIDStrs := make([]string, len(rooms))
	for i, room := range rooms {
		roomIDStrs[i] = room.String()
	}

	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}

	events, err := buildMembershipEvents(
		ctx, roomIDStrs, *profile, userID, evTime, rsAPI,
	)
	{
		var e gomatrixserverlib.BadJSONError
		switch {
		case err == nil:
		case errors.As(err, &e):
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(e.Error())}, e
		default:
			util.GetLogger(ctx).WithError(err).Error("buildMembershipEvents failed")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}, e
		}
	}

	if err := api.SendEvents(ctx, rsAPI, api.KindNew, events, device.UserDomain(), domain, domain, nil, false); err != nil {
		util.GetLogger(ctx).WithError(err).Error("SendEvents failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}, err
	}
	return util.JSONResponse{}, nil
}

// getProfile gets the full profile of a user by querying the database or a
// remote homeserver.
// Returns an error when something goes wrong or specifically
// eventutil.ErrProfileNotExists when the profile doesn't exist.
func getProfile(
	ctx context.Context, profileAPI userapi.ProfileAPI, cfg *config.ClientAPI,
	userID string,
	asAPI appserviceAPI.AppServiceInternalAPI,
	federation fclient.FederationClient,
) (*authtypes.Profile, error) {
	localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return nil, err
	}

	if !cfg.Matrix.IsLocalServerName(domain) {
		profile, fedErr := remoteProfile(ctx, cfg, federation, domain, userID)
		if fedErr != nil {
			var x spec.HTTPError
			if errors.As(fedErr, &x) && x.Code == http.StatusNotFound {
				return nil, appserviceAPI.ErrProfileNotExists
			}
			return nil, fedErr
		}
		profile.Localpart = localpart
		return profile, nil
	}

	profile, err := appserviceAPI.RetrieveUserProfile(ctx, userID, asAPI, profileAPI)
	if err != nil {
		return nil, err
	}

	return profile, nil
}

// remoteProfile fetches a remote user's whole profile over federation. It
// uses the signed transport because LookupProfile only decodes the display
// name and avatar URL, not custom fields.
func remoteProfile(
	ctx context.Context, cfg *config.ClientAPI, federation fclient.FederationClient,
	domain spec.ServerName, userID string,
) (*authtypes.Profile, error) {
	fedReq := fclient.NewFederationRequest(
		http.MethodGet, cfg.Matrix.ServerName, domain,
		"/_matrix/federation/v1/query/profile?user_id="+url.QueryEscape(userID),
	)
	if err := fedReq.Sign(cfg.Matrix.ServerName, cfg.Matrix.KeyID, cfg.Matrix.PrivateKey); err != nil {
		return nil, err
	}
	httpReq, err := fedReq.HTTPRequest()
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = federation.DoRequestAndParseResponse(ctx, httpReq, &fields); err != nil {
		return nil, err
	}

	profile := &authtypes.Profile{}
	// A display name or avatar URL that is not a string is left out, as
	// LookupProfile used to do.
	if raw, ok := fields[authtypes.ProfileFieldDisplayName]; ok {
		_ = json.Unmarshal(raw, &profile.DisplayName)
		delete(fields, authtypes.ProfileFieldDisplayName)
	}
	if raw, ok := fields[authtypes.ProfileFieldAvatarURL]; ok {
		_ = json.Unmarshal(raw, &profile.AvatarURL)
		delete(fields, authtypes.ProfileFieldAvatarURL)
	}
	if len(fields) > 0 {
		profile.Fields = fields
	}
	return profile, nil
}

func buildMembershipEvents(
	ctx context.Context,
	roomIDs []string,
	newProfile authtypes.Profile, userID string,
	evTime time.Time, rsAPI api.ClientRoomserverAPI,
) ([]*types.HeaderedEvent, error) {
	evs := []*types.HeaderedEvent{}

	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return nil, err
	}
	for _, roomID := range roomIDs {
		validRoomID, err := spec.NewRoomID(roomID)
		if err != nil {
			return nil, err
		}
		senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
		if err != nil {
			return nil, err
		} else if senderID == nil {
			return nil, fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validRoomID)
		}
		senderIDString := string(*senderID)
		proto := gomatrixserverlib.ProtoEvent{
			SenderID: senderIDString,
			RoomID:   roomID,
			Type:     "m.room.member",
			StateKey: &senderIDString,
		}

		content := gomatrixserverlib.MemberContent{
			Membership: spec.Join,
		}

		content.DisplayName = newProfile.DisplayName
		content.AvatarURL = newProfile.AvatarURL

		if err = proto.SetContent(content); err != nil {
			return nil, err
		}

		user, err := spec.NewUserID(userID, true)
		if err != nil {
			return nil, err
		}

		identity, err := rsAPI.SigningIdentityFor(ctx, *validRoomID, *user)
		if err != nil {
			return nil, err
		}

		event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, evTime, rsAPI, nil)
		if err != nil {
			return nil, err
		}

		evs = append(evs, event)
	}

	return evs, nil
}
