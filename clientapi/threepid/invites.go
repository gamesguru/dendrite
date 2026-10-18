// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package threepid

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"slices"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// MembershipRequest represents the body of an incoming POST request
// on /rooms/{roomID}/(join|kick|ban|unban|leave|invite).
type MembershipRequest struct {
	UserID        string `json:"user_id"`
	Reason        string `json:"reason"`
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
	Medium        string `json:"medium"`
	Address       string `json:"address"`
}

// idServerStoreInviteResponse represents the response of POST /_matrix/identity/v2/store-invite.
type idServerStoreInviteResponse struct {
	PublicKey   string                        `json:"public_key"`
	Token       string                        `json:"token"`
	DisplayName string                        `json:"display_name"`
	PublicKeys  []gomatrixserverlib.PublicKey `json:"public_keys"`
}

var (
	errMissingParameter = fmt.Errorf("'address', 'id_server', 'id_access_token' and 'medium' must all be supplied")
	errNotTrusted       = fmt.Errorf("untrusted server")
)

// ErrMissingParameter is the error raised if a request for 3PID invite has
// an incomplete body.
type ErrMissingParameter struct{}

func (e ErrMissingParameter) Error() string {
	return errMissingParameter.Error()
}

func (e ErrMissingParameter) Unwrap() error {
	return errMissingParameter
}

// ErrNotTrusted is the error raised if an identity server isn't in the list
// of trusted servers in the configuration file.
type ErrNotTrusted struct{}

func (e ErrNotTrusted) Error() string {
	return errNotTrusted.Error()
}

func (e ErrNotTrusted) Unwrap() error {
	return errNotTrusted
}

// CheckAndProcessInvite analyses the body of an incoming membership request.
// If the fields relative to a third-party-invite are all supplied, lookups the
// matching Matrix ID from the given identity server. If no Matrix ID is
// associated to the given 3PID, asks the identity server to store the invite
// and emit a "m.room.third_party_invite" event.
// Returns a representation of the HTTP response to send to the user.
// Returns a representation of a non-200 HTTP response if something went wrong
// in the process, or if some 3PID fields aren't supplied but others are.
// If none of the 3PID-specific fields are supplied, or if a Matrix ID is
// supplied by the identity server, returns nil to indicate that the request
// must be processed as a non-3PID membership request. In the latter case,
// fills the Matrix ID in the request body so a normal invite membership event
// can be emitted.
func CheckAndProcessInvite(
	ctx context.Context,
	device *userapi.Device, body *MembershipRequest, cfg *config.ClientAPI,
	rsAPI api.ClientRoomserverAPI, db userapi.ClientUserAPI, client *fclient.Client,
	roomID string,
	evTime time.Time,
) (inviteStoredOnIDServer bool, err error) {
	if body.Address == "" && body.IDServer == "" && body.Medium == "" {
		// If none of the 3PID-specific fields are supplied, it's a standard invite
		// so return nil for it to be processed as such
		return
	} else if body.Address == "" || body.IDServer == "" || body.Medium == "" || body.IDAccessToken == "" {
		// If at least one of the 3PID-specific fields is supplied but not all
		// of them, return an error
		err = ErrMissingParameter{}
		return
	}
	body.Address = authtypes.CanonicalThreePIDAddress(body.Medium, body.Address)

	auth := IdentityAuth{Token: body.IDAccessToken, UserAPI: db, Config: cfg, Client: client}
	mxid, err := lookup3PID(ctx, auth, body)
	if err != nil {
		return
	}

	if mxid == "" {
		// No Matrix ID could be found for this 3PID, meaning that a
		// "m.room.third_party_invite" have to be emitted from the data the
		// identity server stored the invite with.
		var storeInviteRes *idServerStoreInviteResponse
		storeInviteRes, err = storeInvite(ctx, auth, rsAPI, device, body, roomID)
		if err != nil {
			return
		}
		err = emit3PIDInviteEvent(
			ctx, body, storeInviteRes, device, roomID, cfg, rsAPI, evTime,
		)
		inviteStoredOnIDServer = err == nil

		return
	}

	// A Matrix ID have been found: set it in the body request and let the process
	// continue to create a "m.room.member" event with an "invite" membership
	body.UserID = mxid

	return
}

// lookup3PID looks up the Matrix ID bound to a 3PID on the identity server
// with the v2 API, which compares hashes of the 3PID when the identity server
// supports sha256, and returns an empty string if there is none.
func lookup3PID(ctx context.Context, auth IdentityAuth, body *MembershipRequest) (string, error) {
	var details struct {
		Algorithms []string `json:"algorithms"`
		Pepper     string   `json:"lookup_pepper"`
	}
	if err := auth.do(ctx, body.IDServer, http.MethodGet, "/_matrix/identity/v2/hash_details", nil, nil, &details); err != nil {
		return "", err
	}

	// The lookup string is the same for both algorithms, see
	// https://spec.matrix.org/latest/identity-service-api/#client-behaviour
	lookup := body.Address + " " + body.Medium
	var algorithm string
	switch {
	case slices.Contains(details.Algorithms, "sha256"):
		algorithm = "sha256"
		sum := sha256.Sum256([]byte(lookup + " " + details.Pepper))
		lookup = base64.RawURLEncoding.EncodeToString(sum[:])
	case slices.Contains(details.Algorithms, "none"):
		algorithm = "none"
	default:
		return "", fmt.Errorf("identity server %s supports no known lookup algorithm: %v", body.IDServer, details.Algorithms)
	}

	var res struct {
		Mappings map[string]string `json:"mappings"`
	}
	if err := auth.do(ctx, body.IDServer, http.MethodPost, "/_matrix/identity/v2/lookup", nil, map[string]any{
		"addresses": []string{lookup},
		"algorithm": algorithm,
		"pepper":    details.Pepper,
	}, &res); err != nil {
		return "", err
	}
	return res.Mappings[lookup], nil
}

// storeInvite asks the identity server to store a 3PID invite with POST
// /_matrix/identity/v2/store-invite, passing on what the email to the invitee
// can show about the room and its sender.
func storeInvite(
	ctx context.Context, auth IdentityAuth, rsAPI api.ClientRoomserverAPI,
	device *userapi.Device, body *MembershipRequest, roomID string,
) (*idServerStoreInviteResponse, error) {
	request := map[string]string{
		"medium":  body.Medium,
		"address": body.Address,
		"room_id": roomID,
		"sender":  device.UserID,
	}

	_, serverName, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		return nil, err
	}
	if auth.Config.Matrix.IsLocalServerName(serverName) {
		profile, profileErr := auth.UserAPI.QueryProfile(ctx, device.UserID)
		if profileErr != nil {
			return nil, profileErr
		}
		request["sender_display_name"] = profile.DisplayName
		request["sender_avatar_url"] = profile.AvatarURL
	}

	// The room's details are optional, so failing to read them is not fatal.
	state := api.QueryCurrentStateResponse{}
	if err = rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
		RoomID: roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomCreate},
			{EventType: spec.MRoomName},
			{EventType: spec.MRoomAvatar},
			{EventType: spec.MRoomJoinRules},
			{EventType: spec.MRoomCanonicalAlias},
		},
	}, &state); err != nil {
		util.GetLogger(ctx).WithError(err).Warn("Failed to read the room's details for a 3PID invite")
	}
	for tuple, ev := range state.StateEvents {
		content := gjson.ParseBytes(ev.Content())
		switch tuple.EventType {
		case spec.MRoomCreate:
			// MSC3288: tell the identity server what kind of room this is.
			request["room_type"] = content.Get("type").Str
		case spec.MRoomName:
			request["room_name"] = content.Get("name").Str
		case spec.MRoomAvatar:
			request["room_avatar_url"] = content.Get("url").Str
		case spec.MRoomJoinRules:
			request["room_join_rules"] = content.Get("join_rule").Str
		case spec.MRoomCanonicalAlias:
			request["room_alias"] = content.Get("alias").Str
		}
	}
	for key, value := range request {
		if value == "" {
			delete(request, key)
		}
	}

	var res idServerStoreInviteResponse
	if err = auth.do(ctx, body.IDServer, http.MethodPost, "/_matrix/identity/v2/store-invite", nil, request, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// emit3PIDInviteEvent builds and sends a "m.room.third_party_invite" event.
// Returns an error if something failed in the process.
func emit3PIDInviteEvent(
	ctx context.Context,
	body *MembershipRequest, res *idServerStoreInviteResponse,
	device *userapi.Device, roomID string, cfg *config.ClientAPI,
	rsAPI api.ClientRoomserverAPI,
	evTime time.Time,
) error {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return err
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}
	sender, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *userID)
	if err != nil {
		return err
	} else if sender == nil {
		return fmt.Errorf("sender ID not found for %s in %s", *userID, *validRoomID)
	}
	proto := &gomatrixserverlib.ProtoEvent{
		SenderID: string(*sender),
		RoomID:   roomID,
		Type:     "m.room.third_party_invite",
		StateKey: &res.Token,
	}

	validityURL := fmt.Sprintf("https://%s/_matrix/identity/v2/pubkey/isvalid", body.IDServer)
	content := gomatrixserverlib.ThirdPartyInviteContent{
		DisplayName:    res.DisplayName,
		KeyValidityURL: validityURL,
		PublicKey:      res.PublicKey,
		PublicKeys:     res.PublicKeys,
	}

	if err = proto.SetContent(content); err != nil {
		return err
	}

	identity, err := cfg.Matrix.SigningIdentityFor(device.UserDomain())
	if err != nil {
		return err
	}

	queryRes := api.QueryLatestEventsAndStateResponse{}
	event, err := eventutil.QueryAndBuildEvent(ctx, proto, identity, evTime, rsAPI, &queryRes)
	if err != nil {
		return err
	}

	return api.SendEvents(
		ctx, rsAPI,
		api.KindNew,
		[]*types.HeaderedEvent{
			event,
		},
		device.UserDomain(),
		cfg.Matrix.ServerName,
		cfg.Matrix.ServerName,
		nil,
		false,
	)
}
