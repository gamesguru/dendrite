// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// http://matrix.org/docs/spec/client_server/r0.2.0.html#put-matrix-client-r0-rooms-roomid-send-eventtype-txnid
// http://matrix.org/docs/spec/client_server/r0.2.0.html#put-matrix-client-r0-rooms-roomid-state-eventtype-statekey
type sendEventResponse struct {
	EventID string `json:"event_id"`
}

var userRoomSendMutexes sync.Map // (roomID+userID) -> mutex. mutexes to ensure correct ordering of sendEvents

var sendEventDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "zendrite",
		Subsystem: "clientapi",
		Name:      "sendevent_duration_millis",
		Help:      "How long it takes to build and submit a new event from the client API to the roomserver",
		Buckets: []float64{ // milliseconds
			5, 10, 25, 50, 75, 100, 250, 500,
			1000, 2000, 3000, 4000, 5000, 6000,
			7000, 8000, 9000, 10000, 15000, 20000,
		},
	},
	[]string{"action"},
)

// SendEvent implements:
//
//	/rooms/{roomID}/send/{eventType}
//	/rooms/{roomID}/send/{eventType}/{txnID}
//	/rooms/{roomID}/state/{eventType}/{stateKey}
func SendEvent(
	req *http.Request,
	device *userapi.Device,
	roomID, eventType string, txnID, stateKey *string,
	cfg *config.ClientAPI,
	rsAPI api.ClientRoomserverAPI,
	txnCache *transactions.Cache,
	federation fclient.FederationClient,
) util.JSONResponse {
	if resErr := callGuestEventForbidden(device, eventType, stateKey); resErr != nil {
		return *resErr
	}
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(req.Context(), roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}
	if txnID != nil {
		// Try to fetch response from transactionsCache
		if res, ok := txnCache.FetchTransaction(device, *txnID, req.URL); ok {
			return *res
		}
	}

	stateKey, resErr := toRoomStateKey(req.Context(), rsAPI, roomVersion, roomID, stateKey)
	if resErr != nil {
		return *resErr
	}

	// create a mutex for the specific user in the specific room
	// this avoids a situation where events that are received in quick succession are sent to the roomserver in a jumbled order
	unlock, resErr := lockUserRoomSend(roomID, device.UserID)
	if resErr != nil {
		return *resErr
	}
	defer unlock()
	// Concurrent retries may have all missed the cache before waiting for the
	// room lock. Recheck after acquiring it, before creating another event.
	if txnID != nil {
		if res, ok := txnCache.FetchTransaction(device, *txnID, req.URL); ok {
			return *res
		}
	}

	var r map[string]any // must be a JSON object
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	evTime, err := httputil.ParseTSParam(req, device)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	// Only message events can be made sticky from the client API.
	var sticky *gomatrixserverlib.StickyEventData
	if stateKey == nil {
		if sticky, resErr = parseStickyDuration(req); resErr != nil {
			return *resErr
		}
	}

	var txnAndSessionID *api.TransactionID
	if txnID != nil {
		txnAndSessionID = &api.TransactionID{
			TransactionID: *txnID,
			SessionID:     device.SessionID,
		}
	}

	eventID, resErr := sendEventContent(
		req.Context(), device, roomVersion, roomID, eventType, stateKey, sticky, r, evTime, txnAndSessionID,
		cfg, rsAPI, federation,
	)
	if resErr != nil {
		return *resErr
	}

	res := util.JSONResponse{
		Code: http.StatusOK,
		JSON: sendEventResponse{eventID},
	}
	// Add response to transactionsCache
	if txnID != nil {
		txnCache.AddTransaction(device, *txnID, req.URL, &res)
	}
	return res
}

// toRoomStateKey translates user ID state keys to room keys in pseudo ID rooms.
func toRoomStateKey(
	ctx context.Context, rsAPI api.ClientRoomserverAPI,
	roomVersion gomatrixserverlib.RoomVersion, roomID string, stateKey *string,
) (*string, *util.JSONResponse) {
	if roomVersion != gomatrixserverlib.RoomVersionPseudoIDs || stateKey == nil {
		return stateKey, nil
	}
	parsedRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}
	newStateKey, err := synctypes.FromClientStateKey(*parsedRoomID, *stateKey, func(roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
		return rsAPI.QuerySenderIDForUser(ctx, roomID, userID)
	})
	if err != nil {
		// TODO: work out better logic for failure cases (e.g. sender ID not found)
		util.GetLogger(ctx).WithError(err).Error("synctypes.FromClientStateKey failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return newStateKey, nil
}

// lockUserRoomSend takes the lock that orders one user's sends into one room,
// and returns the function that releases it.
func lockUserRoomSend(roomID, userID string) (func(), *util.JSONResponse) {
	mutexVal, _ := userRoomSendMutexes.LoadOrStore(roomID+userID, &sync.Mutex{})
	mu, ok := mutexVal.(*sync.Mutex)
	if !ok {
		return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	mu.Lock()
	return mu.Unlock, nil
}

// sendEventContent builds an event from the room's current state as the
// device's user and submits it to the roomserver, returning its event ID. The
// caller holds the user's send lock for the room. Clients send through
// SendEvent; the delayed event scheduler (MSC4140) calls this directly.
//
//nolint:gocyclo
func sendEventContent(
	ctx context.Context,
	device *userapi.Device,
	roomVersion gomatrixserverlib.RoomVersion,
	roomID, eventType string, stateKey *string,
	sticky *gomatrixserverlib.StickyEventData,
	r map[string]any,
	evTime time.Time,
	txnAndSessionID *api.TransactionID,
	cfg *config.ClientAPI,
	rsAPI api.ClientRoomserverAPI,
	federation fclient.FederationClient,
) (string, *util.JSONResponse) {
	domain := device.UserDomain()

	if stateKey != nil {
		// If the existing/new state content are equal and the same user sent
		// the existing event, return the existing event_id, making the request
		// idempotent. Do not short-circuit state set by another user: their
		// request still has to be authorized by the roomserver.
		if eventID, ok := stateEqual(ctx, rsAPI, eventType, *stateKey, roomID, device.UserID, r); ok {
			return eventID, nil
		}
	}

	startedGeneratingEvent := time.Now()

	// If we're sending a membership update, make sure to strip the authorized
	// via key if it is present, otherwise other servers won't be able to auth
	// the event if the room is set to the "restricted" join rule.
	if eventType == spec.MRoomMember {
		delete(r, "join_authorised_via_users_server") //nolint:misspell
	}

	// for power level events we need to replace the userID with the pseudoID
	if roomVersion == gomatrixserverlib.RoomVersionPseudoIDs && eventType == spec.MRoomPowerLevels {
		if err := updatePowerLevels(ctx, r, roomID, rsAPI); err != nil {
			return "", &util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{Err: err.Error()},
			}
		}
	}

	e, resErr := generateSendEvent(ctx, r, device, roomID, eventType, stateKey, sticky, rsAPI, evTime)
	if resErr != nil {
		return "", resErr
	}
	if stateKey == nil {
		if resErr = validateAnnotation(ctx, rsAPI, e, txnAndSessionID); resErr != nil {
			return "", resErr
		}
	}
	timeToGenerateEvent := time.Since(startedGeneratingEvent)

	// validate that the aliases exists
	if eventType == spec.MRoomCanonicalAlias && stateKey != nil && *stateKey == "" {
		aliasReq := api.AliasEvent{}
		if err := json.Unmarshal(e.Content(), &aliasReq); err != nil {
			res := util.ErrorResponse(fmt.Errorf("unable to parse alias event: %w", err))
			return "", &res
		}
		if !aliasReq.Valid() {
			return "", &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Request contains invalid aliases."),
			}
		}

		if resErr = validateCanonicalAliases(ctx, aliasReq, roomID, cfg, rsAPI, federation); resErr != nil {
			return "", resErr
		}
	}

	// Pass the new event to the roomserver. If this retries a transaction
	// that already produced an event, possibly on another process before an
	// HA takeover, the roomserver discards this event and names that one.
	startedSubmittingEvent := time.Now()
	eventID, err := api.SendEventForTransaction(
		ctx, rsAPI,
		&types.HeaderedEvent{PDU: e},
		domain,
		domain,
		domain,
		txnAndSessionID,
	)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("SendEvents failed")
		return "", &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	timeToSubmitEvent := time.Since(startedSubmittingEvent)
	util.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id":     eventID,
		"room_id":      roomID,
		"room_version": roomVersion,
	}).Info("Sent event to roomserver")

	// Take a note of how long it took to generate the event vs submit
	// it to the roomserver.
	sendEventDuration.With(prometheus.Labels{"action": "build"}).Observe(float64(timeToGenerateEvent.Milliseconds()))
	sendEventDuration.With(prometheus.Labels{"action": "submit"}).Observe(float64(timeToSubmitEvent.Milliseconds()))

	return eventID, nil
}

func updatePowerLevels(ctx context.Context, r map[string]any, roomID string, rsAPI api.ClientRoomserverAPI) error {
	users, ok := r["users"]
	if !ok {
		return nil
	}
	userMap, ok := users.(map[string]any)
	if !ok {
		return nil
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}
	for user, level := range userMap {
		uID, err := spec.NewUserID(user, true)
		if err != nil {
			continue // we're modifying the map in place, so we're going to have invalid userIDs after the first iteration
		}
		senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *uID)
		if err != nil {
			return err
		} else if senderID == nil {
			util.GetLogger(ctx).Warnf("sender ID not found for %s in %s", uID, *validRoomID)
			continue
		}
		userMap[string(*senderID)] = level
		delete(userMap, user)
	}
	r["users"] = userMap
	return nil
}

// stateEqual compares the new and the existing state event content. If they
// are equal and the existing event was sent by senderID, it returns the
// existing event ID, making the request idempotent.
func stateEqual(ctx context.Context, rsAPI api.ClientRoomserverAPI, eventType, stateKey, roomID, senderID string, newContent map[string]any) (string, bool) {
	stateRes := api.QueryCurrentStateResponse{}
	tuple := gomatrixserverlib.StateKeyTuple{
		EventType: eventType,
		StateKey:  stateKey,
	}
	err := rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
		RoomID:      roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{tuple},
	}, &stateRes)
	if err != nil {
		return "", false
	}
	if existingEvent, ok := stateRes.StateEvents[tuple]; ok {
		var existingContent map[string]any
		if err = json.Unmarshal(existingEvent.Content(), &existingContent); err != nil {
			return "", false
		}
		if !reflect.DeepEqual(existingContent, newContent) {
			return "", false
		}
		// Events are stored with the sender ID, which differs from the user ID
		// in pseudo ID rooms.
		parsedRoomID, err := spec.NewRoomID(roomID)
		if err != nil {
			return "", false
		}
		parsedUserID, err := spec.NewUserID(senderID, true)
		if err != nil {
			return "", false
		}
		requesterSenderID, err := rsAPI.QuerySenderIDForUser(ctx, *parsedRoomID, *parsedUserID)
		if err != nil || requesterSenderID == nil {
			return "", false
		}
		if existingEvent.SenderID() == *requesterSenderID {
			return existingEvent.EventID(), true
		}
	}
	return "", false
}

// suspendedMayEvent reports whether a suspended user may send the event
// (MSC3823): a redaction, which may only target their own events, or leaving
// a room through its membership state.
func suspendedMayEvent(content map[string]any, userID string, senderID spec.SenderID, eventType string, stateKey *string) bool {
	if stateKey == nil {
		return eventType == spec.MRoomRedaction
	}
	if eventType != spec.MRoomMember || (*stateKey != userID && *stateKey != string(senderID)) {
		return false
	}
	return content["membership"] == spec.Leave
}

func generateSendEvent(
	ctx context.Context,
	r map[string]any,
	device *userapi.Device,
	roomID, eventType string, stateKey *string,
	sticky *gomatrixserverlib.StickyEventData,
	rsAPI api.ClientRoomserverAPI,
	evTime time.Time,
) (gomatrixserverlib.PDU, *util.JSONResponse) {
	// parse the incoming http request
	fullUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Bad userID"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.NotFound("internal server error"),
		}
	} else if senderID == nil {
		// TODO: is it always the case that lack of a sender ID means they're not joined?
		//       And should this logic be deferred to the roomserver somehow?
		return nil, &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("not joined to room"),
		}
	}

	if device.Suspended && !suspendedMayEvent(r, device.UserID, *senderID, eventType, stateKey) {
		res := auth.UserSuspendedResponse()
		return nil, &res
	}

	// create the new event and set all the fields we can
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   roomID,
		Type:     eventType,
		StateKey: stateKey,
	}
	// MSC4169: a redaction sent through /send carries the redacted event ID
	// in content.redacts. Copy it to the top level, where room versions before
	// v11 expect it; /redact also sets both. It gets the same permission
	// check as /redact.
	if eventType == spec.MRoomRedaction && stateKey == nil {
		redacts, ok := r["redacts"].(string)
		if !ok || redacts == "" {
			return nil, &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("content.redacts must be an event ID"),
			}
		}
		if res := checkRedactionAllowed(ctx, rsAPI, roomID, redacts, *senderID, device.Suspended); res != nil {
			return nil, res
		}
		proto.Redacts = redacts
	}
	setSticky(&proto, sticky)
	err = proto.SetContent(r)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("proto.SetContent failed")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if proto.Type == spec.MRoomCreate && proto.StateKey != nil && *proto.StateKey == "" {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("cannot resend m.room.create event"),
		}
	}

	if res := validateThreadRelation(ctx, rsAPI, roomID, r); res != nil {
		return nil, res
	}

	identity, err := rsAPI.SigningIdentityFor(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var queryRes api.QueryLatestEventsAndStateResponse
	e, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, evTime, rsAPI, &queryRes)
	{
		var specificErr eventutil.ErrRoomNoExists
		var specificErr1 gomatrixserverlib.BadJSONError
		var specificErr2 gomatrixserverlib.EventValidationError
		switch {
		case err == nil:
		case errors.As(err, &specificErr):
			return nil, &util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Room does not exist")}
		case errors.As(err, &specificErr1):
			return nil, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(specificErr1.Error())}
		case errors.As(err, &specificErr2):
			if specificErr2.Code == gomatrixserverlib.EventValidationTooLarge {
				return nil, &util.JSONResponse{Code: http.StatusRequestEntityTooLarge, JSON: spec.BadJSON(specificErr2.Error())}
			}
			return nil, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(specificErr2.Error())}
		default:
			util.GetLogger(ctx).WithError(err).Error("eventutil.BuildEvent failed")
			return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
	}

	// check to see if this user can perform this operation
	stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider, err := gomatrixserverlib.NewAuthEvents(gomatrixserverlib.ToPDUs(stateEvents))
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(err.Error()),
		}
	}
	if err = gomatrixserverlib.Allowed(e.PDU, provider, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, *validRoomID, senderID)
	}); err != nil {
		code := 403
		var validationErr *gomatrixserverlib.EventValidationError
		if errors.As(err, &validationErr) {
			code = validationErr.Code
		}
		return nil, &util.JSONResponse{
			Code: code,
			JSON: spec.Forbidden(err.Error()), // TODO: Is this error string comprehensible to the client?
		}
	}

	// User should not be able to send a tombstone event to the same room.
	if e.Type() == "m.room.tombstone" {
		content := make(map[string]any)
		if err = json.Unmarshal(e.Content(), &content); err != nil {
			util.GetLogger(ctx).WithError(err).Error("Cannot unmarshal the event content.")
			return nil, &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("Cannot unmarshal the event content."),
			}
		}
		if content["replacement_room"] == e.RoomID().String() {
			return nil, &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Cannot send tombstone event that points to the same room."),
			}
		}
	}

	return e.PDU, nil
}

// msc4354StickyDurationParam is the unstable query parameter that asks for a
// sticky event (MSC4354).
const msc4354StickyDurationParam = "org.matrix.msc4354.sticky_duration_ms"

// parseStickyDuration reads the MSC4354 sticky duration of a send request,
// which is nil if the request does not ask for a sticky event. As in Synapse,
// any non-negative duration is accepted and kept in the event; servers work
// out when stickiness ends with the duration capped at an hour.
func parseStickyDuration(req *http.Request) (*gomatrixserverlib.StickyEventData, *util.JSONResponse) {
	values, ok := req.URL.Query()[msc4354StickyDurationParam]
	if !ok {
		return nil, nil
	}
	durationMS, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("Query parameter %q must be an integer", msc4354StickyDurationParam)),
		}
	}
	if durationMS < 0 {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("Query parameter %q must be a non-negative integer", msc4354StickyDurationParam)),
		}
	}
	return &gomatrixserverlib.StickyEventData{DurationMillis: durationMS}, nil
}

// setSticky makes the event built from proto an MSC4354 sticky event, unless
// sticky is nil. Every way of sending an event passes the sticky duration of
// the request through here.
func setSticky(proto *gomatrixserverlib.ProtoEvent, sticky *gomatrixserverlib.StickyEventData) {
	proto.UnstableSticky = sticky
}

// validateThreadRelation rejects a thread whose root is itself a relation,
// as the spec asks of servers. An unknown root is left for clients to handle.
func validateThreadRelation(ctx context.Context, rsAPI api.ClientRoomserverAPI, roomID string, content map[string]any) *util.JSONResponse {
	relation, ok := content["m.relates_to"].(map[string]any)
	if !ok || relation["rel_type"] != "m.thread" {
		return nil
	}
	rootID, ok := relation["event_id"].(string)
	if !ok || rootID == "" {
		return nil
	}
	var res api.QueryEventsByIDResponse
	if err := rsAPI.QueryEventsByID(ctx, &api.QueryEventsByIDRequest{RoomID: roomID, EventIDs: []string{rootID}}, &res); err != nil {
		util.GetLogger(ctx).WithError(err).Error("rsAPI.QueryEventsByID failed")
		return &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	for _, root := range res.Events {
		// The roomserver does not filter by room, and another room's event must not be probed.
		if root.EventID() == rootID && root.RoomID().String() == roomID && gjson.GetBytes(root.Content(), `m\.relates_to.rel_type`).Str != "" {
			return &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("Cannot start a thread from an event with a relation"),
			}
		}
	}
	return nil
}

// validateAnnotation rejects an annotation that the sender has already made
// with the same event type and key on the same event and not redacted, as
// the spec asks of servers (MSC2677). The send lock for the sender and room
// is held, so the sender cannot race this check with another request. A
// retry of the transaction that produced the annotation is not rejected, so
// that it is answered with the original event even after an HA takeover.
func validateAnnotation(ctx context.Context, rsAPI api.ClientRoomserverAPI, e gomatrixserverlib.PDU, txnID *api.TransactionID) *util.JSONResponse {
	relation := gjson.GetBytes(e.Content(), `m\.relates_to`)
	targetID, key := relation.Get("event_id"), relation.Get("key")
	if relation.Get("rel_type").Str != "m.annotation" || targetID.Type != gjson.String || key.Type != gjson.String {
		return nil
	}
	exists, err := rsAPI.QueryAnnotationExists(ctx, e.RoomID().String(), targetID.Str, e.SenderID(), e.Type(), key.Str, txnID)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("rsAPI.QueryAnnotationExists failed")
		return &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if exists {
		return &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{ErrCode: "M_DUPLICATE_ANNOTATION", Err: "Cannot send the same annotation twice"},
		}
	}
	return nil
}

// validateCanonicalAliases resolves newly advertised aliases, including remote ones.
// Existing aliases need not be resolved again, as they may have been removed from
// the directory since the previous state event (MSC2432).
func validateCanonicalAliases(ctx context.Context, content api.AliasEvent, roomID string, cfg *config.ClientAPI, rsAPI api.ClientRoomserverAPI, federation fclient.FederationClient) *util.JSONResponse {
	tuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCanonicalAlias, StateKey: ""}
	var state api.QueryCurrentStateResponse
	if err := rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{RoomID: roomID, StateTuples: []gomatrixserverlib.StateKeyTuple{tuple}}, &state); err != nil {
		return &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	existing := make(map[string]bool)
	if event := state.StateEvents[tuple]; event != nil {
		var previous api.AliasEvent
		if err := json.Unmarshal(event.Content(), &previous); err != nil {
			return &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
		existing[previous.Alias] = true
		for _, alias := range previous.AltAliases {
			existing[alias] = true
		}
	}
	aliases := append([]string{}, content.AltAliases...)
	if content.Alias != "" {
		aliases = append(aliases, content.Alias)
	}
	for _, alias := range aliases {
		if existing[alias] {
			continue
		}
		_, domain, err := gomatrixserverlib.SplitID('#', alias)
		if err != nil {
			return &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Request contains invalid aliases.")}
		}
		var resolvedRoomID string
		if cfg.Matrix.IsLocalServerName(domain) {
			var result api.GetRoomIDForAliasResponse
			if err = rsAPI.GetRoomIDForAlias(ctx, &api.GetRoomIDForAliasRequest{Alias: alias, IncludeAppservices: true}, &result); err != nil {
				return &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
			}
			resolvedRoomID = result.RoomID
		} else {
			result, lookupErr := federation.LookupRoomAlias(ctx, cfg.Matrix.ServerName, domain, alias)
			if lookupErr == nil {
				resolvedRoomID = result.RoomID
			}
		}
		if resolvedRoomID != roomID {
			return &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadAlias(fmt.Sprintf("The alias %q does not point to this room.", alias))}
		}
		existing[alias] = true
	}
	return nil
}
