// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	clientutil "codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// MSC4140 delayed events, in the unstable form that matrix-js-sdk and Synapse
// use: a delay query parameter on the send and state endpoints schedules an
// event, and the management endpoints restart, cancel or send it early.
const (
	delayQueryParam = "org.matrix.msc4140.delay"
	// The unstable form of M_DELAY_TOO_LARGE.
	errorDelayTooLarge spec.MatrixErrorCode = "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE"
	// MSC4140's spelling of the finalized status.
	statusFinalised = "finalised" //nolint:misspell // MSC4140's spelling
	// How long the scheduler waits after a storage error.
	delayedEventsRetry = 5 * time.Second
	// How often finalized delayed events past their retention are forgotten.
	delayedEventsPurgeInterval = time.Hour
)

// DelayedEvents schedules, manages and sends MSC4140 delayed events. Its
// scheduler runs only in the process that serves clients, which in HA is the
// single active one; the user API stores the events.
type DelayedEvents struct {
	cfg        *config.ClientAPI
	rsAPI      api.ClientRoomserverAPI
	userAPI    userapi.ClientUserAPI
	federation fclient.FederationClient
	txnCache   *transactions.Cache
	// rateLimits limits scheduling like any other rate-limited request;
	// mgmtLimits limits the management and lookup requests.
	rateLimits *httputil.RateLimits
	mgmtLimits *httputil.RateLimits
	now        func() time.Time
	// wake asks the scheduler to look for the next send time again.
	wake chan struct{}
}

func newDelayedEvents(
	cfg *config.ClientAPI,
	rsAPI api.ClientRoomserverAPI,
	userAPI userapi.ClientUserAPI,
	federation fclient.FederationClient,
	txnCache *transactions.Cache,
	rateLimits, mgmtLimits *httputil.RateLimits,
) *DelayedEvents {
	return &DelayedEvents{
		cfg:        cfg,
		rsAPI:      rsAPI,
		userAPI:    userAPI,
		federation: federation,
		txnCache:   txnCache,
		rateLimits: rateLimits,
		mgmtLimits: mgmtLimits,
		now:        time.Now,
		wake:       make(chan struct{}, 1),
	}
}

// Requested reports whether a send or state request asks for a delay.
func (d *DelayedEvents) Requested(req *http.Request) bool {
	return req.URL.Query().Has(delayQueryParam)
}

type delayedEventScheduledResponse struct {
	DelayID string `json:"delay_id"`
}

// Schedule implements the delay query parameter of
//
//	PUT /rooms/{roomID}/send/{eventType}/{txnID}
//	PUT /rooms/{roomID}/state/{eventType}/{stateKey}
func (d *DelayedEvents) Schedule(
	req *http.Request, device *userapi.Device,
	roomID, eventType string, txnID, stateKey *string,
) util.JSONResponse {
	delayMS, err := strconv.ParseInt(req.URL.Query().Get(delayQueryParam), 10, 64)
	if err != nil || delayMS <= 0 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("%s must be a positive integer", delayQueryParam)),
		}
	}
	var content map[string]any
	if resErr := clientutil.UnmarshalJSONRequest(req, &content); resErr != nil {
		return *resErr
	}
	return d.schedule(req, device, roomID, eventType, txnID, stateKey, delayMS, content)
}

type delayedEventRequest struct {
	DelayMS  *int64         `json:"delay_ms"`
	StateKey *string        `json:"state_key"`
	Content  map[string]any `json:"content"`
}

// ScheduleUnstable implements
//
//	PUT /_matrix/client/unstable/org.matrix.msc4140/rooms/{roomID}/delayed_event/{eventType}/{txnID}
//
// the dedicated scheduling endpoint of the current MSC4140 text.
func (d *DelayedEvents) ScheduleUnstable(
	req *http.Request, device *userapi.Device, roomID, eventType, txnID string,
) util.JSONResponse {
	var body delayedEventRequest
	if resErr := clientutil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	if body.DelayMS == nil || body.Content == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("delay_ms and content are required"),
		}
	}
	if *body.DelayMS <= 0 {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("delay_ms must be a positive integer"),
		}
	}
	return d.schedule(req, device, roomID, eventType, &txnID, body.StateKey, *body.DelayMS, body.Content)
}

func (d *DelayedEvents) schedule(
	req *http.Request, device *userapi.Device,
	roomID, eventType string, txnID, stateKey *string,
	delayMS int64, content map[string]any,
) util.JSONResponse {
	ctx := req.Context()
	if r := d.rateLimits.Limit(req, device); r != nil {
		return *r
	}
	if resErr := callGuestEventForbidden(device, eventType, stateKey); resErr != nil {
		return *resErr
	}
	if !d.cfg.DelayedEvents.Enabled() {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Sending delayed events has been disallowed"),
		}
	}
	if maxDelay := d.cfg.DelayedEvents.MaxDelay.Milliseconds(); delayMS > maxDelay {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{
				ErrCode: errorDelayTooLarge,
				Err:     fmt.Sprintf("The requested delay (%dms) exceeds the allowed maximum (%dms)", delayMS, maxDelay),
			},
		}
	}

	// Like a direct send, only a message event can be made sticky (MSC4354).
	var sticky *gomatrixserverlib.StickyEventData
	if stateKey == nil {
		var resErr *util.JSONResponse
		if sticky, resErr = parseStickyDuration(req); resErr != nil {
			return *resErr
		}
	}

	if txnID != nil {
		if res, ok := d.txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}
	var originServerTS spec.Timestamp
	if device.AppserviceID != "" && req.URL.Query().Has("ts") {
		ts, err := clientutil.ParseTSParam(req, device)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(err.Error()),
			}
		}
		originServerTS = spec.AsTimestamp(ts)
	}
	roomVersion, err := d.rsAPI.QueryRoomVersionForRoom(ctx, roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}
	roomStateKey, resErr := toRoomStateKey(ctx, d.rsAPI, roomVersion, roomID, stateKey)
	if resErr != nil {
		return *resErr
	}

	unlock, resErr := lockUserRoomSend(roomID, device.UserID)
	if resErr != nil {
		return *resErr
	}
	defer unlock()
	if txnID != nil {
		if res, ok := d.txnCache.FetchTransaction(device.AccessToken, *txnID, req.URL); ok {
			return *res
		}
	}

	// The event is built again from the room state when it is sent. Building
	// it now checks that it is well formed and that the user may send it, so
	// that nothing is stored for a room the user cannot send to.
	if _, resErr = generateSendEvent(ctx, content, device, roomID, eventType, roomStateKey, sticky, d.rsAPI, d.now()); resErr != nil {
		return *resErr
	}

	rawContent, err := json.Marshal(content)
	if err != nil {
		return util.ErrorResponse(err)
	}
	localpart, serverName, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		return util.ErrorResponse(err)
	}
	now := spec.AsTimestamp(d.now())
	event := &userapi.DelayedEvent{
		DelayID:        rand.Text(),
		Localpart:      localpart,
		ServerName:     serverName,
		DeviceID:       device.ID,
		RoomID:         roomID,
		Type:           eventType,
		StateKey:       stateKey,
		Content:        rawContent,
		OriginServerTS: originServerTS,
		DelayMS:        delayMS,
		DelayedSince:   now,
		SendTS:         now + spec.Timestamp(delayMS),
	}
	if sticky != nil {
		event.StickyDurationMS = &sticky.DurationMillis
	}
	err = d.userAPI.PerformDelayedEventAdd(ctx, event, d.cfg.DelayedEvents.MaxPerUser)
	var limitErr *userapi.DelayedEventLimitError
	switch {
	case errors.As(err, &limitErr):
		return httputil.LimitExceededResponse(
			"The maximum number of delayed events has been reached.",
			limitErr.NextSendTS.Time().Sub(d.now()),
		)
	case err != nil:
		util.GetLogger(ctx).WithError(err).Error("userAPI.PerformDelayedEventAdd failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	d.notify()

	res := util.JSONResponse{
		Code: http.StatusOK,
		JSON: delayedEventScheduledResponse{DelayID: event.DelayID},
	}
	if txnID != nil {
		d.txnCache.AddTransaction(device.AccessToken, *txnID, req.URL, &res)
	}
	return res
}

// managementAPI serves a management endpoint with or without an access
// token, as Synapse does: knowing the delay ID is enough to manage an event.
// An access token rate-limits per device instead of per address, and an
// invalid one is refused rather than ignored.
func (d *DelayedEvents) managementAPI(name string, f func(*http.Request, *userapi.Device) util.JSONResponse) http.Handler {
	return httputil.MakeOptionalAuthAPI(name, d.userAPI, func(req *http.Request, device *userapi.Device) util.JSONResponse {
		if device == nil {
			if _, err := auth.ExtractAccessToken(req); err == nil {
				if _, resErr := auth.VerifyUserFromRequest(req, d.userAPI); resErr != nil {
					return *resErr
				}
			}
		}
		return f(req, device)
	}, httputil.WithAllowGuests(), httputil.WithAllowCallGuests())
}

type delayedEventAction string

const (
	delayedEventCancel  delayedEventAction = "cancel"
	delayedEventRestart delayedEventAction = "restart"
	delayedEventSend    delayedEventAction = "send"
)

// UpdateWithActionInBody implements
//
//	POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayID}
//
// the form of the management endpoint that earlier revisions of MSC4140 had.
func (d *DelayedEvents) UpdateWithActionInBody(req *http.Request, device *userapi.Device, delayID string) util.JSONResponse {
	var body map[string]any
	if resErr := clientutil.UnmarshalJSONRequest(req, &body); resErr != nil {
		if resErr.Code == http.StatusBadRequest {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.NotJSON("Content not JSON.")}
		}
		return *resErr
	}
	if body == nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.NotJSON("Content must be a JSON object.")}
	}
	value, ok := body["action"]
	if !ok {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("'action' is missing")}
	}
	action, _ := value.(string)
	switch delayedEventAction(action) {
	case delayedEventCancel, delayedEventRestart, delayedEventSend:
		return d.Update(req, device, delayID, delayedEventAction(action))
	default:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("'action' is not one of 'cancel', 'restart', 'send'"),
		}
	}
}

// Update implements
//
//	POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayID}/{action}
func (d *DelayedEvents) Update(req *http.Request, device *userapi.Device, delayID string, action delayedEventAction) util.JSONResponse {
	if r := d.mgmtLimits.Limit(req, device); r != nil {
		return *r
	}
	ctx := req.Context()
	var done bool
	var err error
	switch action {
	case delayedEventCancel:
		done, err = d.userAPI.PerformDelayedEventCancel(ctx, delayID, spec.AsTimestamp(d.now()))
	case delayedEventRestart:
		done, err = d.userAPI.PerformDelayedEventRestart(ctx, delayID, spec.AsTimestamp(d.now()))
	case delayedEventSend:
		var event *userapi.DelayedEvent
		if event, err = d.userAPI.PerformDelayedEventClaim(ctx, delayID); err == nil && event != nil {
			return d.sendNow(ctx, event)
		}
	}
	if err != nil {
		util.GetLogger(ctx).WithError(err).Errorf("failed to %s delayed event", action)
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if done || d.alreadyFinalizedAs(ctx, delayID, action) {
		return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
	}
	return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Delayed event not found")}
}

// alreadyFinalizedAs reports whether an action may be safely retried because
// the event is already finalized with the outcome the action asks for: sent
// for send, and canceled, whether by a client or an error, for cancel.
//
// MSC4140 answers a conflicting outcome with 409. Synapse, which forgets
// finalized events, answers 404, and matrix-js-sdk's MatrixRTC membership
// manager schedules a new delayed event when a restart finds none, so a
// conflict is reported as 404 here as well.
func (d *DelayedEvents) alreadyFinalizedAs(ctx context.Context, delayID string, action delayedEventAction) bool {
	event, err := d.userAPI.QueryDelayedEvent(ctx, delayID)
	if err != nil || event == nil || event.Status != userapi.DelayedEventFinalized {
		return false
	}
	switch action {
	case delayedEventSend:
		return event.EventID != ""
	case delayedEventCancel:
		return event.EventID == ""
	default:
		return false
	}
}

// sendNow sends a claimed event for the send action. Unlike a send at the
// scheduled time, a failure leaves the event scheduled, as MSC4140 asks, so
// that the client may retry until it is due.
func (d *DelayedEvents) sendNow(ctx context.Context, event *userapi.DelayedEvent) util.JSONResponse {
	eventID, resErr := d.sendEvent(ctx, event)
	if resErr != nil {
		if err := d.userAPI.PerformDelayedEventUnclaim(context.WithoutCancel(ctx), event.DelayID); err != nil {
			util.GetLogger(ctx).WithError(err).Error("failed to reschedule delayed event")
		}
		// It may have become due while it was claimed.
		d.notify()
		return *resErr
	}
	d.finalize(ctx, event, eventID, nil)
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}

type delayedEventResponse struct {
	DelayID        string                    `json:"delay_id"`
	RoomID         string                    `json:"room_id"`
	Type           string                    `json:"type"`
	StateKey       *string                   `json:"state_key,omitempty"`
	DelayMS        int64                     `json:"delay_ms"`
	DelayedSinceTS spec.Timestamp            `json:"delayed_since_ts"`
	Content        json.RawMessage           `json:"content"`
	Finalized      *delayedEventFinalization `json:"finalised,omitempty"` //nolint:misspell // MSC4140's spelling
}

type delayedEventFinalization struct {
	Error       json.RawMessage `json:"error,omitempty"`
	EventID     string          `json:"event_id,omitempty"`
	FinalizedTS spec.Timestamp  `json:"finalised_ts"` //nolint:misspell // MSC4140's spelling
}

// delayedEventListEntry adds the field names of earlier revisions of
// MSC4140 to the list, as Synapse does for clients that still read them.
type delayedEventListEntry struct {
	delayedEventResponse
	Delay        int64          `json:"delay"`
	RunningSince spec.Timestamp `json:"running_since"`
}

func newDelayedEventResponse(event *userapi.DelayedEvent) delayedEventResponse {
	res := delayedEventResponse{
		DelayID:        event.DelayID,
		RoomID:         event.RoomID,
		Type:           event.Type,
		StateKey:       event.StateKey,
		DelayMS:        event.DelayMS,
		DelayedSinceTS: event.DelayedSince,
		Content:        event.Content,
	}
	if event.Status == userapi.DelayedEventFinalized {
		res.Finalized = &delayedEventFinalization{
			Error:       event.Error,
			EventID:     event.EventID,
			FinalizedTS: event.FinalizedTS,
		}
	}
	return res
}

// Get implements
//
//	GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayID}
func (d *DelayedEvents) Get(req *http.Request, device *userapi.Device, delayID string) util.JSONResponse {
	if r := d.mgmtLimits.Limit(req, device); r != nil {
		return *r
	}
	event, err := d.userAPI.QueryDelayedEvent(req.Context(), delayID)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryDelayedEvent failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	// Another user's delayed event is indistinguishable from a missing one.
	if event == nil || fmt.Sprintf("@%s:%s", event.Localpart, event.ServerName) != device.UserID {
		return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Delayed event not found")}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: newDelayedEventResponse(event)}
}

// List implements
//
//	GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events
//
// It returns the user's scheduled events, or their finalized ones if the
// status parameter asks for them, optionally only those with the given
// delay_id parameters.
// Everything fits in one response, so from is ignored and there is no
// next_batch.
func (d *DelayedEvents) List(req *http.Request, device *userapi.Device) util.JSONResponse {
	if r := d.mgmtLimits.Limit(req, device); r != nil {
		return *r
	}
	query := req.URL.Query()
	var finalized bool
	switch query.Get("status") {
	case "", string(userapi.DelayedEventScheduled):
	case statusFinalised:
		finalized = true
	default:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("status must be 'scheduled' or '" + statusFinalised + "'"),
		}
	}
	localpart, serverName, err := gomatrixserverlib.SplitID('@', device.UserID)
	if err != nil {
		return util.ErrorResponse(err)
	}
	events, err := d.userAPI.QueryDelayedEvents(req.Context(), localpart, serverName, finalized)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryDelayedEvents failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	delayIDs := query["delay_id"]
	entries := make([]delayedEventListEntry, 0, len(events))
	for i := range events {
		if len(delayIDs) > 0 && !slices.Contains(delayIDs, events[i].DelayID) {
			continue
		}
		entries = append(entries, delayedEventListEntry{
			delayedEventResponse: newDelayedEventResponse(&events[i]),
			Delay:                events[i].DelayMS,
			RunningSince:         events[i].DelayedSince,
		})
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct {
			DelayedEvents []delayedEventListEntry `json:"delayed_events"`
		}{entries},
	}
}

// sendEvent sends a delayed event as its owner would send it now: built
// from the current room state and authorized against it, through the same
// path as a client's send. The roomserver transaction is keyed by the delay
// ID, so sending again after a crash yields the event that was sent before.
func (d *DelayedEvents) sendEvent(ctx context.Context, event *userapi.DelayedEvent) (string, *util.JSONResponse) {
	device := &userapi.Device{
		UserID: fmt.Sprintf("@%s:%s", event.Localpart, event.ServerName),
		ID:     event.DeviceID,
	}
	roomVersion, err := d.rsAPI.QueryRoomVersionForRoom(ctx, event.RoomID)
	if err != nil {
		return "", &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.UnsupportedRoomVersion(err.Error()),
		}
	}
	stateKey, resErr := toRoomStateKey(ctx, d.rsAPI, roomVersion, event.RoomID, event.StateKey)
	if resErr != nil {
		return "", resErr
	}
	var content map[string]any
	if resErr = clientutil.UnmarshalJSON(event.Content, &content); resErr != nil {
		return "", resErr
	}
	evTime := d.now()
	if event.OriginServerTS != 0 {
		evTime = event.OriginServerTS.Time()
	}

	var sticky *gomatrixserverlib.StickyEventData
	if event.StickyDurationMS != nil {
		sticky = &gomatrixserverlib.StickyEventData{DurationMillis: *event.StickyDurationMS}
	}

	unlock, resErr := lockUserRoomSend(event.RoomID, device.UserID)
	if resErr != nil {
		return "", resErr
	}
	defer unlock()
	return sendEventContent(
		ctx, device, roomVersion, event.RoomID, event.Type, stateKey, sticky, content, evTime,
		&api.TransactionID{SessionID: api.DelayedEventSessionID, TransactionID: event.DelayID},
		d.cfg, d.rsAPI, d.federation,
	)
}

// finalize records the outcome of a claimed event.
func (d *DelayedEvents) finalize(ctx context.Context, event *userapi.DelayedEvent, eventID string, resErr *util.JSONResponse) {
	var sendErr json.RawMessage
	if resErr != nil {
		sendErr = errorResponseJSON(resErr)
	}
	if err := d.userAPI.PerformDelayedEventFinalize(
		context.WithoutCancel(ctx), event.DelayID, eventID, sendErr, spec.AsTimestamp(d.now()),
	); err != nil {
		util.GetLogger(ctx).WithError(err).WithField("room_id", event.RoomID).Error("failed to finalize delayed event")
	}
}

// errorResponseJSON returns a failed send's standard error response.
func errorResponseJSON(res *util.JSONResponse) json.RawMessage {
	body, err := json.Marshal(res.JSON)
	var standard struct {
		ErrCode string `json:"errcode"`
	}
	if err == nil && json.Unmarshal(body, &standard) == nil && standard.ErrCode != "" {
		return body
	}
	return json.RawMessage(fmt.Sprintf(`{"errcode":%q,"error":"Sending failed with HTTP status %d"}`, spec.ErrorUnknown, res.Code))
}

// Start runs the scheduler until ctx is done.
func (d *DelayedEvents) Start(ctx context.Context) {
	process.Go(ctx, func() { d.run(ctx) })
}

// notify asks the scheduler to look for the next send time again.
func (d *DelayedEvents) notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// run sends delayed events when they are due. It keeps a single timer for
// the earliest send time, which it reads from storage whenever it wakes, so
// the schedule survives restarts and changes made by management requests.
// Its first pass also resends the events that a previous process claimed but
// did not finalize, such as one that stopped while sending.
func (d *DelayedEvents) run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	reclaim := true
	var lastPurge time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-d.wake:
		}
		wait := delayedEventsRetry
		if err := d.sendDue(ctx, reclaim); err != nil {
			logrus.WithError(err).Error("Failed to send due delayed events")
		} else {
			reclaim = false
			if d.now().Sub(lastPurge) >= delayedEventsPurgeInterval {
				before := spec.AsTimestamp(d.now().Add(-userapi.DelayedEventRetention))
				if err = d.userAPI.PerformDelayedEventsPurge(ctx, before); err != nil {
					logrus.WithError(err).Error("Failed to purge finalized delayed events")
				} else {
					lastPurge = d.now()
				}
			}
			if wait, err = d.untilNext(ctx, lastPurge); err != nil {
				logrus.WithError(err).Error("Failed to find the next delayed event")
				wait = delayedEventsRetry
			}
		}
		timer.Reset(wait)
	}
}

// untilNext returns how long to wait for the next delayed event or purge.
func (d *DelayedEvents) untilNext(ctx context.Context, lastPurge time.Time) (time.Duration, error) {
	wait := lastPurge.Add(delayedEventsPurgeInterval).Sub(d.now())
	next, ok, err := d.userAPI.QueryNextDelayedEventSendTS(ctx)
	if err != nil {
		return 0, err
	}
	if ok {
		wait = min(wait, next.Time().Sub(d.now()))
	}
	return max(wait, 0), nil
}

type stateTuple struct {
	roomID, eventType, stateKey string
}

// sendDue claims and sends every event that is due. When several are due
// for the same piece of state, only the first is sent and the rest are
// canceled, as in Synapse, since each would overwrite the one before.
func (d *DelayedEvents) sendDue(ctx context.Context, reclaim bool) error {
	events, err := d.userAPI.PerformDelayedEventsClaimDue(ctx, spec.AsTimestamp(d.now()), reclaim)
	if err != nil {
		return err
	}
	sentState := map[stateTuple]bool{}
	for i := range events {
		event := &events[i]
		var tuple stateTuple
		if event.StateKey != nil {
			tuple = stateTuple{event.RoomID, event.Type, *event.StateKey}
			if sentState[tuple] {
				d.finalize(ctx, event, "", nil)
				continue
			}
		}
		eventID, resErr := d.sendEvent(ctx, event)
		if resErr != nil && ctx.Err() != nil {
			// Shutting down is not the event's failure: leave it claimed for
			// the next process to send.
			return nil
		}
		if resErr != nil {
			logrus.WithFields(logrus.Fields{
				"room_id":    event.RoomID,
				"event_type": event.Type,
				"status":     resErr.Code,
			}).Warn("Failed to send delayed event")
		} else if event.StateKey != nil {
			sentState[tuple] = true
		}
		d.finalize(ctx, event, eventID, resErr)
	}
	return nil
}
