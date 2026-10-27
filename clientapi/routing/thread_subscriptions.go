package routing

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	roomserver "codefloe.com/pat-s/zendrite/roomserver/api"
	roomtypes "codefloe.com/pat-s/zendrite/roomserver/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	threadSubscriptionsAccountDataType = "io.element.msc4306.thread_subscriptions"
	maxThreadSubscriptionRetries       = 10
)

type threadSubscription struct {
	Automatic  bool   `json:"automatic"`
	BumpStamp  int64  `json:"bump_stamp"`
	BumpOrder  int64  `json:"bump_order,omitempty"`
	CauseEvent string `json:"-"`
}

type threadSubscriptionsData struct {
	Subscriptions     map[string]threadSubscription `json:"subscriptions"`
	Unsubscribed      map[string]int64              `json:"unsubscribed,omitempty"`
	UnsubscribedOrder map[string]int64              `json:"unsubscribed_order,omitempty"`
}

// normalize initializes missing subscription and unsubscription maps for updates.
func (d *threadSubscriptionsData) normalize() {
	if d.Subscriptions == nil {
		d.Subscriptions = make(map[string]threadSubscription)
	}
	if d.Unsubscribed == nil {
		d.Unsubscribed = make(map[string]int64)
	}
	if d.UnsubscribedOrder == nil {
		d.UnsubscribedOrder = make(map[string]int64)
	}
}

// automaticSubscriptionConflicts reports whether a manual unsubscription
// is at or after the event that caused an automatic subscription. Positive local
// orderings take precedence when both are available; otherwise positive timestamps
// in Unix milliseconds are compared. Missing values do not establish a conflict.
func automaticSubscriptionConflicts(data threadSubscriptionsData, threadID string, bumpOrder, bumpStamp int64) bool {
	if unsubscribedOrder := data.UnsubscribedOrder[threadID]; unsubscribedOrder > 0 && bumpOrder > 0 {
		return bumpOrder <= unsubscribedOrder
	}
	unsubscribedAt := data.Unsubscribed[threadID]
	return unsubscribedAt > 0 && bumpStamp > 0 && bumpStamp <= unsubscribedAt
}

// queryThreadSubscriptions returns the user's room subscriptions and the stored
// JSON for a conditional update. Missing account data yields initialized empty
// maps and nil JSON. Account-data query and JSON decoding errors are returned.
func queryThreadSubscriptions(req *http.Request, userID, roomID string, userAPI userapi.ClientUserAPI) (threadSubscriptionsData, json.RawMessage, error) {
	var response userapi.QueryAccountDataResponse
	err := userAPI.QueryAccountData(req.Context(), &userapi.QueryAccountDataRequest{
		UserID: userID, RoomID: roomID, DataType: threadSubscriptionsAccountDataType,
	}, &response)
	if err != nil {
		return threadSubscriptionsData{}, nil, err
	}
	data := threadSubscriptionsData{}
	var raw json.RawMessage
	if stored := response.RoomAccountData[roomID][threadSubscriptionsAccountDataType]; len(stored) > 0 {
		raw = stored
		if err = json.Unmarshal(stored, &data); err != nil {
			return threadSubscriptionsData{}, nil, err
		}
	}
	data.normalize()
	return data, raw, nil
}

// saveThreadSubscriptionsIfUnchanged writes data only if the stored document
// still matches expected, so a concurrent change to another thread is not lost.
// An empty expected value represents a missing document. A mismatch returns
// false without an error; account-data update errors propagate.
func saveThreadSubscriptionsIfUnchanged(req *http.Request, userID, roomID string, expected []byte, data threadSubscriptionsData, userAPI userapi.ClientUserAPI) (bool, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return false, err
	}
	var response userapi.InputAccountDataResponse
	if err = userAPI.InputAccountData(req.Context(), &userapi.InputAccountDataRequest{
		UserID: userID, RoomID: roomID, DataType: threadSubscriptionsAccountDataType,
		AccountData: raw, CompareAccountData: true, ExpectedAccountData: expected,
	}, &response); err != nil {
		return false, err
	}
	return response.Updated, nil
}

// threadSubscriptionLocks serializes the read-modify-write of each user's
// per-room subscription document, so concurrent changes to different threads
// are not lost.
var threadSubscriptionLocks = struct {
	sync.Mutex
	locks map[string]*sync.Mutex
}{locks: make(map[string]*sync.Mutex)}

func lockThreadSubscriptions(userID, roomID string) func() {
	key := userID + "\x00" + roomID
	threadSubscriptionLocks.Lock()
	mu := threadSubscriptionLocks.locks[key]
	if mu == nil {
		mu = &sync.Mutex{}
		threadSubscriptionLocks.locks[key] = mu
	}
	threadSubscriptionLocks.Unlock()

	mu.Lock()
	return mu.Unlock
}

// getThreadEvent returns the event if it exists in the room and the user may
// see it, together with its local event ordering (zero if unavailable). Missing
// or inaccessible events and query failures return nil and zero.
func getThreadEvent(req *http.Request, userID, roomID, eventID string, rsAPI roomserver.ClientRoomserverAPI) (*roomtypes.HeaderedEvent, int64) {
	var response roomserver.QueryEventsByIDResponse
	if err := rsAPI.QueryEventsByID(req.Context(), &roomserver.QueryEventsByIDRequest{
		RoomID: roomID, EventIDs: []string{eventID},
	}, &response); err != nil {
		return nil, 0
	}
	for _, event := range response.Events {
		if event == nil || event.EventID() != eventID || event.RoomID().String() != roomID {
			continue
		}
		if !canSeeThreadEvent(req, userID, event, rsAPI) {
			return nil, 0
		}
		order := int64(response.EventNIDs[eventID])
		if order == 0 {
			order = int64(event.EventNID)
		}
		return event, order
	}
	return nil, 0
}

// canSeeThreadEvent applies the room's history visibility at the event.
func canSeeThreadEvent(req *http.Request, userID string, event *roomtypes.HeaderedEvent, rsAPI roomserver.ClientRoomserverAPI) bool {
	ctx := req.Context()
	user, err := spec.NewUserID(userID, true)
	if err != nil {
		return false
	}
	var current roomserver.QueryMembershipForUserResponse
	if err = rsAPI.QueryMembershipForUser(ctx, &roomserver.QueryMembershipForUserRequest{
		RoomID: event.RoomID().String(), UserID: *user,
	}, &current); err != nil {
		return false
	}
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, event.RoomID(), *user)
	if err != nil || senderID == nil {
		return false
	}
	var state roomserver.QueryStateAfterEventsResponse
	if err = rsAPI.QueryStateAfterEvents(ctx, &roomserver.QueryStateAfterEventsRequest{
		RoomID:       event.RoomID().String(),
		PrevEventIDs: []string{event.EventID()},
		StateToFetch: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomHistoryVisibility, StateKey: ""},
			{EventType: spec.MRoomMember, StateKey: string(*senderID)},
		},
	}, &state); err != nil || !state.PrevEventsExist {
		return false
	}
	visibility, membership := gomatrixserverlib.HistoryVisibilityShared, ""
	for _, ev := range state.StateEvents {
		switch ev.Type() {
		case spec.MRoomHistoryVisibility:
			if v, vErr := ev.HistoryVisibility(); vErr == nil {
				visibility = v
			}
		case spec.MRoomMember:
			membership, _ = ev.Membership()
		}
	}
	switch visibility {
	case gomatrixserverlib.HistoryVisibilityWorldReadable:
		return true
	case gomatrixserverlib.HistoryVisibilityShared:
		return current.IsInRoom || membership == spec.Join
	case gomatrixserverlib.HistoryVisibilityInvited:
		return membership == spec.Join || membership == spec.Invite
	default:
		return membership == spec.Join
	}
}

// getThreadSubscription returns the subscription's automatic flag and bump
// stamp in Unix milliseconds. Missing or inaccessible thread roots and absent
// subscriptions return 404; account-data query or decoding failures return an
// internal error response.
func getThreadSubscription(req *http.Request, device *userapi.Device, roomID, threadID string, userAPI userapi.ClientUserAPI, rsAPI roomserver.ClientRoomserverAPI) util.JSONResponse {
	if root, _ := getThreadEvent(req, device.UserID, roomID, threadID, rsAPI); root == nil {
		return util.MatrixErrorResponse(http.StatusNotFound, string(spec.ErrorNotFound), "Unknown thread")
	}
	data, _, err := queryThreadSubscriptions(req, device.UserID, roomID, userAPI)
	if err != nil {
		return util.ErrorResponse(err)
	}
	subscription, ok := data.Subscriptions[threadID]
	if !ok {
		return util.MatrixErrorResponse(http.StatusNotFound, string(spec.ErrorNotFound), "Thread is not subscribed")
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: map[string]any{
		"automatic":  subscription.Automatic,
		"bump_stamp": subscription.BumpStamp,
	}}
}

// putThreadSubscription creates or updates a subscription from body. A nonempty
// "automatic" value identifies a visible reply in the thread that caused the
// subscription; an empty value requests a manual subscription. Automatic requests
// preserve manual subscriptions and return 409 when a manual unsubscription is
// at or after the cause. Bump stamps use Unix milliseconds and increase past
// stored stamps. Unknown roots return 404; invalid JSON or causes return 400.
// Storage failures or ten consecutive conditional-write conflicts return an
// internal error response.
func putThreadSubscription(req *http.Request, device *userapi.Device, roomID, threadID string, body io.Reader, userAPI userapi.ClientUserAPI, rsAPI roomserver.ClientRoomserverAPI) util.JSONResponse {
	root, rootOrder := getThreadEvent(req, device.UserID, roomID, threadID, rsAPI)
	if root == nil {
		return util.MatrixErrorResponse(http.StatusNotFound, string(spec.ErrorNotFound), "Unknown thread")
	}
	var request struct {
		Automatic string `json:"automatic"`
	}
	if err := json.NewDecoder(body).Decode(&request); err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(err.Error())}
	}
	var bumpStamp int64
	var bumpOrder int64
	if request.Automatic != "" {
		cause, causeOrder := getThreadEvent(req, device.UserID, roomID, request.Automatic, rsAPI)
		if cause == nil || gjson.GetBytes(cause.Content(), `m\.relates_to.rel_type`).Str != "m.thread" || gjson.GetBytes(cause.Content(), `m\.relates_to.event_id`).Str != threadID {
			return util.MatrixErrorResponse(http.StatusBadRequest, "IO.ELEMENT.MSC4306.M_NOT_IN_THREAD", "Automatic subscription cause event is not in this thread")
		}
		bumpStamp = cause.OriginServerTS().Time().UnixMilli()
		bumpOrder = causeOrder
	} else {
		bumpStamp = root.OriginServerTS().Time().UnixMilli()
		bumpOrder = rootOrder
		if roomOrdering, orderingErr := maxRoomOrdering(req, roomID, rsAPI); orderingErr == nil && roomOrdering > bumpOrder {
			bumpOrder = roomOrdering
		}
	}
	defer lockThreadSubscriptions(device.UserID, roomID)()
	for attempt := 0; attempt < maxThreadSubscriptionRetries; attempt++ {
		data, expected, err := queryThreadSubscriptions(req, device.UserID, roomID, userAPI)
		if err != nil {
			return util.ErrorResponse(err)
		}
		existing, subscribed := data.Subscriptions[threadID]
		if request.Automatic != "" && subscribed && !existing.Automatic {
			// An automatic subscription never downgrades a manual one.
			return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
		}
		if request.Automatic != "" && automaticSubscriptionConflicts(data, threadID, bumpOrder, bumpStamp) {
			return util.MatrixErrorResponse(http.StatusConflict, "IO.ELEMENT.MSC4306.M_CONFLICTING_UNSUBSCRIPTION", "Thread was unsubscribed after the cause event")
		}
		// Every change gets a later stamp than the subscription or unsubscribe it replaces.
		floor := data.Unsubscribed[threadID]
		if subscribed && existing.BumpStamp > floor {
			floor = existing.BumpStamp
		}
		updatedStamp := bumpStamp
		if updatedStamp <= floor {
			updatedStamp = floor + 1
		}
		data.Subscriptions[threadID] = threadSubscription{Automatic: request.Automatic != "", BumpStamp: updatedStamp, BumpOrder: bumpOrder, CauseEvent: request.Automatic}
		delete(data.Unsubscribed, threadID)
		delete(data.UnsubscribedOrder, threadID)
		updated, err := saveThreadSubscriptionsIfUnchanged(req, device.UserID, roomID, expected, data, userAPI)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if updated {
			return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
		}
	}
	return util.ErrorResponse(errors.New("thread subscription update conflicted with concurrent changes"))
}

// maxRoomOrdering returns the greatest locally assigned event ordering among
// the room's latest events. Unlike depth, it cannot be chosen by a remote
// sender. It is scoped to this database's event sequence, so account data and
// the roomserver database must be restored together. It returns zero when no
// positive ordering is available and propagates roomserver query errors.
func maxRoomOrdering(req *http.Request, roomID string, rsAPI roomserver.ClientRoomserverAPI) (int64, error) {
	var latest roomserver.QueryLatestEventsAndStateResponse
	if err := rsAPI.QueryLatestEventsAndState(req.Context(), &roomserver.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
		// Avoid fetching the entire current state just to learn the ordering.
		StateToFetch: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomCreate, StateKey: ""}},
	}, &latest); err != nil {
		return 0, err
	}
	if len(latest.LatestEvents) == 0 {
		return 0, nil
	}
	var events roomserver.QueryEventsByIDResponse
	if err := rsAPI.QueryEventsByID(req.Context(), &roomserver.QueryEventsByIDRequest{
		RoomID:   roomID,
		EventIDs: latest.LatestEvents,
	}, &events); err != nil {
		return 0, err
	}
	var maxOrdering int64
	for _, event := range events.Events {
		ordering := int64(events.EventNIDs[event.EventID()])
		if ordering == 0 {
			ordering = int64(event.EventNID)
		}
		if ordering > maxOrdering {
			maxOrdering = ordering
		}
	}
	return maxOrdering, nil
}

// deleteThreadSubscription removes a subscription, even if absent, and records
// a manual unsubscription's local room ordering and increasing Unix millisecond
// stamp. It returns 200 on success. Roomserver or account-data failures and ten
// consecutive conditional-write conflicts return an internal error response.
func deleteThreadSubscription(req *http.Request, device *userapi.Device, roomID, threadID string, userAPI userapi.ClientUserAPI, rsAPI roomserver.ClientRoomserverAPI) util.JSONResponse {
	defer lockThreadSubscriptions(device.UserID, roomID)()
	roomOrdering, err := maxRoomOrdering(req, roomID, rsAPI)
	if err != nil {
		return util.ErrorResponse(err)
	}
	for attempt := 0; attempt < maxThreadSubscriptionRetries; attempt++ {
		data, expected, err := queryThreadSubscriptions(req, device.UserID, roomID, userAPI)
		if err != nil {
			return util.ErrorResponse(err)
		}
		existing, subscribed := data.Subscriptions[threadID]
		delete(data.Subscriptions, threadID)
		unsubscribedAt := time.Now().UnixMilli()
		if subscribed && unsubscribedAt <= existing.BumpStamp {
			unsubscribedAt = existing.BumpStamp + 1
		}
		if previous := data.Unsubscribed[threadID]; previous >= unsubscribedAt {
			unsubscribedAt = previous + 1
		}
		data.Unsubscribed[threadID] = unsubscribedAt
		data.UnsubscribedOrder[threadID] = roomOrdering
		updated, err := saveThreadSubscriptionsIfUnchanged(req, device.UserID, roomID, expected, data, userAPI)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if updated {
			return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
		}
	}
	return util.ErrorResponse(errors.New("thread subscription update conflicted with concurrent changes"))
}
