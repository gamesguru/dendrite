package routing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	mutualRoomsPageSize = 100
	mutualRoomsTokenTTL = 15 * time.Minute
)

type mutualRoomsResponse struct {
	Joined    []string `json:"joined"`
	Count     int      `json:"count"`
	NextBatch string   `json:"next_batch,omitempty"`
}

type mutualRoomsCursor struct {
	Caller  string `json:"caller"`
	Target  string `json:"target"`
	After   string `json:"after"`
	Expires int64  `json:"expires"`
}

type mutualRoomsHandler struct {
	rsAPI    api.ClientRoomserverAPI
	key      []byte
	pageSize int
}

// Tokens are stateless and scoped to this API instance; restarting invalidates them.
func newMutualRoomsHandler(rsAPI api.ClientRoomserverAPI) *mutualRoomsHandler {
	return &mutualRoomsHandler{rsAPI: rsAPI, key: []byte(rand.Text()), pageSize: mutualRoomsPageSize}
}

func (h *mutualRoomsHandler) signature(payload string) []byte {
	mac := hmac.New(sha256.New, h.key)
	// Hash writes cannot fail.
	_, _ = mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func (h *mutualRoomsHandler) get(req *http.Request, device *userapi.Device) util.JSONResponse {
	query, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil || len(query["user_id"]) != 1 || len(query["from"]) > 1 {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Invalid mutual rooms query parameters")}
	}
	target, err := spec.NewUserID(query.Get("user_id"), true)
	if err != nil || target.Local() == "" || strings.IndexFunc(target.Local(), func(r rune) bool {
		return r < '!' || r > '~'
	}) >= 0 || target.String() == device.UserID {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("user_id must be a valid user ID other than the caller")}
	}
	caller, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("Failed to query mutual rooms")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}

	var cursor mutualRoomsCursor
	if query.Has("from") {
		payload, signature, ok := strings.Cut(query.Get("from"), ".")
		mac, decodeErr := base64.RawURLEncoding.DecodeString(signature)
		if !ok || decodeErr != nil || !hmac.Equal(mac, h.signature(payload)) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Invalid pagination token")}
		}
		data, decodeErr := base64.RawURLEncoding.DecodeString(payload)
		if decodeErr != nil || json.Unmarshal(data, &cursor) != nil || cursor.Caller != caller.String() || cursor.Target != target.String() || cursor.Expires <= time.Now().Unix() {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Invalid or expired pagination token")}
		}
	}

	// Query current membership on every page. Never trust membership from a token.
	callerRooms, err := h.rsAPI.QueryRoomsForUser(req.Context(), *caller, spec.Join)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("Failed to query mutual rooms")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	targetRooms, err := h.rsAPI.QueryRoomsForUser(req.Context(), *target, spec.Join)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("Failed to query mutual rooms")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	joined := make(map[string]struct{}, len(callerRooms))
	for _, room := range callerRooms {
		joined[room.String()] = struct{}{}
	}
	shared := make([]string, 0)
	for _, room := range targetRooms {
		if _, ok := joined[room.String()]; ok {
			shared = append(shared, room.String())
		}
	}
	sort.Strings(shared)
	response := mutualRoomsResponse{Count: len(shared), Joined: []string{}}
	// Keyset pagination remains usable even when earlier rooms are left.
	start := sort.Search(len(shared), func(i int) bool { return shared[i] > cursor.After })
	end := min(start+h.pageSize, len(shared))
	response.Joined = append(response.Joined, shared[start:end]...)
	if end < len(shared) {
		cursor = mutualRoomsCursor{Caller: caller.String(), Target: target.String(), After: shared[end-1], Expires: time.Now().Add(mutualRoomsTokenTTL).Unix()}
		data, marshalErr := json.Marshal(cursor)
		if marshalErr != nil {
			util.GetLogger(req.Context()).WithError(marshalErr).Error("Failed to encode mutual rooms cursor")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
		payload := base64.RawURLEncoding.EncodeToString(data)
		response.NextBatch = payload + "." + base64.RawURLEncoding.EncodeToString(h.signature(payload))
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: response}
}
