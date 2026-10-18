package routing

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

type PublicRoomReq struct {
	Since              string `json:"since,omitempty"`
	Limit              int16  `json:"limit,omitempty"`
	Filter             filter `json:"filter,omitempty"`
	IncludeAllNetworks bool   `json:"include_all_networks,omitempty"`
	NetworkID          string `json:"third_party_instance_id,omitempty"`
}

type filter struct {
	SearchTerms string `json:"generic_search_term,omitempty"`
	// RoomTypes filters by room type (MSC3827); a null entry matches rooms
	// without a type.
	RoomTypes []*string `json:"room_types,omitzero"`
}

// GetPostPublicRooms implements GET and POST /publicRooms.
func GetPostPublicRooms(req *http.Request, rsAPI roomserverAPI.FederationRoomserverAPI) util.JSONResponse {
	var request PublicRoomReq
	if fillErr := fillPublicRoomsReq(req, &request); fillErr != nil {
		return *fillErr
	}
	if request.Limit == 0 {
		request.Limit = 50
	}
	response, err := publicRooms(req.Context(), request, rsAPI)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}

// publicRoomsResponse is fclient.RespPublicRooms with room types (MSC3827).
type publicRoomsResponse struct {
	Chunk                  []roomserverAPI.PublicRoom `json:"chunk"`
	NextBatch              string                     `json:"next_batch,omitempty"`
	PrevBatch              string                     `json:"prev_batch,omitempty"`
	TotalRoomCountEstimate int                        `json:"total_room_count_estimate,omitempty"`
}

func publicRooms(
	ctx context.Context, request PublicRoomReq, rsAPI roomserverAPI.FederationRoomserverAPI,
) (*publicRoomsResponse, error) {
	response := publicRoomsResponse{Chunk: []roomserverAPI.PublicRoom{}}
	offset, err := strconv.ParseInt(request.Since, 10, 64)
	// ParseInt returns 0 and an error when trying to parse an empty string
	// In that case, we want to assign 0 so we ignore the error
	if err != nil && len(request.Since) > 0 {
		util.GetLogger(ctx).WithError(err).Error("strconv.ParseInt failed")
		return nil, err
	}

	if request.IncludeAllNetworks && request.NetworkID != "" {
		return nil, fmt.Errorf("include_all_networks and third_party_instance_id can not be used together")
	}

	var queryRes roomserverAPI.QueryPublishedRoomsResponse
	err = rsAPI.QueryPublishedRooms(ctx, &roomserverAPI.QueryPublishedRoomsRequest{
		NetworkID: request.NetworkID,
	}, &queryRes)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryPublishedRooms failed")
		return nil, err
	}
	rooms, err := roomserverAPI.PopulatePublicRooms(ctx, queryRes.RoomIDs, rsAPI)
	if err != nil {
		return nil, err
	}
	// Largest rooms first, as on the client API, with a stable order so that
	// pagination tokens stay valid between requests.
	sort.Slice(rooms, func(i, j int) bool {
		if rooms[i].JoinedMembersCount != rooms[j].JoinedMembersCount {
			return rooms[i].JoinedMembersCount > rooms[j].JoinedMembersCount
		}
		return rooms[i].RoomID < rooms[j].RoomID
	})
	rooms = roomserverAPI.FilterPublicRooms(rooms, request.Filter.SearchTerms, request.Filter.RoomTypes)
	response.TotalRoomCountEstimate = len(rooms)

	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(rooms)) {
		offset = int64(len(rooms))
	}
	if offset > 0 {
		response.PrevBatch = strconv.FormatInt(max(offset-int64(request.Limit), 0), 10)
	}
	nextIndex := int(offset) + int(request.Limit)
	if len(rooms) > nextIndex {
		response.NextBatch = strconv.Itoa(nextIndex)
	} else {
		nextIndex = len(rooms)
	}
	response.Chunk = append(response.Chunk, rooms[offset:nextIndex]...)
	return &response, nil
}

// fillPublicRoomsReq fills the Limit, Since and Filter attributes of a GET or POST request
// on /publicRooms by parsing the incoming HTTP request
// Filter is only filled for POST requests.
func fillPublicRoomsReq(httpReq *http.Request, request *PublicRoomReq) *util.JSONResponse {
	switch httpReq.Method {
	case http.MethodGet:
		limit, err := strconv.Atoi(httpReq.FormValue("limit"))
		// Atoi returns 0 and an error when trying to parse an empty string
		// In that case, we want to assign 0 so we ignore the error
		if err != nil && len(httpReq.FormValue("limit")) > 0 {
			util.GetLogger(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
			return &util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		request.Limit = int16(limit)
		request.Since = httpReq.FormValue("since")
		return nil
	case http.MethodPost:
		return httputil.UnmarshalJSONRequest(httpReq, request)
	}

	return &util.JSONResponse{
		Code: http.StatusMethodNotAllowed,
		JSON: spec.NotFound("Bad method"),
	}
}
