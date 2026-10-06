// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/api"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

var (
	cacheMu          sync.Mutex
	publicRoomsCache []roomserverAPI.PublicRoom
)

type PublicRoomReq struct {
	Since              string `json:"since,omitempty"`
	Limit              int64  `json:"limit,omitempty"`
	Filter             filter `json:"filter,omitempty"`
	Server             string `json:"server,omitempty"`
	IncludeAllNetworks bool   `json:"include_all_networks,omitempty"`
	NetworkID          string `json:"third_party_instance_id,omitempty"`
}

type filter struct {
	SearchTerms string    `json:"generic_search_term,omitempty"`
	RoomTypes   []*string `json:"room_types,omitzero"`
}

type publicRoomsResponse struct {
	Chunk                  []roomserverAPI.PublicRoom `json:"chunk"`
	NextBatch              string                     `json:"next_batch,omitempty"`
	PrevBatch              string                     `json:"prev_batch,omitempty"`
	TotalRoomCountEstimate int                        `json:"total_room_count_estimate,omitempty"`
}

// remotePublicRooms uses the signed transport because GetPublicRoomsFiltered
// cannot represent room-type filters.
func remotePublicRooms(ctx context.Context, request PublicRoomReq, federation fclient.FederationClient, cfg *config.ClientAPI) (*publicRoomsResponse, error) {
	destination := spec.ServerName(request.Server)
	request.Server = ""
	fedReq := fclient.NewFederationRequest(http.MethodPost, cfg.Matrix.ServerName, destination, "/_matrix/federation/v1/publicRooms")
	if err := fedReq.SetContent(request); err != nil {
		return nil, err
	}
	if err := fedReq.Sign(cfg.Matrix.ServerName, cfg.Matrix.KeyID, cfg.Matrix.PrivateKey); err != nil {
		return nil, err
	}
	httpReq, err := fedReq.HTTPRequest()
	if err != nil {
		return nil, err
	}
	var response publicRoomsResponse
	if err := federation.DoRequestAndParseResponse(ctx, httpReq, &response); err != nil {
		return nil, err
	}
	// Servers that predate MSC3827 ignore the room type filter.
	response.Chunk = roomserverAPI.FilterPublicRooms(response.Chunk, "", request.Filter.RoomTypes)
	return &response, nil
}

// GetPostPublicRooms implements GET and POST /publicRooms.
func GetPostPublicRooms(
	req *http.Request, rsAPI roomserverAPI.ClientRoomserverAPI,
	extRoomsProvider api.ExtraPublicRoomsProvider,
	federation fclient.FederationClient,
	cfg *config.ClientAPI,
) util.JSONResponse {
	var request PublicRoomReq
	if fillErr := fillPublicRoomsReq(req, &request); fillErr != nil {
		return *fillErr
	}

	if request.IncludeAllNetworks && request.NetworkID != "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("include_all_networks and third_party_instance_id can not be used together"),
		}
	}

	serverName := spec.ServerName(request.Server)
	if serverName != "" && !cfg.Matrix.IsLocalServerName(serverName) {
		res, err := remotePublicRooms(req.Context(), request, federation, cfg)
		if err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("failed to get public rooms")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: res,
		}
	}

	// Local pagination tokens carry a T prefix; remote tokens remain opaque.
	request.Since = strings.TrimPrefix(request.Since, "T")
	response, err := publicRooms(req.Context(), request, rsAPI, extRoomsProvider)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Errorf("failed to work out public rooms")
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

func publicRooms(
	ctx context.Context, request PublicRoomReq, rsAPI roomserverAPI.ClientRoomserverAPI, extRoomsProvider api.ExtraPublicRoomsProvider,
) (*publicRoomsResponse, error) {
	response := publicRoomsResponse{
		Chunk: []roomserverAPI.PublicRoom{},
	}
	var limit int64
	var offset int64
	limit = request.Limit
	if limit == 0 {
		limit = 50
	}
	offset, err := strconv.ParseInt(request.Since, 10, 64)
	// ParseInt returns 0 and an error when trying to parse an empty string
	// In that case, we want to assign 0 so we ignore the error
	if err != nil && len(request.Since) > 0 {
		util.GetLogger(ctx).WithError(err).Error("strconv.ParseInt failed")
		return nil, err
	}
	err = nil

	var rooms []roomserverAPI.PublicRoom
	if request.Since == "" {
		rooms = refreshPublicRoomCache(ctx, rsAPI, extRoomsProvider, request)
	} else {
		rooms = getPublicRoomsFromCache()
	}

	rooms = roomserverAPI.FilterPublicRooms(rooms, request.Filter.SearchTerms, request.Filter.RoomTypes)
	response.TotalRoomCountEstimate = len(rooms)

	chunk, prev, next := sliceInto(rooms, offset, limit)
	if prev >= 0 {
		response.PrevBatch = "T" + strconv.Itoa(prev)
	}
	if next >= 0 {
		response.NextBatch = "T" + strconv.Itoa(next)
	}
	if chunk != nil {
		response.Chunk = chunk
	}
	return &response, err
}

// fillPublicRoomsReq fills the Limit, Since and Filter attributes of a GET or POST request
// on /publicRooms by parsing the incoming HTTP request
// Filter is only filled for POST requests.
func fillPublicRoomsReq(httpReq *http.Request, request *PublicRoomReq) *util.JSONResponse {
	if httpReq.Method != "GET" && httpReq.Method != "POST" {
		return &util.JSONResponse{
			Code: http.StatusMethodNotAllowed,
			JSON: spec.NotFound("Bad method"),
		}
	}
	if httpReq.Method == "GET" {
		limit, err := strconv.Atoi(httpReq.FormValue("limit"))
		// Atoi returns 0 and an error when trying to parse an empty string
		// In that case, we want to assign 0 so we ignore the error
		if err != nil && len(httpReq.FormValue("limit")) > 0 {
			util.GetLogger(httpReq.Context()).WithError(err).Error("strconv.Atoi failed")
			return &util.JSONResponse{
				Code: 400, //nolint:mnd
				JSON: spec.BadJSON("limit param is not a number"),
			}
		}
		request.Limit = int64(limit)
		request.Since = httpReq.FormValue("since")
		request.Server = httpReq.FormValue("server")
	} else {
		resErr := httputil.UnmarshalJSONRequest(httpReq, request)
		if resErr != nil {
			return resErr
		}
		request.Server = httpReq.FormValue("server")
	}

	return nil
}

// sliceInto returns a subslice of `slice` which honors the since/limit values given.
//
//	  0  1  2  3  4  5  6   index
//	 [A, B, C, D, E, F, G]  slice
//
//	 limit=3          => A,B,C (prev='', next='3')
//	 limit=3&since=3  => D,E,F (prev='0', next='6')
//	 limit=3&since=6  => G     (prev='3', next='')
//
//	A value of '-1' for prev/next indicates no position.
func sliceInto(slice []roomserverAPI.PublicRoom, since int64, limit int64) (subset []roomserverAPI.PublicRoom, prev, next int) {
	prev = -1
	next = -1

	if since > 0 {
		prev = int(since) - int(limit)
	}
	nextIndex := int(since) + int(limit)
	if len(slice) > nextIndex { // there are more rooms ahead of us
		next = nextIndex
	}

	// apply sanity caps
	if since < 0 {
		since = 0
	}
	if nextIndex > len(slice) {
		nextIndex = len(slice)
	}
	if since > int64(len(slice)) {
		since = int64(len(slice))
	}

	subset = slice[since:nextIndex]
	return
}

func refreshPublicRoomCache(
	ctx context.Context, rsAPI roomserverAPI.ClientRoomserverAPI, extRoomsProvider api.ExtraPublicRoomsProvider,
	request PublicRoomReq,
) []roomserverAPI.PublicRoom {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	var extraRooms []roomserverAPI.PublicRoom
	if extRoomsProvider != nil {
		for _, room := range extRoomsProvider.Rooms() {
			pub := roomserverAPI.PublicRoom{PublicRoom: room}
			if room.RoomType != "" {
				pub.RoomType = &room.RoomType
			}
			extraRooms = append(extraRooms, pub)
		}
	}

	// TODO: this is only here to make Sytest happy, for now.
	ns := strings.Split(request.NetworkID, "|")
	if len(ns) == 2 { //nolint:mnd
		request.NetworkID = ns[1]
	}

	var queryRes roomserverAPI.QueryPublishedRoomsResponse
	err := rsAPI.QueryPublishedRooms(ctx, &roomserverAPI.QueryPublishedRoomsRequest{
		NetworkID:          request.NetworkID,
		IncludeAllNetworks: request.IncludeAllNetworks,
	}, &queryRes)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryPublishedRooms failed")
		return publicRoomsCache
	}
	pubRooms, err := roomserverAPI.PopulatePublicRooms(ctx, queryRes.RoomIDs, rsAPI)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("PopulatePublicRooms failed")
		return publicRoomsCache
	}
	publicRoomsCache = []roomserverAPI.PublicRoom{}
	publicRoomsCache = append(publicRoomsCache, pubRooms...)
	publicRoomsCache = append(publicRoomsCache, extraRooms...)
	publicRoomsCache = dedupeAndShuffle(publicRoomsCache)

	// sort by total joined member count (big to small)
	sort.SliceStable(publicRoomsCache, func(i, j int) bool {
		return publicRoomsCache[i].JoinedMembersCount > publicRoomsCache[j].JoinedMembersCount
	})
	return publicRoomsCache
}

func getPublicRoomsFromCache() []roomserverAPI.PublicRoom {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	return publicRoomsCache
}

func dedupeAndShuffle(in []roomserverAPI.PublicRoom) []roomserverAPI.PublicRoom {
	// de-duplicate rooms with the same room ID. We can join the room via any of these aliases as we know these servers
	// are alive and well, so we arbitrarily pick one (purposefully shuffling them to spread the load a bit)
	var publicRooms []roomserverAPI.PublicRoom
	haveRoomIDs := make(map[string]bool)
	rand.Shuffle(len(in), func(i, j int) {
		in[i], in[j] = in[j], in[i]
	})
	for _, r := range in {
		if haveRoomIDs[r.RoomID] {
			continue
		}
		haveRoomIDs[r.RoomID] = true
		publicRooms = append(publicRooms, r)
	}
	return publicRooms
}
