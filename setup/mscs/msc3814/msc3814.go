// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package msc3814 implements dehydrated devices v2
// https://github.com/matrix-org/matrix-spec-proposals/pull/3814
package msc3814

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/matrix-org/util"

	clientutil "codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

const mscPath = "/unstable/org.matrix.msc3814.v1"

// Enable registers the MSC3814 endpoints on the unstable client mux.
func Enable(cfg *config.Zendrite, cm *sqlutil.Connections, routers httputil.Routers, userAPI api.ClientUserAPI) error {
	syncDB, _, err := cm.Connection(&cfg.SyncAPI.Database)
	if err != nil {
		return fmt.Errorf("cannot enable MSC3814: failed to connect to syncapi database: %w", err)
	}

	routers.Client.Handle(mscPath+"/dehydrated_device",
		httputil.MakeAuthAPI("msc3814_put_dehydrated_device", userAPI, putDehydratedDevice(userAPI)),
	).Methods(http.MethodPut, http.MethodOptions)

	routers.Client.Handle(mscPath+"/dehydrated_device",
		httputil.MakeAuthAPI("msc3814_get_dehydrated_device", userAPI, getDehydratedDevice(userAPI)),
	).Methods(http.MethodGet, http.MethodOptions)

	routers.Client.Handle(mscPath+"/dehydrated_device",
		httputil.MakeAuthAPI("msc3814_delete_dehydrated_device", userAPI, deleteDehydratedDevice(userAPI)),
	).Methods(http.MethodDelete, http.MethodOptions)

	routers.Client.Handle(mscPath+"/dehydrated_device/{deviceID}/events",
		httputil.MakeAuthAPI("msc3814_get_dehydrated_device_events", userAPI, getDehydratedDeviceEvents(userAPI, syncDB)),
	).Methods(http.MethodPost, http.MethodOptions)

	return nil
}

type putDehydratedDeviceRequest struct {
	DeviceData               json.RawMessage            `json:"device_data"`
	DeviceKeys               *deviceKeysReq             `json:"device_keys"`
	OneTimeKeys              map[string]json.RawMessage `json:"one_time_keys"`
	FallbackKeys             map[string]json.RawMessage `json:"fallback_keys"`
	DeviceID                 string                     `json:"device_id"`
	InitialDeviceDisplayName string                     `json:"initial_device_display_name"`
}

type deviceKeysReq struct {
	Algorithms []string                     `json:"algorithms"`
	Keys       map[string]string            `json:"keys"`
	Signatures map[string]map[string]string `json:"signatures"`
	DeviceID   string                       `json:"device_id"`
	UserID     string                       `json:"user_id"`
}

func putDehydratedDevice(userAPI api.ClientUserAPI) func(*http.Request, *api.Device) util.JSONResponse {
	return func(req *http.Request, device *api.Device) util.JSONResponse {
		var body putDehydratedDeviceRequest
		if resErr := clientutil.UnmarshalJSONRequest(req, &body); resErr != nil {
			return *resErr
		}

		if body.DeviceData == nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec{"errcode": "M_MISSING_PARAM", "error": "device_data is required"},
			}
		}

		// Generate a device ID if one wasn't provided.
		deviceID := body.DeviceID
		if deviceID == "" {
			deviceID = "DEHYDRATED_" + util.RandomString(8) //nolint:mnd
		}

		storeReq := &api.PerformStoreDehydratedDeviceRequest{
			UserID:     device.UserID,
			DeviceID:   deviceID,
			DeviceData: body.DeviceData,
		}

		// Convert device keys if provided.
		if body.DeviceKeys != nil {
			keyJSON, err := json.Marshal(body.DeviceKeys)
			if err != nil {
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to marshal device keys"},
				}
			}
			storeReq.DeviceKeys = &api.DeviceKeys{
				UserID:   device.UserID,
				DeviceID: deviceID,
				KeyJSON:  keyJSON,
			}
		}

		if len(body.OneTimeKeys) > 0 {
			storeReq.OneTimeKeys = &api.OneTimeKeys{
				UserID:   device.UserID,
				DeviceID: deviceID,
				KeyJSON:  body.OneTimeKeys,
			}
		}

		if len(body.FallbackKeys) > 0 {
			storeReq.FallbackKeys = &api.FallbackKeys{
				UserID:   device.UserID,
				DeviceID: deviceID,
				KeyJSON:  body.FallbackKeys,
			}
		}

		var storeRes api.PerformStoreDehydratedDeviceResponse
		if err := userAPI.PerformStoreDehydratedDevice(req.Context(), storeReq, &storeRes); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("PerformStoreDehydratedDevice failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to store dehydrated device"},
			}
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: map[string]string{"device_id": storeRes.DeviceID},
		}
	}
}

func getDehydratedDevice(userAPI api.ClientUserAPI) func(*http.Request, *api.Device) util.JSONResponse {
	return func(req *http.Request, device *api.Device) util.JSONResponse {
		var res api.QueryDehydratedDeviceResponse
		if err := userAPI.QueryDehydratedDevice(req.Context(), &api.QueryDehydratedDeviceRequest{
			UserID: device.UserID,
		}, &res); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("QueryDehydratedDevice failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to query dehydrated device"},
			}
		}

		if !res.Found {
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec{"errcode": "M_NOT_FOUND", "error": "no dehydrated device found"},
			}
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: map[string]any{
				"device_id":   res.DeviceID,
				"device_data": res.DeviceData,
			},
		}
	}
}

func deleteDehydratedDevice(userAPI api.ClientUserAPI) func(*http.Request, *api.Device) util.JSONResponse {
	return func(req *http.Request, device *api.Device) util.JSONResponse {
		var res api.PerformDeleteDehydratedDeviceResponse
		if err := userAPI.PerformDeleteDehydratedDevice(req.Context(), &api.PerformDeleteDehydratedDeviceRequest{
			UserID: device.UserID,
		}, &res); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("PerformDeleteDehydratedDevice failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to delete dehydrated device"},
			}
		}

		if res.DeviceID == "" {
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec{"errcode": "M_NOT_FOUND", "error": "no dehydrated device found"},
			}
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: map[string]string{"device_id": res.DeviceID},
		}
	}
}

type eventsRequest struct {
	NextBatch string `json:"next_batch"`
}

func getDehydratedDeviceEvents(userAPI api.ClientUserAPI, syncDB *sql.DB) func(*http.Request, *api.Device) util.JSONResponse {
	return func(req *http.Request, device *api.Device) util.JSONResponse {
		vars := httputil.Vars(req)
		deviceID := vars["deviceID"]
		if deviceID == "" {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec{"errcode": "M_MISSING_PARAM", "error": "device_id is required"},
			}
		}

		// Verify the user owns this dehydrated device.
		var queryRes api.QueryDehydratedDeviceResponse
		if err := userAPI.QueryDehydratedDevice(req.Context(), &api.QueryDehydratedDeviceRequest{
			UserID: device.UserID,
		}, &queryRes); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("QueryDehydratedDevice failed")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to query dehydrated device"},
			}
		}

		if !queryRes.Found || queryRes.DeviceID != deviceID {
			return util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: spec{"errcode": "M_FORBIDDEN", "error": "the given device is not a dehydrated device owned by you"},
			}
		}

		var body eventsRequest
		if resErr := clientutil.UnmarshalJSONRequest(req, &body); resErr != nil {
			return *resErr
		}

		var from int64
		if body.NextBatch != "" {
			var err error
			from, err = strconv.ParseInt(body.NextBatch, 10, 64)
			if err != nil {
				return util.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec{"errcode": "M_INVALID_PARAM", "error": "invalid next_batch token"},
				}
			}
		}

		// Query send-to-device messages directly from the syncapi database.
		events, nextBatch, err := selectSendToDeviceMessages(req.Context(), syncDB, device.UserID, deviceID, from)
		if err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("Failed to query send-to-device messages")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec{"errcode": "M_UNKNOWN", "error": "failed to query events"},
			}
		}

		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: map[string]any{
				"events":     events,
				"next_batch": strconv.FormatInt(nextBatch, 10),
			},
		}
	}
}

// selectSendToDeviceMessages queries the syncapi_send_to_device table for messages
// addressed to the given user/device with id > from.
func selectSendToDeviceMessages(ctx context.Context, db *sql.DB, userID, deviceID string, from int64) ([]json.RawMessage, int64, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT id, content FROM syncapi_send_to_device WHERE user_id = $1 AND device_id = $2 AND id > $3 ORDER BY id ASC",
		userID, deviceID, from,
	)
	if err != nil {
		return nil, from, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectSendToDeviceMessages: rows.close() failed")

	var events []json.RawMessage
	var lastID int64
	for rows.Next() {
		var id int64
		var content string
		if err = rows.Scan(&id, &content); err != nil {
			return nil, from, err
		}
		events = append(events, json.RawMessage(content))
		if id > lastID {
			lastID = id
		}
	}
	if err = rows.Err(); err != nil {
		return nil, from, err
	}

	if events == nil {
		events = []json.RawMessage{}
	}

	nextBatch := lastID
	if nextBatch == 0 {
		nextBatch = from
	}

	return events, nextBatch, nil
}

// spec is a convenience alias for JSON response bodies.
type spec = map[string]any
