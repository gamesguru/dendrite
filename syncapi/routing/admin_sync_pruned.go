// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const maxAdminSyncPrunedEvents = 100

type syncPrunedResult struct {
	Status     string `json:"status"`
	Output     string `json:"output"`
	State      string `json:"state"`
	OutputHash string `json:"output_json_sha256,omitempty"`
	StateHash  string `json:"state_json_sha256,omitempty"`
}

// AdminSyncPruned updates sync API copies after admin refetch has repaired the
// roomserver copy. Each sync row keeps its own unsigned metadata.
func AdminSyncPruned(req *http.Request, syncDB storage.Database, rsAPI api.SyncRoomserverAPI, _ *userapi.Device) util.JSONResponse {
	var body struct {
		DryRun   bool     `json:"dry_run"`
		EventIDs []string `json:"event_ids"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("invalid JSON request: " + err.Error())}
	}
	if len(body.EventIDs) == 0 || len(body.EventIDs) > maxAdminSyncPrunedEvents {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Expecting an event_ids array of 1-100 event IDs.")}
	}

	results := make(map[string]syncPrunedResult, len(body.EventIDs))
	for _, eventID := range body.EventIDs {
		if _, exists := results[eventID]; exists {
			continue
		}
		var queried api.QueryEventsByIDResponse
		if err := rsAPI.QueryEventsByID(req.Context(), &api.QueryEventsByIDRequest{
			EventIDs: []string{eventID}, ExcludeRejected: true,
		}, &queried); err != nil {
			results[eventID] = syncPrunedResult{Status: "error: " + err.Error(), Output: "error", State: "error"}
			continue
		}
		if len(queried.Events) == 0 {
			results[eventID] = syncPrunedResult{Status: "not_in_roomserver", Output: "not_present", State: "not_present"}
			continue
		}
		event := queried.Events[0]
		if event.EventID() != eventID {
			results[eventID] = syncPrunedResult{Status: "roomserver_returned_wrong_event", Output: "not_present", State: "not_present"}
			continue
		}
		if event.Redacted() {
			results[eventID] = syncPrunedResult{Status: "not_refetched", Output: "not_present", State: "not_present"}
			continue
		}
		outputStatus, stateStatus, outputHash, stateHash, err := syncDB.ReplaceEventJSON(req.Context(), eventID, event, body.DryRun)
		if err != nil {
			results[eventID] = syncPrunedResult{Status: "error: " + err.Error(), Output: outputStatus, State: stateStatus}
			continue
		}
		status := syncPrunedOverallStatus(outputStatus, stateStatus, body.DryRun)
		results[eventID] = syncPrunedResult{Status: status, Output: outputStatus, State: stateStatus, OutputHash: outputHash, StateHash: stateHash}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: struct {
		DryRun  bool                        `json:"dry_run"`
		Results map[string]syncPrunedResult `json:"results"`
	}{body.DryRun, results}}
}

func syncPrunedOverallStatus(output, state string, dryRun bool) string {
	if output == "write_skipped" || state == "write_skipped" {
		return "partial_or_skipped"
	}
	if output == "redacted" || state == "redacted" {
		if (output == "redacted" && (state == "redacted" || state == "not_present")) ||
			(state == "redacted" && output == "not_present") {
			return "redacted"
		}
		return "partial_or_skipped"
	}
	if dryRun {
		if output == "would_update_output" && state == "would_update_state" {
			return "would_update_both"
		}
		if output == "would_update_output" {
			return "would_update_output"
		}
		if state == "would_update_state" {
			return "would_update_state"
		}
	}
	if output == "updated_output" && state == "updated_state" {
		return "both"
	}
	if output == "updated_output" {
		return "updated_output"
	}
	if state == "updated_state" {
		return "updated_state"
	}
	if output == "not_present" && state == "not_present" {
		return "not_present"
	}
	if output == "redacted" || state == "redacted" {
		return "redacted"
	}
	return "write_skipped"
}
