//go:build rezzy && linux && cgo

package routing

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"

	"codefloe.com/pat-s/zendrite/internal/reconciliation"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

type fakeReconciliationRSAPI struct {
	roomserverAPI.FederationRoomserverAPI // embed the rest; only used methods are called
	state                                 roomserverAPI.QueryRoomReconciliationStateResponse
}

func (f *fakeReconciliationRSAPI) QueryRoomReconciliationState(
	_ context.Context, _ *roomserverAPI.QueryRoomReconciliationStateRequest,
	res *roomserverAPI.QueryRoomReconciliationStateResponse,
) error {
	*res = f.state
	return nil
}

func (f *fakeReconciliationRSAPI) QueryServerJoinedToRoom(
	_ context.Context, _ *roomserverAPI.QueryServerJoinedToRoomRequest,
	res *roomserverAPI.QueryServerJoinedToRoomResponse,
) error {
	res.RoomExists = true
	res.IsInRoom = true
	return nil
}

func (f *fakeReconciliationRSAPI) QueryServerBannedFromRoom(
	_ context.Context, _ *roomserverAPI.QueryServerBannedFromRoomRequest,
	_ *roomserverAPI.QueryServerBannedFromRoomResponse,
) error {
	return nil
}

func reconciliationTestID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "$" + base64.RawURLEncoding.EncodeToString(sum[:])
}

func newReconciliationTestRequest(method, path string, body any) (*http.Request, *fclient.FederationRequest) {
	request := fclient.NewFederationRequest(method, "origin.example", "destination.example", path)
	if body != nil {
		if err := request.SetContent(body); err != nil {
			panic(err)
		}
	}
	httpReq, err := request.HTTPRequest()
	if err != nil {
		panic(err)
	}
	return httpReq, &request
}

func TestRoomDigestAndDiff(t *testing.T) {
	base := []string{
		reconciliationTestID("create"), reconciliationTestID("join"),
		reconciliationTestID("message-1"), reconciliationTestID("message-2"),
	}
	extraRemote := reconciliationTestID("remote-only")
	remote := append(append([]string{}, base...), extraRemote)
	state := roomserverAPI.QueryRoomReconciliationStateResponse{
		RoomVersion:        gomatrixserverlib.RoomVersionV10,
		EventIDs:           base,
		ForwardExtremities: []string{base[len(base)-1]},
	}
	rsAPI := &fakeReconciliationRSAPI{state: state}

	t.Run("Digest", func(t *testing.T) {
		httpReq, request := newReconciliationTestRequest("GET", "/", nil)
		res := RoomDigest(httpReq, request, rsAPI, "!room:destination.example", true)
		if res.Code != 200 {
			t.Fatalf("status = %d: %v", res.Code, res.JSON)
		}
		body, _ := json.Marshal(res.JSON)
		var parsed struct {
			Digest string   `json:"digest"`
			Strata []string `json:"strata"`
			Count  uint64   `json:"known_event_count"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if parsed.Count != uint64(len(base)) || parsed.Digest == "" || len(parsed.Strata) != 32 {
			t.Fatalf("unexpected digest response: %s", body)
		}
	})

	t.Run("Diff", func(t *testing.T) {
		remoteSnapshot, err := reconciliation.Snapshot(remote, "v4plus")
		if err != nil {
			t.Fatalf("remote snapshot: %v", err)
		}
		requests := []reconciliation.BucketRequest{{Depth: 0, Prefix: 0, Capacity: 4}}
		sketches, err := reconciliation.BucketSketches(remote, "v4plus", requests)
		if err != nil {
			t.Fatalf("remote sketches: %v", err)
		}
		body := map[string]any{
			"mode":                    "sketch",
			"digest_type":             reconciliation.DigestType,
			"frame_id":                "frame",
			"local_digest":            remoteSnapshot.Digest,
			"local_known_event_count": remoteSnapshot.Count,
			"requests": []map[string]any{{
				"depth": 0, "prefix": "0", "capacity": 4,
			}},
			"local_sketches": sketches,
			"scope":          "event_set",
		}
		httpReq, request := newReconciliationTestRequest("POST", "/", body)
		res := RoomDiff(httpReq, request, rsAPI, "!room:destination.example")
		if res.Code != 200 {
			t.Fatalf("status = %d: %v", res.Code, res.JSON)
		}
		encoded, _ := json.Marshal(res.JSON)
		var parsed struct {
			Missing       []string `json:"missing_event_ids"`
			RequesterOnly []string `json:"requester_only_short_ids"`
			SketchStatus  string   `json:"sketch_status"`
			Expected      string   `json:"expected_requester_side_accumulator"`
		}
		if err := json.Unmarshal(encoded, &parsed); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if parsed.SketchStatus != "decoded" {
			t.Fatalf("sketch status = %q: %s", parsed.SketchStatus, encoded)
		}
		if len(parsed.Missing) != 0 {
			t.Fatalf("missing = %v, want none", parsed.Missing)
		}
		if len(parsed.RequesterOnly) != 1 {
			t.Fatalf("requester-only = %v, want one", parsed.RequesterOnly)
		}
		if parsed.Expected == "" {
			t.Fatalf("missing expected accumulator: %s", encoded)
		}
	})

	t.Run("CapacityExceeded", func(t *testing.T) {
		remoteExtra := make([]string, 0, 12)
		for i := 0; i < 12; i++ {
			remoteExtra = append(remoteExtra, reconciliationTestID("remote-only-"+strconv.Itoa(i)))
		}
		remoteWithExtras := append(append([]string{}, base...), remoteExtra...)
		remoteSnapshot, err := reconciliation.Snapshot(remoteWithExtras, "v4plus")
		if err != nil {
			t.Fatalf("remote snapshot: %v", err)
		}
		requests := []reconciliation.BucketRequest{{Depth: 0, Prefix: 0, Capacity: 8}}
		sketches, err := reconciliation.BucketSketches(remoteWithExtras, "v4plus", requests)
		if err != nil {
			t.Fatalf("remote sketches: %v", err)
		}
		body := map[string]any{
			"mode":                    "sketch",
			"digest_type":             reconciliation.DigestType,
			"frame_id":                "frame",
			"local_digest":            remoteSnapshot.Digest,
			"local_known_event_count": remoteSnapshot.Count,
			"requests": []map[string]any{{
				"depth": 0, "prefix": "0", "capacity": 8,
			}},
			"local_sketches": sketches,
		}
		httpReq, request := newReconciliationTestRequest("POST", "/", body)
		res := RoomDiff(httpReq, request, rsAPI, "!room:destination.example")
		if res.Code != 200 {
			t.Fatalf("status = %d: %v", res.Code, res.JSON)
		}
		encoded, _ := json.Marshal(res.JSON)
		var parsed struct {
			SketchStatus string `json:"sketch_status"`
			Truncated    bool   `json:"truncated"`
		}
		if err := json.Unmarshal(encoded, &parsed); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if parsed.SketchStatus != "capacity_exceeded" || !parsed.Truncated {
			t.Fatalf("unexpected capacity response: %s", encoded)
		}
	})

	t.Run("InvalidMode", func(t *testing.T) {
		httpReq, request := newReconciliationTestRequest("POST", "/", map[string]any{
			"mode":           "digest",
			"digest_type":    reconciliation.DigestType,
			"requests":       []any{},
			"local_sketches": []any{},
		})
		res := RoomDiff(httpReq, request, rsAPI, "!room:destination.example")
		if res.Code != 400 {
			t.Fatalf("status = %d, want 400", res.Code)
		}
	})
}
