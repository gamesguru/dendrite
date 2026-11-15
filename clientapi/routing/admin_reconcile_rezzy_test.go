package routing

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/go-chi/chi/v5"

	"codefloe.com/pat-s/zendrite/internal/reconciliation"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type fakeReconcileRS struct {
	roomserverAPI.ClientRoomserverAPI
	local    []string
	outcomes map[string]string
	fetched  []string
}

func (f *fakeReconcileRS) QueryRoomVersionForRoom(context.Context, string) (gomatrixserverlib.RoomVersion, error) {
	return gomatrixserverlib.RoomVersionV10, nil
}

func (f *fakeReconcileRS) QueryRoomReconciliationState(
	_ context.Context, _ *roomserverAPI.QueryRoomReconciliationStateRequest,
	res *roomserverAPI.QueryRoomReconciliationStateResponse,
) error {
	res.EventIDs = f.local
	return nil
}

func (f *fakeReconcileRS) PerformAdminFetchEvent(
	_ context.Context, _, eventID string, _ spec.ServerName,
) (string, error) {
	f.fetched = append(f.fetched, eventID)
	return f.outcomes[eventID], nil
}

type fakeReconcileFed struct {
	fclient.FederationClient
	missing []string
}

func (f *fakeReconcileFed) DoRequestAndParseResponse(_ context.Context, req *http.Request, result any) error {
	var body any
	if strings.Contains(req.URL.Path, "room_digest") {
		body = map[string]any{"digest": "remote", "digest_type": reconciliation.DigestType, "known_event_count": 3}
	} else {
		body = map[string]any{
			"digest_type":       reconciliation.DigestType,
			"sketch_status":     "decoded",
			"missing_event_ids": f.missing,
		}
	}
	raw, _ := json.Marshal(body)
	return json.Unmarshal(raw, result)
}

func runReconcile(t *testing.T, rs *fakeReconcileRS, fed *fakeReconcileFed) map[string]any {
	t.Helper()
	_, key, _ := ed25519.GenerateKey(rand.Reader)
	cfg := &config.Zendrite{}
	cfg.Global.ServerName = "local.test"
	cfg.Global.SigningIdentity = fclient.SigningIdentity{
		ServerName: "local.test", KeyID: "ed25519:test", PrivateKey: key,
	}
	cfg.FederationAPI.MSC4521Enabled = true

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("serverName", "remote.test")
	rctx.URLParams.Add("roomID", "!room:remote.test")
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	res := AdminReconcileRoom(req, &userapi.Device{UserID: "@admin:local.test"}, rs, fed, cfg)
	if res.Code != http.StatusOK {
		t.Fatalf("code %d: %+v", res.Code, res.JSON)
	}
	raw, _ := json.Marshal(res.JSON)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAdminReconcileRoomReportsFetchOutcomes(t *testing.T) {
	rs := &fakeReconcileRS{
		local: []string{"$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		outcomes: map[string]string{
			"$good": "accepted",
			"$bad":  "rejected",
			"$gone": "not_stored",
		},
	}
	fed := &fakeReconcileFed{missing: []string{"$good", "$bad", "$gone", "$good"}}
	out := runReconcile(t, rs, fed)

	if len(rs.fetched) != 3 {
		t.Fatalf("expected each missing event fetched once, got %v", rs.fetched)
	}
	fetched, _ := out["fetched"].([]any)
	if len(fetched) != 1 || fetched[0] != "$good" {
		t.Fatalf("fetched = %v, want [$good]", out["fetched"])
	}
	failed, _ := out["failed"].(map[string]any)
	if failed["$bad"] != "rejected" || failed["$gone"] != "not_stored" || len(failed) != 2 {
		t.Fatalf("failed = %v", out["failed"])
	}
}

func TestAdminReconcileRoomNoProgress(t *testing.T) {
	rs := &fakeReconcileRS{
		local:    []string{"$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		outcomes: map[string]string{"$bad": "rejected"},
	}
	out := runReconcile(t, rs, &fakeReconcileFed{missing: []string{"$bad"}})
	if out["status"] != "no_progress" {
		t.Fatalf("status = %v, want no_progress", out["status"])
	}
}
