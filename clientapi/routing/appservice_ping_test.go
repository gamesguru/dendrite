// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/setup/config"
)

func TestAppservicePing(t *testing.T) {
	// The handler may still run after a timed-out ping returned, so its
	// state is guarded.
	var mu sync.Mutex
	var gotAuth, gotBody string
	status := http.StatusOK
	delay := time.Duration(0)
	set := func(newStatus int, newDelay time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		status, delay = newStatus, newDelay
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_matrix/app/v1/ping" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		gotBody = string(b)
		respStatus, respDelay := status, delay
		mu.Unlock()
		time.Sleep(respDelay)
		w.WriteHeader(respStatus)
		_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN_TOKEN"}`))
	}))
	defer srv.Close()

	bridge := config.ApplicationService{ID: "bridge", URL: srv.URL, ASToken: "as_bridge", HSToken: "hs_bridge"}
	bridge.CreateHTTPClient(false)
	noURL := config.ApplicationService{ID: "nourl", ASToken: "as_nourl", HSToken: "hs_nourl"}
	noURL.CreateHTTPClient(false)
	cfg := &config.AppServiceAPI{Derived: &config.Derived{}}
	cfg.Derived.ApplicationServices = []config.ApplicationService{bridge, noURL}

	ping := func(token, asID, body string) (int, map[string]any) {
		req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v1/appservice/"+asID+"/ping", strings.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res := AppservicePing(req, cfg, asID)
		var out map[string]any
		b, err := json.Marshal(res.JSON)
		if err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(b, &out)
		return res.Code, out
	}

	t.Run("successful ping passes the transaction ID", func(t *testing.T) {
		code, out := ping("as_bridge", "bridge", `{"transaction_id":"txn1"}`)
		if code != http.StatusOK {
			t.Fatalf("got %d: %v", code, out)
		}
		if _, ok := out["duration_ms"]; !ok {
			t.Fatalf("duration_ms missing: %v", out)
		}
		mu.Lock()
		defer mu.Unlock()
		if gotAuth != "Bearer hs_bridge" || gotBody != `{"transaction_id":"txn1"}` {
			t.Fatalf("unexpected ping request: %q %q", gotAuth, gotBody)
		}
	})

	t.Run("missing token", func(t *testing.T) {
		if code, _ := ping("", "bridge", `{}`); code != http.StatusUnauthorized {
			t.Fatalf("got %d", code)
		}
	})

	t.Run("token of another appservice or a user", func(t *testing.T) {
		for _, token := range []string{"as_nourl", "some_user_token"} {
			code, out := ping(token, "bridge", `{}`)
			if code != http.StatusForbidden || out["errcode"] != "M_FORBIDDEN" {
				t.Fatalf("token %s: got %d %v", token, code, out)
			}
		}
	})

	t.Run("no URL configured", func(t *testing.T) {
		code, out := ping("as_nourl", "nourl", `{}`)
		if code != http.StatusBadRequest || out["errcode"] != "M_URL_NOT_SET" {
			t.Fatalf("got %d %v", code, out)
		}
	})

	t.Run("bad status", func(t *testing.T) {
		set(http.StatusUnauthorized, 0)
		defer set(http.StatusOK, 0)
		code, out := ping("as_bridge", "bridge", `{}`)
		if code != http.StatusBadGateway || out["errcode"] != "M_BAD_STATUS" ||
			out["status"] != float64(http.StatusUnauthorized) || out["body"] != `{"errcode":"M_UNKNOWN_TOKEN"}` {
			t.Fatalf("got %d %v", code, out)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		set(http.StatusOK, 200*time.Millisecond)
		defer set(http.StatusOK, 0)
		as := &cfg.Derived.ApplicationServices[0]
		as.HTTPClient.Timeout = 50 * time.Millisecond
		defer func() { as.HTTPClient.Timeout = 30 * time.Second }()
		code, out := ping("as_bridge", "bridge", `{}`)
		if code != http.StatusGatewayTimeout || out["errcode"] != "M_CONNECTION_TIMEOUT" {
			t.Fatalf("got %d %v", code, out)
		}
	})

	t.Run("connection failed", func(t *testing.T) {
		as := &cfg.Derived.ApplicationServices[0]
		url := as.URL
		as.URL = "http://127.0.0.1:1"
		defer func() { as.URL = url }()
		code, out := ping("as_bridge", "bridge", `{}`)
		if code != http.StatusBadGateway || out["errcode"] != "M_CONNECTION_FAILED" {
			t.Fatalf("got %d %v", code, out)
		}
	})
}
