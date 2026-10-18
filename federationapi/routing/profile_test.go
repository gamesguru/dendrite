// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http/httptest"
	"net/url"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	fedAPI "codefloe.com/pat-s/zendrite/federationapi"
	"codefloe.com/pat-s/zendrite/federationapi/routing"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

type fakeUserAPI struct {
	userAPI.FederationUserAPI
}

func (u *fakeUserAPI) QueryProfile(ctx context.Context, userID string) (*authtypes.Profile, error) {
	return &authtypes.Profile{
		DisplayName: "User",
		Fields:      map[string]json.RawMessage{"m.tz": json.RawMessage(`"Europe/Berlin"`)},
	}, nil
}

func TestHandleQueryProfile(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		routers := httputil.NewRouters()
		defer close()

		natsInstance := jetstream.NATSInstance{}
		cfg.FederationAPI.Matrix.ServerName = testOrigin
		cfg.FederationAPI.Matrix.Metrics.Enabled = false
		fedClient := fakeFedClient{}
		serverKeyAPI := &testKeys{}
		keyRing := serverKeyAPI.KeyRing()
		fedapi := fedAPI.NewInternalAPI(processCtx, cfg, cm, &natsInstance, &fedClient, nil, nil, keyRing, true)
		userapi := fakeUserAPI{}

		routing.Setup(routers, cfg, nil, fedapi, keyRing, &fedClient, &userapi, &cfg.MSCs, nil, caching.DisableMetrics)

		_, sk, _ := ed25519.GenerateKey(nil)
		keyID := testKeyID
		pk, ok := sk.Public().(ed25519.PublicKey)
		if !ok {
			t.Fatal("unexpected public key type")
		}
		serverName := spec.ServerName(hex.EncodeToString(pk))

		for _, tc := range []struct {
			field    string
			wantCode int
			wantBody string
		}{
			{"", 200, `{"displayname":"User","m.tz":"Europe/Berlin"}`},
			{"displayname", 200, `{"displayname":"User"}`},
			{"avatar_url", 200, `{}`},
			{"m.tz", 200, `{"m.tz":"Europe/Berlin"}`},
			{"org.example.unset", 404, `{"errcode":"M_NOT_FOUND","error":"The requested profile key does not exist."}`},
		} {
			t.Run("field="+tc.field, func(t *testing.T) {
				path := "/_matrix/federation/v1/query/profile?user_id=" + url.QueryEscape("@user:"+string(testOrigin))
				if tc.field != "" {
					path += "&field=" + url.QueryEscape(tc.field)
				}
				req := fclient.NewFederationRequest("GET", serverName, testOrigin, path)
				type queryContent struct{}
				content := queryContent{}
				err := req.SetContent(content)
				if err != nil {
					t.Fatalf("Error: %s", err.Error())
				}
				err = req.Sign(serverName, gomatrixserverlib.KeyID(keyID), sk)
				if err != nil {
					t.Fatalf("Error: %s", err.Error())
				}
				httpReq, err := req.HTTPRequest()
				if err != nil {
					t.Fatalf("Error: %s", err.Error())
				}
				w := httptest.NewRecorder()
				routers.Federation.ServeHTTP(w, httpReq)

				res := w.Result()
				data, _ := io.ReadAll(res.Body)
				assert.Equal(t, tc.wantCode, res.StatusCode)
				assert.JSONEq(t, tc.wantBody, string(data))
			})
		}
	})
}
