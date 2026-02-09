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
	"io"
	"net/http/httptest"
	"net/url"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/dendrite/clientapi/auth/authtypes"
	fedAPI "codefloe.com/pat-s/dendrite/federationapi"
	"codefloe.com/pat-s/dendrite/federationapi/routing"
	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/httputil"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/setup/jetstream"
	"codefloe.com/pat-s/dendrite/test"
	"codefloe.com/pat-s/dendrite/test/testrig"
	userAPI "codefloe.com/pat-s/dendrite/userapi/api"
)

type fakeUserAPI struct {
	userAPI.FederationUserAPI
}

func (u *fakeUserAPI) QueryProfile(ctx context.Context, userID string) (*authtypes.Profile, error) {
	return &authtypes.Profile{}, nil
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
		req := fclient.NewFederationRequest("GET", serverName, testOrigin, "/_matrix/federation/v1/query/profile?user_id="+url.QueryEscape("@user:"+string(testOrigin)))
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
		t.Log(string(data))
		assert.Equal(t, 200, res.StatusCode)
	})
}
