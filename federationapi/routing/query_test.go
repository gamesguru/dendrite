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

	fedAPI "codefloe.com/pat-s/zendrite/federationapi"
	"codefloe.com/pat-s/zendrite/federationapi/routing"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

type fakeFedClient struct {
	fclient.FederationClient
}

func (f *fakeFedClient) LookupRoomAlias(ctx context.Context, origin, s spec.ServerName, roomAlias string) (res fclient.RespDirectory, err error) {
	return
}

func TestHandleQueryDirectory(t *testing.T) {
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
		req := fclient.NewFederationRequest("GET", serverName, testOrigin, "/_matrix/federation/v1/query/directory?room_alias="+url.QueryEscape("#room:server"))
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
