// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
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

const (
	testOrigin = spec.ServerName("kaer.morhen")
)

type sendContent struct {
	PDUs []json.RawMessage       `json:"pdus"`
	EDUs []gomatrixserverlib.EDU `json:"edus"`
}

func TestHandleSend(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		routers := httputil.NewRouters()
		defer close()

		natsInstance := jetstream.NATSInstance{}
		cfg.FederationAPI.Matrix.ServerName = testOrigin
		cfg.FederationAPI.Matrix.Metrics.Enabled = false
		fedapi := fedAPI.NewInternalAPI(processCtx, cfg, cm, &natsInstance, nil, nil, nil, nil, true)
		serverKeyAPI := &testKeys{}
		keyRing := serverKeyAPI.KeyRing()

		routing.Setup(routers, cfg, nil, fedapi, keyRing, nil, nil, &cfg.MSCs, nil, caching.DisableMetrics)

		_, sk, _ := ed25519.GenerateKey(nil)
		keyID := testKeyID
		pk, ok := sk.Public().(ed25519.PublicKey)
		if !ok {
			t.Fatal("unexpected public key type")
		}
		serverName := spec.ServerName(hex.EncodeToString(pk))
		req := fclient.NewFederationRequest("PUT", serverName, testOrigin, "/_matrix/federation/v1/send/1234")
		content := sendContent{}
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
		assert.Equal(t, 200, res.StatusCode)
	})
}
