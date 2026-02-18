// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package relayapi

import (
	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/producers"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/relayapi/api"
	"codefloe.com/pat-s/zendrite/relayapi/internal"
	"codefloe.com/pat-s/zendrite/relayapi/routing"
	"codefloe.com/pat-s/zendrite/relayapi/storage"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// AddPublicRoutes sets up and registers HTTP handlers on the base API muxes for the FederationAPI component.
func AddPublicRoutes(
	routers httputil.Routers,
	zendriteCfg *config.Zendrite,
	keyRing gomatrixserverlib.JSONVerifier,
	relayAPI api.RelayInternalAPI,
) {
	relay, ok := relayAPI.(*internal.RelayInternalAPI)
	if !ok {
		panic("relayapi.AddPublicRoutes called with a RelayInternalAPI impl which was not " +
			"RelayInternalAPI. This is a programming error.")
	}

	routing.Setup(
		routers.Federation,
		&zendriteCfg.FederationAPI,
		relay,
		keyRing,
	)
}

func NewRelayInternalAPI(
	zendriteCfg *config.Zendrite,
	cm *sqlutil.Connections,
	fedClient fclient.FederationClient,
	rsAPI rsAPI.RoomserverInternalAPI,
	keyRing *gomatrixserverlib.KeyRing,
	producer *producers.SyncAPIProducer,
	relayingEnabled bool,
	caches caching.FederationCache,
) api.RelayInternalAPI {
	relayDB, err := storage.NewDatabase(cm, &zendriteCfg.RelayAPI.Database, caches, zendriteCfg.Global.IsLocalServerName)
	if err != nil {
		logrus.WithError(err).Panic("failed to connect to relay db")
	}

	return internal.NewRelayInternalAPI(
		relayDB,
		fedClient,
		rsAPI,
		keyRing,
		producer,
		zendriteCfg.Global.Presence.EnableInbound,
		zendriteCfg.Global.ServerName,
		relayingEnabled,
	)
}
