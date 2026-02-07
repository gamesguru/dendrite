// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package mediaapi

import (
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/dendrite/internal/httputil"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/mediaapi/routing"
	"codefloe.com/pat-s/dendrite/mediaapi/storage"
	"codefloe.com/pat-s/dendrite/setup/config"
	userapi "codefloe.com/pat-s/dendrite/userapi/api"
)

// AddPublicRoutes sets up and registers HTTP handlers for the MediaAPI component.
func AddPublicRoutes(
	routers httputil.Routers,
	cm *sqlutil.Connections,
	cfg *config.Dendrite,
	userAPI userapi.MediaUserAPI,
	client *fclient.Client,
	fedClient fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier,
) {
	mediaDB, err := storage.NewMediaAPIDatasource(cm, &cfg.MediaAPI.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to media db")
	}

	routing.Setup(
		routers, cfg, mediaDB, userAPI, client, fedClient, keyRing,
	)
}
