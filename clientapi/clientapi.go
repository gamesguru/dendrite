// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/httputil"
	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/setup/process"
	userapi "codefloe.com/pat-s/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/fclient"

	appserviceAPI "codefloe.com/pat-s/dendrite/appservice/api"
	"codefloe.com/pat-s/dendrite/clientapi/api"
	"codefloe.com/pat-s/dendrite/clientapi/producers"
	"codefloe.com/pat-s/dendrite/clientapi/routing"
	federationAPI "codefloe.com/pat-s/dendrite/federationapi/api"
	"codefloe.com/pat-s/dendrite/internal/transactions"
	roomserverAPI "codefloe.com/pat-s/dendrite/roomserver/api"
	"codefloe.com/pat-s/dendrite/setup/jetstream"
)

// AddPublicRoutes sets up and registers HTTP handlers for the ClientAPI component.
func AddPublicRoutes(
	processContext *process.ProcessContext,
	routers httputil.Routers,
	cfg *config.Dendrite,
	natsInstance *jetstream.NATSInstance,
	federation fclient.FederationClient,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	asAPI appserviceAPI.AppServiceInternalAPI,
	transactionsCache *transactions.Cache,
	fsAPI federationAPI.ClientFederationAPI,
	userAPI userapi.ClientUserAPI,
	userDirectoryProvider userapi.QuerySearchProfilesAPI,
	extRoomsProvider api.ExtraPublicRoomsProvider,
	caches *caching.Caches,
	enableMetrics bool,
) {
	js, natsClient := natsInstance.Prepare(processContext, &cfg.Global.JetStream)

	syncProducer := &producers.SyncAPIProducer{
		JetStream:              js,
		TopicReceiptEvent:      cfg.Global.JetStream.Prefixed(jetstream.OutputReceiptEvent),
		TopicSendToDeviceEvent: cfg.Global.JetStream.Prefixed(jetstream.OutputSendToDeviceEvent),
		TopicTypingEvent:       cfg.Global.JetStream.Prefixed(jetstream.OutputTypingEvent),
		TopicPresenceEvent:     cfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		UserAPI:                userAPI,
		ServerName:             cfg.Global.ServerName,
	}

	routing.Setup(
		routers,
		cfg, rsAPI, asAPI,
		userAPI, userDirectoryProvider, federation,
		syncProducer, transactionsCache, fsAPI,
		extRoomsProvider, caches, natsClient, enableMetrics,
	)
}
