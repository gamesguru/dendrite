// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"sync"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/federationapi/producers"
	"codefloe.com/pat-s/zendrite/relayapi/storage"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

type RelayInternalAPI struct {
	db                     storage.Database
	fedClient              fclient.FederationClient
	rsAPI                  rsAPI.RoomserverInternalAPI
	keyRing                *gomatrixserverlib.KeyRing
	producer               *producers.SyncAPIProducer
	presenceEnabledInbound bool
	serverName             spec.ServerName
	relayingEnabledMutex   sync.Mutex
	relayingEnabled        bool
}

func NewRelayInternalAPI(
	db storage.Database,
	fedClient fclient.FederationClient,
	rsAPI rsAPI.RoomserverInternalAPI,
	keyRing *gomatrixserverlib.KeyRing,
	producer *producers.SyncAPIProducer,
	presenceEnabledInbound bool,
	serverName spec.ServerName,
	relayingEnabled bool,
) *RelayInternalAPI {
	return &RelayInternalAPI{
		db:                     db,
		fedClient:              fedClient,
		rsAPI:                  rsAPI,
		keyRing:                keyRing,
		producer:               producer,
		presenceEnabledInbound: presenceEnabledInbound,
		serverName:             serverName,
		relayingEnabled:        relayingEnabled,
	}
}
