// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package syncapi

import (
	"os"
	"path/filepath"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/fulltext"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/consumers"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/notifier"
	"codefloe.com/pat-s/zendrite/syncapi/producers"
	"codefloe.com/pat-s/zendrite/syncapi/routing"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/streams"
	"codefloe.com/pat-s/zendrite/syncapi/sync"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// AddPublicRoutes sets up and registers HTTP handlers for the SyncAPI
// component.
func AddPublicRoutes(
	processContext *process.ProcessContext,
	routers httputil.Routers,
	zendriteCfg *config.Zendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	userAPI userapi.SyncUserAPI,
	rsAPI api.SyncRoomserverAPI,
	caches caching.LazyLoadCache,
	enableMetrics bool,
	fedClient fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier,
) {
	js, natsClient := natsInstance.Prepare(processContext, &zendriteCfg.Global.JetStream)

	syncDB, err := storage.NewSyncServerDatasource(processContext.Context(), cm, &zendriteCfg.SyncAPI.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to sync db")
	}

	// Start the sliding sync metadata worker for Phase 12 optimization
	metadataWorker := internal.NewSlidingSyncMetadataWorker(processContext, syncDB)
	if err = metadataWorker.Start(processContext.Context()); err != nil {
		logrus.WithError(err).Warn("failed to start sliding sync metadata worker")
		// Non-fatal - we can continue without background population
	}

	eduCache := caching.NewTypingCache(processContext.Context())
	processContext.Defer(eduCache.Close)
	notifier := notifier.NewNotifier(rsAPI)
	streams := streams.NewSyncStreamProviders(processContext.Context(), syncDB, userAPI, rsAPI, eduCache, caches, notifier)
	notifier.SetCurrentPosition(streams.Latest(processContext.Context()))
	if err = notifier.Load(processContext.Context(), syncDB); err != nil {
		logrus.WithError(err).Panicf("failed to load notifier ")
	}

	// index stays a nil interface when search is disabled.
	var index fulltext.Indexer
	if zendriteCfg.SyncAPI.Fulltext.Enabled {
		searchConfig := zendriteCfg.SyncAPI.Fulltext
		if searchConfig.RebuildOnStartup && !searchConfig.InMemory {
			// A new directory avoids stale documents and never deletes an operator's index.
			if err = os.MkdirAll(string(searchConfig.IndexPath), 0o700); err != nil {
				logrus.WithError(err).Panic("failed to create search recovery directory")
			}
			var directory string
			directory, err = os.MkdirTemp(string(searchConfig.IndexPath), "recovery-")
			if err != nil {
				logrus.WithError(err).Panic("failed to allocate search recovery index")
			}
			processContext.Defer(func() { _ = os.RemoveAll(directory) })
			searchConfig.IndexPath = config.Path(filepath.Join(directory, "index"))
		}
		fts, ftsErr := fulltext.New(processContext, searchConfig)
		if ftsErr != nil {
			logrus.WithError(ftsErr).Panicf("failed to create full text")
		}
		index = fts
		if searchConfig.RebuildOnStartup {
			// Consumers write to the index during the rebuild; searches wait for it.
			recovering := fulltext.NewRecovering(fts)
			index = recovering
			processContext.Go(func() { consumers.RebuildSearch(processContext.Context(), syncDB, recovering) })
		}
	}

	federationPresenceProducer := &producers.FederationAPIPresenceProducer{
		Topic:     zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		JetStream: js,
	}
	presenceConsumer := consumers.NewPresenceConsumer(
		processContext, &zendriteCfg.SyncAPI, js, natsClient, syncDB,
		notifier, streams.PresenceStreamProvider,
		userAPI,
	)

	requestPool := sync.NewRequestPoolWithContext(processContext.Context(), syncDB, &zendriteCfg.SyncAPI, userAPI, rsAPI, streams, notifier, federationPresenceProducer, presenceConsumer, enableMetrics)

	if err = presenceConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start presence consumer")
	}

	keyChangeConsumer := consumers.NewOutputKeyChangeEventConsumer(
		processContext, &zendriteCfg.SyncAPI, zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		js, rsAPI, syncDB, notifier,
		streams.DeviceListStreamProvider,
	)
	if err = keyChangeConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start key change consumer")
	}

	var asProducer *producers.AppserviceEventProducer
	if len(zendriteCfg.AppServiceAPI.Derived.ApplicationServices) > 0 {
		asProducer = &producers.AppserviceEventProducer{
			JetStream: js, Topic: zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputAppserviceEvent),
		}
	}

	pushProducer := &producers.PushEventProducer{
		JetStream: js, Topic: zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputPushEvent),
	}

	roomConsumer := consumers.NewOutputRoomEventConsumer(
		processContext, &zendriteCfg.SyncAPI, js, syncDB, notifier, streams.PDUStreamProvider,
		streams.InviteStreamProvider, streams.StickyEventsStreamProvider, rsAPI, index, asProducer, pushProducer,
	)
	// Wire up the metadata worker for continuous updates (Phase 12 optimization)
	roomConsumer.SetMetadataQueuer(metadataWorker)
	if err = roomConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start room server consumer")
	}

	clientConsumer := consumers.NewOutputClientDataConsumer(
		processContext, &zendriteCfg.SyncAPI, js, natsClient, syncDB, notifier,
		streams.AccountDataStreamProvider, index,
	)
	if err = clientConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start client data consumer")
	}

	notificationConsumer := consumers.NewOutputNotificationDataConsumer(
		processContext, &zendriteCfg.SyncAPI, js, syncDB, notifier, streams.NotificationDataStreamProvider,
	)
	if err = notificationConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start notification data consumer")
	}

	typingConsumer := consumers.NewOutputTypingEventConsumer(
		processContext, &zendriteCfg.SyncAPI, js, eduCache, notifier, streams.TypingStreamProvider,
	)
	if err = typingConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start typing consumer")
	}

	sendToDeviceConsumer := consumers.NewOutputSendToDeviceEventConsumer(
		processContext, &zendriteCfg.SyncAPI, js, syncDB, userAPI, notifier, streams.SendToDeviceStreamProvider,
	)
	if err = sendToDeviceConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start send-to-device consumer")
	}
	requestPool.SetSendToDeviceBarrier(func() {
		sendToDeviceConsumer.WaitUntilCaughtUp(processContext.Context(), time.Second)
	})

	receiptConsumer := consumers.NewOutputReceiptEventConsumer(
		processContext, &zendriteCfg.SyncAPI, js, syncDB, notifier, streams.ReceiptStreamProvider,
	)
	if err = receiptConsumer.Start(); err != nil {
		logrus.WithError(err).Panicf("failed to start receipts consumer")
	}

	rateLimits := httputil.NewRateLimits(&zendriteCfg.ClientAPI.RateLimiting, processContext.Context())

	routing.Setup(
		routers.Client, requestPool, syncDB, userAPI,
		rsAPI, &zendriteCfg.SyncAPI, caches, index,
		rateLimits, fedClient,
	)
	if keyRing != nil {
		routing.SetupFederation(routers.Federation, &zendriteCfg.SyncAPI, syncDB, rsAPI, keyRing)
	}
}
