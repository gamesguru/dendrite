// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package userapi

import (
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	fedsenderapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/internal/pushgateway"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/consumers"
	"codefloe.com/pat-s/zendrite/userapi/internal"
	"codefloe.com/pat-s/zendrite/userapi/producers"
	"codefloe.com/pat-s/zendrite/userapi/storage"
	"codefloe.com/pat-s/zendrite/userapi/util"
)

// NewInternalAPI returns a concrete implementation of the internal API. Callers
// can call functions directly on the returned API or via an HTTP interface using AddInternalRoutes.
//
// Creating a new instance of the user API requires a roomserver API with a federation API set
// using its `SetFederationAPI` method, other you may get nil-dereference errors.
func NewInternalAPI(
	processContext *process.ProcessContext,
	zendriteCfg *config.Zendrite,
	cm *sqlutil.Connections,
	natsInstance *jetstream.NATSInstance,
	rsAPI rsapi.UserRoomserverAPI,
	fedClient fedsenderapi.KeyserverFederationAPI,
	enableMetrics bool,
	blacklistedOrBackingOffFn func(s spec.ServerName) (*statistics.ServerStatistics, error),
) *internal.UserInternalAPI {
	js, _ := natsInstance.Prepare(processContext, &zendriteCfg.Global.JetStream)
	appServices := zendriteCfg.Derived.ApplicationServices

	pgClient := pushgateway.NewHTTPClient(zendriteCfg.UserAPI.PushGatewayDisableTLSValidation)

	db, err := storage.NewUserDatabase(
		processContext.Context(),
		cm,
		&zendriteCfg.UserAPI.AccountDatabase,
		zendriteCfg.Global.ServerName,
		zendriteCfg.UserAPI.BCryptCost,
		zendriteCfg.UserAPI.OpenIDTokenLifetimeMS,
		api.DefaultLoginTokenLifetime,
		zendriteCfg.UserAPI.Matrix.ServerNotices.LocalPart,
	)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to accounts db")
	}

	keyDB, err := storage.NewKeyDatabase(cm, &zendriteCfg.KeyServer.Database)
	if err != nil {
		logrus.WithError(err).Panicf("failed to connect to key db")
	}

	syncProducer := producers.NewSyncAPI(
		db, js,
		// TODO: user API should handle syncs for account data. Right now,
		// it's handled by clientapi, and hence uses its topic. When user
		// API handles it for all account data, we can remove it from
		// here.
		zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputClientData),
		zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputNotificationData),
	)
	keyChangeProducer := &producers.KeyChange{
		Topic:     zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputKeyChangeEvent),
		JetStream: js,
		DB:        keyDB,
	}

	userAPI := &internal.UserInternalAPI{
		DB:                   db,
		KeyDatabase:          keyDB,
		SyncProducer:         syncProducer,
		KeyChangeProducer:    keyChangeProducer,
		Config:               &zendriteCfg.UserAPI,
		HTTPClient:           &http.Client{Timeout: 10 * time.Second}, //nolint:mnd
		AppServices:          appServices,
		RSAPI:                rsAPI,
		DisableTLSValidation: zendriteCfg.UserAPI.PushGatewayDisableTLSValidation,
		PgClient:             pgClient,
		FedClient:            fedClient,
	}

	updater := internal.NewDeviceListUpdater(processContext, keyDB, userAPI, keyChangeProducer, fedClient, zendriteCfg.UserAPI.WorkerCount, rsAPI, zendriteCfg.Global.ServerName, enableMetrics, blacklistedOrBackingOffFn)
	userAPI.Updater = updater
	// Remove users which we don't share a room with anymore
	if err := updater.CleanUp(); err != nil {
		logrus.WithError(err).Error("failed to cleanup stale device lists")
	}

	go func() {
		if err := updater.Start(); err != nil {
			logrus.WithError(err).Panicf("failed to start device list updater")
		}
	}()

	dlConsumer := consumers.NewDeviceListUpdateConsumer(
		processContext, &zendriteCfg.UserAPI, js, updater,
	)
	if err := dlConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start device list consumer")
	}

	sigConsumer := consumers.NewSigningKeyUpdateConsumer(
		processContext, &zendriteCfg.UserAPI, js, userAPI,
	)
	if err := sigConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start signing key consumer")
	}

	receiptConsumer := consumers.NewOutputReceiptEventConsumer(
		processContext, &zendriteCfg.UserAPI, js, db, syncProducer, pgClient,
	)
	if err := receiptConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start user API receipt consumer")
	}

	eventConsumer := consumers.NewOutputRoomEventConsumer(
		processContext, &zendriteCfg.UserAPI, js, db, pgClient, rsAPI, syncProducer,
	)
	if err := eventConsumer.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start user API streamed event consumer")
	}

	var cleanOldNotifs func()
	cleanOldNotifs = func() {
		logrus.Infof("Cleaning old notifications")
		if err := db.DeleteOldNotifications(processContext.Context()); err != nil {
			logrus.WithError(err).Error("Failed to clean old notifications")
		}
		time.AfterFunc(time.Hour, cleanOldNotifs)
	}
	time.AfterFunc(time.Minute, cleanOldNotifs)

	if zendriteCfg.Global.ReportStats.Enabled {
		go util.StartPhoneHomeCollector(time.Now(), zendriteCfg, db)
	}

	return userAPI
}
