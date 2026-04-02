package internal

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/federationapi/storage"
	"codefloe.com/pat-s/zendrite/federationapi/storage/cache"
	"codefloe.com/pat-s/zendrite/internal/caching"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// FederationInternalAPI is an implementation of api.FederationInternalAPI.
type FederationInternalAPI struct {
	db                 storage.Database
	cfg                *config.FederationAPI
	statistics         *statistics.Statistics
	rsAPI              roomserverAPI.FederationRoomserverAPI
	federation         fclient.FederationClient
	keyRing            *gomatrixserverlib.KeyRing
	queues             *queue.OutgoingQueues
	joins              sync.Map            // joins currently in progress
	partialStateWorker *PartialStateWorker // MSC3706: worker for background state resync
}

func NewFederationInternalAPI(
	db storage.Database, cfg *config.FederationAPI,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	federation fclient.FederationClient,
	statistics *statistics.Statistics,
	caches *caching.Caches,
	queues *queue.OutgoingQueues,
	keyRing *gomatrixserverlib.KeyRing,
) *FederationInternalAPI {
	serverKeyDB, err := cache.NewKeyDatabase(db, caches)
	if err != nil {
		logrus.WithError(err).Panicf("failed to set up caching wrapper for server key database")
	}

	if keyRing == nil {
		keyRing = &gomatrixserverlib.KeyRing{
			KeyFetchers: []gomatrixserverlib.KeyFetcher{},
			KeyDatabase: serverKeyDB,
		}

		pubKey, ok := cfg.Matrix.PrivateKey.Public().(ed25519.PublicKey)
		if !ok {
			logrus.Panicf("failed to cast public key to ed25519.PublicKey")
		}
		addDirectFetcher := func() {
			keyRing.KeyFetchers = append(
				keyRing.KeyFetchers,
				&gomatrixserverlib.DirectKeyFetcher{
					Client:            federation,
					IsLocalServerName: cfg.Matrix.IsLocalServerName,
					LocalPublicKey:    []byte(pubKey),
				},
			)
		}

		if cfg.PreferDirectFetch {
			addDirectFetcher()
		} else {
			defer addDirectFetcher()
		}

		b64e := base64.StdEncoding.WithPadding(base64.NoPadding)
		for _, ps := range cfg.KeyPerspectives {
			perspective := &gomatrixserverlib.PerspectiveKeyFetcher{
				PerspectiveServerName: ps.ServerName,
				PerspectiveServerKeys: map[gomatrixserverlib.KeyID]ed25519.PublicKey{},
				Client:                federation,
			}

			for _, key := range ps.Keys {
				rawkey, err := b64e.DecodeString(key.PublicKey)
				if err != nil {
					logrus.WithError(err).WithFields(logrus.Fields{
						"server_name": ps.ServerName,
						"public_key":  key.PublicKey,
					}).Warn("Couldn't parse perspective key")
					continue
				}
				perspective.PerspectiveServerKeys[key.KeyID] = rawkey
			}

			keyRing.KeyFetchers = append(keyRing.KeyFetchers, perspective)

			logrus.WithFields(logrus.Fields{
				"server_name":     ps.ServerName,
				"num_public_keys": len(ps.Keys),
			}).Info("Enabled perspective key fetcher")
		}
	}

	return &FederationInternalAPI{
		db:         db,
		cfg:        cfg,
		rsAPI:      rsAPI,
		keyRing:    keyRing,
		federation: federation,
		statistics: statistics,
		queues:     queues,
	}
}

// SetPartialStateWorker sets the partial state worker for MSC3706 background state resync.
func (a *FederationInternalAPI) SetPartialStateWorker(worker *PartialStateWorker) {
	a.partialStateWorker = worker
}

// IsServerBackingOff returns true if the server is blacklisted or currently in a backoff period.
func (a *FederationInternalAPI) IsServerBackingOff(s spec.ServerName) bool {
	_, err := a.IsBlacklistedOrBackingOff(s)
	return err != nil
}

func (a *FederationInternalAPI) IsBlacklistedOrBackingOff(s spec.ServerName) (*statistics.ServerStatistics, error) {
	stats := a.statistics.ForServer(s)
	if stats.Blacklisted() {
		return stats, &api.FederationClientError{
			Blacklisted: true,
		}
	}

	now := time.Now()
	until := stats.BackoffInfo()
	if until != nil && now.Before(*until) {
		return stats, &api.FederationClientError{
			RetryAfter: time.Until(*until),
		}
	}

	return stats, nil
}

func failBlacklistableError(err error, stats *statistics.ServerStatistics) (until time.Time, blacklisted bool) {
	if err == nil {
		return
	}
	var mxerr gomatrix.HTTPError
	if !errors.As(err, &mxerr) {
		return stats.Failure()
	}
	if mxerr.Code == 401 { //nolint:mnd // invalid signature in X-Matrix header
		return stats.Failure()
	}
	if mxerr.Code >= 500 && mxerr.Code < 600 { // internal server errors
		return stats.Failure()
	}
	return
}

// doFederationRequest makes a federation request, refusing if the server is blacklisted.
// When checkBackoff is true, also refuses if the server is currently in a backoff period.
// Set checkBackoff to false for read-only, user-triggered queries (like room hierarchy)
// where blocking on backoff from unrelated failures is counterproductive.
// Failures still contribute to backoff/blacklist statistics via failBlacklistableError.
func (a *FederationInternalAPI) doFederationRequest(
	s spec.ServerName, request func() (any, error), checkBackoff bool,
) (any, error) {
	stats := a.statistics.ForServer(s)
	if stats.Blacklisted() {
		return nil, &api.FederationClientError{
			Err:         fmt.Sprintf("server %q is blacklisted", s),
			Blacklisted: true,
		}
	}
	if checkBackoff {
		now := time.Now()
		until := stats.BackoffInfo()
		if until != nil && now.Before(*until) {
			return nil, &api.FederationClientError{
				Err:        fmt.Sprintf("server %q is backing off", s),
				RetryAfter: time.Until(*until),
			}
		}
	}

	res, err := request()
	if err != nil {
		failUntil, blacklisted := failBlacklistableError(err, stats)
		now := time.Now()
		var retryAfter time.Duration
		if failUntil.After(now) {
			retryAfter = time.Until(failUntil)
		}
		var httpCode int
		var specErr spec.HTTPError
		var mxErr gomatrix.HTTPError
		if errors.As(err, &specErr) {
			httpCode = specErr.Code
		} else if errors.As(err, &mxErr) {
			httpCode = mxErr.Code
		}
		return res, &api.FederationClientError{
			Err:         err.Error(),
			Blacklisted: blacklisted,
			RetryAfter:  retryAfter,
			Code:        httpCode,
		}
	}
	stats.Success(statistics.SendDirect)
	return res, nil
}
