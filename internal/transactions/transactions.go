// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package transactions

import (
	"context"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/setup/process"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// DefaultCleanupPeriod represents the default time duration after which cacheCleanService runs.
const DefaultCleanupPeriod time.Duration = 30 * time.Minute

type txnsMap map[CacheKey]*util.JSONResponse

// CacheKey is the type for the key in a transactions cache.
// The spec scopes transaction IDs to a device and an endpoint (MSC3970),
// so a refreshed access token keeps the transactions of its device.
type CacheKey struct {
	Scope    string
	TxnID    string
	Endpoint string
}

// scope returns the part of the cache key that identifies who sent a request.
// Application services share one scope per application service, whichever user
// they masquerade as, as their as_token did before transactions were scoped to devices.
func scope(device *userapi.Device) string {
	if device.AppserviceID != "" {
		return "as\x00" + device.AppserviceID
	}
	return "device\x00" + device.UserID + "\x00" + device.ID
}

// Cache represents a temporary store for response entries.
// Entries are evicted after a certain period, defined by cleanupPeriod.
// This works by keeping two maps of entries, and cycling the maps after the cleanupPeriod.
type Cache struct {
	sync.RWMutex
	txnsMaps      [2]txnsMap
	cleanupPeriod time.Duration
}

// New is a wrapper which calls NewWithCleanupPeriod with DefaultCleanupPeriod as argument.
func New(contexts ...context.Context) *Cache {
	return NewWithCleanupPeriod(DefaultCleanupPeriod, contexts...)
}

// NewWithCleanupPeriod creates a new Cache object, starts cacheCleanService.
// Takes cleanupPeriod as argument.
// Returns a reference to newly created Cache.
func NewWithCleanupPeriod(cleanupPeriod time.Duration, contexts ...context.Context) *Cache {
	ctx := context.Background()
	if len(contexts) > 0 {
		ctx = contexts[0]
	}
	t := Cache{txnsMaps: [2]txnsMap{make(txnsMap), make(txnsMap)}}
	t.cleanupPeriod = cleanupPeriod

	// Start clean service as the Cache is created
	process.Go(ctx, func() { cacheCleanService(ctx, &t) })
	return &t
}

// FetchTransaction looks up an entry for the (device, txnID, req.URL) tuple in Cache.
// Looks in both the txnMaps.
// Returns (JSON response, true) if txnID is found, else the returned bool is false.
func (t *Cache) FetchTransaction(device *userapi.Device, txnID string, u *url.URL) (*util.JSONResponse, bool) {
	t.RLock()
	defer t.RUnlock()
	for _, txns := range t.txnsMaps {
		res, ok := txns[CacheKey{scope(device), txnID, filepath.Dir(u.Path)}]
		if ok {
			return res, true
		}
	}
	return nil, false
}

// AddTransaction adds an entry for the (device, txnID, req.URL) tuple in Cache.
// Adds to the front txnMap.
func (t *Cache) AddTransaction(device *userapi.Device, txnID string, u *url.URL, res *util.JSONResponse) {
	t.Lock()
	defer t.Unlock()
	t.txnsMaps[0][CacheKey{scope(device), txnID, filepath.Dir(u.Path)}] = res
}

// cacheCleanService is responsible for cleaning up entries after cleanupPeriod.
// It guarantees that an entry will be present in cache for at least cleanupPeriod & at most 2 * cleanupPeriod.
// This cycles the txnMaps forward, i.e. back map is assigned the front and front is assigned an empty map.
func cacheCleanService(ctx context.Context, t *Cache) {
	for process.Sleep(ctx, t.cleanupPeriod) {
		t.Lock()
		t.txnsMaps[1] = t.txnsMaps[0]
		t.txnsMaps[0] = make(txnsMap)
		t.Unlock()
	}
}
