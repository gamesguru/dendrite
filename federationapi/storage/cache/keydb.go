package cache

import (
	"context"
	"errors"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
)

// A Database implements gomatrixserverlib.KeyDatabase and is used to store
// the public keys for other matrix servers.
type KeyDatabase struct {
	inner gomatrixserverlib.KeyDatabase
	cache caching.ServerKeyCache
}

func NewKeyDatabase(inner gomatrixserverlib.KeyDatabase, cache caching.ServerKeyCache) (*KeyDatabase, error) {
	if inner == nil {
		return nil, errors.New("inner database can't be nil")
	}
	if cache == nil {
		return nil, errors.New("cache can't be nil")
	}
	return &KeyDatabase{
		inner: inner,
		cache: cache,
	}, nil
}

// FetcherName implements KeyFetcher.
func (d KeyDatabase) FetcherName() string {
	return "InMemoryKeyCache"
}

// FetchKeys implements gomatrixserverlib.KeyDatabase.
func (d *KeyDatabase) FetchKeys(
	ctx context.Context,
	requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	results := make(map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult)
	for req, ts := range requests {
		if res, cached := d.cache.GetServerKey(req, ts); cached {
			results[req] = res
			delete(requests, req)
		}
	}
	// Don't bother hitting the DB if we got everything from cache.
	if len(requests) == 0 {
		return results, nil
	}
	fromDB, err := d.inner.FetchKeys(ctx, requests)
	if err != nil {
		return results, err
	}
	for req, res := range fromDB {
		d.cache.StoreServerKey(req, res)
		// Like Synapse (MSC4499), judge a live key against the timestamp of
		// the event being verified. If its validity window ended before then,
		// withhold it so the key ring asks the origin again: the window may
		// have been extended since we cached it. Expired keys are final.
		if res.ExpiredTS == gomatrixserverlib.PublicKeyNotExpired &&
			!res.WasValidAt(requests[req], gomatrixserverlib.StrictValiditySignatureCheck) {
			continue
		}
		results[req] = res
	}
	return results, nil
}

// StoreKeys implements gomatrixserverlib.KeyDatabase.
func (d *KeyDatabase) StoreKeys(
	ctx context.Context,
	keyMap map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult,
) error {
	if err := d.inner.StoreKeys(ctx, keyMap); err != nil {
		return err
	}
	// The persistent database may enforce first-seen key bindings and ignore
	// a later response which reuses a key ID for a different public key. Cache
	// the authoritative value from storage so the in-memory cache cannot
	// temporarily override that binding.
	requests := make(map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp, len(keyMap))
	for req := range keyMap {
		requests[req] = 0
	}
	stored, err := d.inner.FetchKeys(ctx, requests)
	if err != nil {
		return err
	}
	for req, res := range stored {
		d.cache.StoreServerKey(req, res)
	}
	return nil
}
