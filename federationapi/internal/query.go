package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/federationapi/api"
)

const (
	// The maxVerifyKeys constant is the MSC4499 per-response ceiling on active keys.
	maxVerifyKeys = 50
	// The maxExpiredTSSkew constant is the MSC4499 allowed clock skew for expired_ts.
	maxExpiredTSSkew = 5 * time.Minute
)

// QueryJoinedHostServerNamesInRoom implements api.FederationInternalAPI.
func (f *FederationInternalAPI) QueryJoinedHostServerNamesInRoom(
	ctx context.Context,
	request *api.QueryJoinedHostServerNamesInRoomRequest,
	response *api.QueryJoinedHostServerNamesInRoomResponse,
) (err error) {
	joinedHosts, err := f.db.GetJoinedHostsForRooms(ctx, []string{request.RoomID}, request.ExcludeSelf, request.ExcludeBlacklisted)
	if err != nil {
		return
	}
	response.ServerNames = joinedHosts

	return
}

const serverKeyFetchFailureBackoff = 2 * time.Second

func (a *FederationInternalAPI) fetchServerKeysDirectly(ctx context.Context, serverName spec.ServerName) (*gomatrixserverlib.ServerKeys, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30) //nolint:mnd
	defer cancel()

	// Do not hammer a server which has just failed: notary clients retry
	// immediately, and a failed key fetch is not useful to repeat (MSC4499 §3).
	if until, ok := a.serverKeyFetchBackoffUntil(serverName); ok && time.Now().Before(until) {
		return nil, fmt.Errorf("server key fetch for %s is backing off until %s", serverName, until.Format(time.RFC3339))
	}

	// Coalesce concurrent direct fetches for the same server. Every request
	// fetches the full key document anyway, so a single upstream request can
	// satisfy all of them (MSC4499 §3).
	res, err, _ := a.serverKeyFetches.Do(string(serverName), func() (any, error) {
		return a.fetchServerKeysDirectlyUncoalesced(ctx, serverName)
	})
	if err != nil {
		a.recordServerKeyFetchFailure(serverName)
		return nil, err
	}
	a.clearServerKeyFetchFailure(serverName)
	sks, ok := res.(*gomatrixserverlib.ServerKeys)
	if !ok {
		return nil, fmt.Errorf("unexpected response type for GetServerKeys")
	}
	return sks, nil
}

func (a *FederationInternalAPI) serverKeyFetchBackoffUntil(serverName spec.ServerName) (time.Time, bool) {
	a.serverKeyFetchBackoffMu.Lock()
	defer a.serverKeyFetchBackoffMu.Unlock()
	until, ok := a.serverKeyFetchBackoff[serverName]
	return until, ok
}

func (a *FederationInternalAPI) recordServerKeyFetchFailure(serverName spec.ServerName) {
	a.serverKeyFetchBackoffMu.Lock()
	defer a.serverKeyFetchBackoffMu.Unlock()
	if a.serverKeyFetchBackoff == nil {
		a.serverKeyFetchBackoff = make(map[spec.ServerName]time.Time)
	}
	a.serverKeyFetchBackoff[serverName] = time.Now().Add(serverKeyFetchFailureBackoff)
}

func (a *FederationInternalAPI) clearServerKeyFetchFailure(serverName spec.ServerName) {
	a.serverKeyFetchBackoffMu.Lock()
	defer a.serverKeyFetchBackoffMu.Unlock()
	delete(a.serverKeyFetchBackoff, serverName)
}

func (a *FederationInternalAPI) fetchServerKeysDirectlyUncoalesced(
	ctx context.Context, serverName spec.ServerName,
) (*gomatrixserverlib.ServerKeys, error) {
	// Fetch the raw document so that duplicate JSON keys can be rejected
	// before the decoder silently collapses them.
	url := "matrix://" + string(serverName) + "/_matrix/key/v2/server"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err = a.federation.DoRequestAndParseResponse(ctx, req, &raw); err != nil {
		return nil, err
	}
	keys, err := validateServerKeyJSON(raw, time.Now())
	if err != nil {
		return nil, err
	}
	return &keys, nil
}

// validateServerKeyJSON applies the MSC4499 structural constraints that
// encoding/json cannot express: duplicate object keys and malformed key maps.
func validateServerKeyJSON(raw json.RawMessage, now time.Time) (gomatrixserverlib.ServerKeys, error) {
	var keys gomatrixserverlib.ServerKeys
	if err := checkDuplicateJSONKeys(raw); err != nil {
		return keys, err
	}
	if err := json.Unmarshal(raw, &keys); err != nil {
		return keys, err
	}
	if len(keys.VerifyKeys) > maxVerifyKeys {
		return keys, fmt.Errorf("server key response has %d verify_keys, exceeding the %d-key ceiling", len(keys.VerifyKeys), maxVerifyKeys)
	}

	// A key ID is a permanent identity. It must not be bound to different key
	// material in verify_keys and old_verify_keys in the same response.
	for keyID, verifyKey := range keys.VerifyKeys {
		if oldKey, ok := keys.OldVerifyKeys[keyID]; ok && !bytes.Equal(verifyKey.Key, oldKey.Key) {
			return keys, fmt.Errorf("server key response binds key ID %q to conflicting key material", keyID)
		}
	}

	// A retired key cannot expire in the future. Drop only that entry; the rest
	// of the response remains usable.
	for keyID, oldKey := range keys.OldVerifyKeys {
		if oldKey.ExpiredTS != gomatrixserverlib.PublicKeyNotExpired &&
			oldKey.ExpiredTS.Time().After(now.Add(maxExpiredTSSkew)) {
			delete(keys.OldVerifyKeys, keyID)
		}
	}
	return keys, nil
}

// checkDuplicateJSONKeys walks a JSON document and rejects duplicate keys in
// any object, at any nesting depth.
func checkDuplicateJSONKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	return walkJSONValue(dec)
}

func walkJSONValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			keyToken, keyErr := dec.Token()
			if keyErr != nil {
				return keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("invalid JSON object key")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err = walkJSONValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	case '[':
		for dec.More() {
			if err = walkJSONValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
		return err
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}

func (a *FederationInternalAPI) fetchServerKeysFromCache(
	ctx context.Context, req *api.QueryServerKeysRequest,
) ([]gomatrixserverlib.ServerKeys, error) {
	var results []gomatrixserverlib.ServerKeys

	// We got a request for _all_ server keys, return them.
	if len(req.KeyIDToCriteria) == 0 {
		serverKeysResponses, _ := a.db.GetNotaryKeys(ctx, req.ServerName, []gomatrixserverlib.KeyID{})
		if len(serverKeysResponses) == 0 {
			return nil, fmt.Errorf("failed to find server key response for server %s", req.ServerName)
		}
		return serverKeysResponses, nil
	}
	for keyID, criteria := range req.KeyIDToCriteria {
		serverKeysResponses, _ := a.db.GetNotaryKeys(ctx, req.ServerName, []gomatrixserverlib.KeyID{keyID})
		if len(serverKeysResponses) == 0 {
			return nil, fmt.Errorf("failed to find server key response for key ID %s", keyID)
		}
		// we should only get 1 result as we only gave 1 key ID
		sk := serverKeysResponses[0]
		util.GetLogger(ctx).Infof("fetchServerKeysFromCache: minvalid:%v  keys: %+v", criteria.MinimumValidUntilTS, sk)
		if criteria.MinimumValidUntilTS != 0 {
			// check if it's still valid. if they have the same value that's also valid
			if sk.ValidUntilTS < criteria.MinimumValidUntilTS {
				return nil, fmt.Errorf(
					"found server response for key ID %s but it is no longer valid, min: %v valid_until: %v",
					keyID, criteria.MinimumValidUntilTS, sk.ValidUntilTS,
				)
			}
		}
		results = append(results, sk)
	}
	return results, nil
}

func (a *FederationInternalAPI) QueryServerKeys(
	ctx context.Context, req *api.QueryServerKeysRequest, res *api.QueryServerKeysResponse,
) error {
	// attempt to satisfy the entire request from the cache first
	results, err := a.fetchServerKeysFromCache(ctx, req)
	if err == nil {
		// satisfied entirely from cache, return it
		res.ServerKeys = results
		return nil
	}
	util.GetLogger(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to satisfy keys request entirely from cache, hitting direct")

	serverKeys, err := a.fetchServerKeysDirectly(ctx, req.ServerName)
	if err != nil {
		// try to load as much as we can from the cache in a best effort basis
		util.GetLogger(ctx).WithField("server", req.ServerName).WithError(err).Warn("notary: failed to ask server for keys, returning best effort keys")
		serverKeysResponses, dbErr := a.db.GetNotaryKeys(ctx, req.ServerName, req.KeyIDs())
		if dbErr != nil {
			return fmt.Errorf("notary: server returned %w, and db returned %w", err, dbErr)
		}
		res.ServerKeys = serverKeysResponses
		return nil
	}
	// cache it!
	if err = a.db.UpdateNotaryKeys(context.Background(), req.ServerName, *serverKeys); err != nil { //nolint:contextcheck
		// non-fatal, still return the response
		util.GetLogger(ctx).WithError(err).Warn("failed to UpdateNotaryKeys")
		res.ServerKeys = []gomatrixserverlib.ServerKeys{*serverKeys}
		return nil
	}
	// Return the database's selected responses. In particular, this preserves
	// the first-seen binding when a remote reuses a key ID for another key.
	res.ServerKeys, err = a.db.GetNotaryKeys(ctx, req.ServerName, req.KeyIDs())
	if err != nil {
		return fmt.Errorf("failed to read selected notary keys: %w", err)
	}
	return nil
}
