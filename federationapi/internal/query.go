package internal

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/federationapi/api"
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

func (a *FederationInternalAPI) fetchServerKeysDirectly(ctx context.Context, serverName spec.ServerName) (*gomatrixserverlib.ServerKeys, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30) //nolint:mnd
	defer cancel()
	ires, err := a.doFederationRequest(serverName, func() (any, error) { //nolint:contextcheck
		return a.federation.GetServerKeys(ctx, serverName)
	}, true)
	if err != nil {
		return nil, err
	}
	sks, ok := ires.(gomatrixserverlib.ServerKeys)
	if !ok {
		return nil, fmt.Errorf("unexpected response type for GetServerKeys")
	}
	return &sks, nil
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
	// First seen wins (MSC4499): don't serve or store a response that binds a
	// key ID we already hold to a different key.
	held, dbErr := a.db.GetNotaryKeys(ctx, req.ServerName, nil)
	if dbErr == nil {
		if keyID, collides := collidingKeyID(*serverKeys, held); collides {
			util.GetLogger(ctx).WithField("server", req.ServerName).Warnf(
				"notary: server returned a different key for held key ID %s, returning held keys", keyID,
			)
			res.ServerKeys = held
			return nil
		}
	}
	// cache it!
	if err = a.db.UpdateNotaryKeys(context.Background(), req.ServerName, *serverKeys); err != nil { //nolint:contextcheck
		// non-fatal, still return the response
		util.GetLogger(ctx).WithError(err).Warn("failed to UpdateNotaryKeys")
	}
	res.ServerKeys = []gomatrixserverlib.ServerKeys{*serverKeys}
	return nil
}

// collidingKeyID reports a key ID in fetched that held already binds to a
// different key.
func collidingKeyID(fetched gomatrixserverlib.ServerKeys, held []gomatrixserverlib.ServerKeys) (gomatrixserverlib.KeyID, bool) {
	heldKeys := map[gomatrixserverlib.KeyID]spec.Base64Bytes{}
	for _, sk := range held {
		for id, vk := range sk.VerifyKeys {
			heldKeys[id] = vk.Key
		}
		for id, vk := range sk.OldVerifyKeys {
			heldKeys[id] = vk.Key
		}
	}
	for id, vk := range fetched.VerifyKeys {
		if key, ok := heldKeys[id]; ok && !bytes.Equal(key, vk.Key) {
			return id, true
		}
	}
	for id, vk := range fetched.OldVerifyKeys {
		if key, ok := heldKeys[id]; ok && !bytes.Equal(key, vk.Key) {
			return id, true
		}
	}
	return "", false
}
