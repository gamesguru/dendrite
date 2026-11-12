package internal

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/storage"
)

type storedNotaryDatabase struct {
	storage.Database
	keys map[gomatrixserverlib.KeyID]gomatrixserverlib.ServerKeys
}

func (d storedNotaryDatabase) GetNotaryKeysIncludingExpired(_ context.Context, _ spec.ServerName, ids []gomatrixserverlib.KeyID) ([]gomatrixserverlib.ServerKeys, error) {
	var keys []gomatrixserverlib.ServerKeys
	for _, id := range ids {
		if key, ok := d.keys[id]; ok {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (d storedNotaryDatabase) GetNotaryKeys(ctx context.Context, server spec.ServerName, ids []gomatrixserverlib.KeyID) ([]gomatrixserverlib.ServerKeys, error) {
	keys, err := d.GetNotaryKeysIncludingExpired(ctx, server, ids)
	var valid []gomatrixserverlib.ServerKeys
	for _, key := range keys {
		if key.ValidUntilTS.Time().After(time.Now()) {
			valid = append(valid, key)
		}
	}
	return valid, err
}

func TestStoredNotaryKeysMixedValidity(t *testing.T) {
	now := time.Now()
	expired := spec.AsTimestamp(now.Add(-time.Hour))
	future := spec.AsTimestamp(now.Add(time.Hour))
	makeKey := func(id gomatrixserverlib.KeyID, until spec.Timestamp) gomatrixserverlib.ServerKeys {
		return gomatrixserverlib.ServerKeys{ServerKeyFields: gomatrixserverlib.ServerKeyFields{
			ServerName: "remote.test", ValidUntilTS: until,
			VerifyKeys: map[gomatrixserverlib.KeyID]gomatrixserverlib.VerifyKey{id: {}},
		}}
	}
	a := FederationInternalAPI{db: storedNotaryDatabase{keys: map[gomatrixserverlib.KeyID]gomatrixserverlib.ServerKeys{
		"ed25519:zero":      makeKey("ed25519:zero", expired),
		"ed25519:default":   makeKey("ed25519:default", expired),
		"ed25519:future":    makeKey("ed25519:future", future),
		"ed25519:too_short": makeKey("ed25519:too_short", future),
	}}}
	keys, err := a.storedNotaryKeys(t.Context(), &api.QueryServerKeysRequest{
		ServerName: "remote.test",
		KeyIDToCriteria: map[gomatrixserverlib.KeyID]gomatrixserverlib.PublicKeyNotaryQueryCriteria{
			"ed25519:zero":      {MinimumValidUntilTS: 0},
			"ed25519:default":   {MinimumValidUntilTS: spec.AsTimestamp(now)},
			"ed25519:future":    {MinimumValidUntilTS: future},
			"ed25519:too_short": {MinimumValidUntilTS: spec.AsTimestamp(now.Add(2 * time.Hour))},
		},
	})
	require.NoError(t, err)
	var ids []gomatrixserverlib.KeyID
	for _, key := range keys {
		for id := range key.VerifyKeys {
			ids = append(ids, id)
		}
	}
	require.ElementsMatch(t, []gomatrixserverlib.KeyID{"ed25519:zero", "ed25519:future"}, ids)
}
