package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage"
	"codefloe.com/pat-s/zendrite/userapi/types"
)

type cancellationKeyDB struct {
	storage.KeyDatabase
	keyCalls, sigCalls int
	keyErr, sigErr     error
	afterKeys          func()
}

func (d *cancellationKeyDB) CrossSigningKeysForUser(_ context.Context, _ string) (map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey, error) {
	d.keyCalls++
	if d.afterKeys != nil {
		d.afterKeys()
	}
	return map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{fclient.CrossSigningKeyPurposeMaster: {}}, d.keyErr
}

func (d *cancellationKeyDB) CrossSigningSigsForTarget(_ context.Context, _, _ string, _ gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	d.sigCalls++
	return nil, d.sigErr
}

func TestCrossSigningCancellation(t *testing.T) {
	for _, tc := range []struct {
		name             string
		before, during   bool
		keyErr, sigErr   error
		keys, sigs, logs int
	}{
		{name: "already canceled", before: true},
		{name: "canceled after keys", during: true, keys: 1},
		{name: "wrapped key cancellation", keyErr: fmt.Errorf("query: %w", context.Canceled), keys: 1},
		{name: "key deadline", keyErr: context.DeadlineExceeded, keys: 1},
		{name: "signature cancellation", sigErr: context.Canceled, keys: 1, sigs: 1},
		{name: "wrapped signature deadline", sigErr: fmt.Errorf("query: %w", context.DeadlineExceeded), keys: 1, sigs: 1},
		{name: "genuine key failures", keyErr: errors.New("database failed"), keys: 3, logs: 3},
		{name: "genuine signature failures", sigErr: errors.New("database failed"), keys: 3, sigs: 3, logs: 3},
		{name: "success", keys: 3, sigs: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := logrustest.NewGlobal()
			defer hook.Reset()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			db := &cancellationKeyDB{keyErr: tc.keyErr, sigErr: tc.sigErr}
			if tc.before {
				cancel(context.Canceled)
			}
			if tc.during {
				db.afterKeys = func() { cancel(context.Canceled) }
			}
			a := &UserInternalAPI{KeyDatabase: db}
			req := &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil, "@b:test": nil, "@c:test": nil}}
			res := &api.QueryKeysResponse{MasterKeys: make(map[string]fclient.CrossSigningKey)}
			a.crossSigningKeysFromDatabase(ctx, req, res, nil)
			require.Equal(t, tc.keys, db.keyCalls)
			require.Equal(t, tc.sigs, db.sigCalls)
			logs := 0
			for _, e := range hook.AllEntries() {
				if e.Level == logrus.ErrorLevel {
					logs++
				}
			}
			require.Equal(t, tc.logs, logs)
			if tc.name == "success" {
				require.Len(t, res.MasterKeys, 3)
			}
		})
	}
}

func TestCrossSigningKeysFromDatabaseKeepsFederationKeys(t *testing.T) {
	db := &cancellationKeyDB{}
	a := &UserInternalAPI{KeyDatabase: db}
	req := &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil}}
	existing := fclient.CrossSigningKey{
		Usage: []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:  map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:federation": {1, 2, 3}},
	}
	res := &api.QueryKeysResponse{
		MasterKeys:      map[string]fclient.CrossSigningKey{"@a:test": existing},
		SelfSigningKeys: map[string]fclient.CrossSigningKey{},
		UserSigningKeys: map[string]fclient.CrossSigningKey{},
	}

	a.crossSigningKeysFromDatabase(t.Context(), req, res, nil)

	require.Equal(t, existing, res.MasterKeys["@a:test"],
		"a master key supplied by federation must not be replaced by the database fallback")
}

type rotatingCrossSigningKeyDB struct {
	storage.KeyDatabase
	reads []map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey
}

func (d *rotatingCrossSigningKeyDB) CrossSigningKeysForUser(context.Context, string) (map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey, error) {
	keys := d.reads[0]
	d.reads = d.reads[1:]
	return keys, nil
}

func (d *rotatingCrossSigningKeyDB) CrossSigningSigsForTarget(context.Context, string, string, gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	return nil, nil
}

func TestCrossSigningRefreshReplacesDatabaseGeneration(t *testing.T) {
	key := func(purpose fclient.CrossSigningKeyPurpose, id string) fclient.CrossSigningKey {
		return fclient.CrossSigningKey{
			Usage: []fclient.CrossSigningKeyPurpose{purpose},
			Keys:  map[gomatrixserverlib.KeyID]spec.Base64Bytes{gomatrixserverlib.KeyID("ed25519:" + id): {1}},
		}
	}
	db := &rotatingCrossSigningKeyDB{reads: []map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{
		{fclient.CrossSigningKeyPurposeMaster: key(fclient.CrossSigningKeyPurposeMaster, "master-a")},
		{
			fclient.CrossSigningKeyPurposeMaster:      key(fclient.CrossSigningKeyPurposeMaster, "master-b"),
			fclient.CrossSigningKeyPurposeSelfSigning: key(fclient.CrossSigningKeyPurposeSelfSigning, "self-b"),
		},
	}}
	a := &UserInternalAPI{KeyDatabase: db}
	req := &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil}}
	res := &api.QueryKeysResponse{
		MasterKeys:      make(map[string]fclient.CrossSigningKey),
		SelfSigningKeys: make(map[string]fclient.CrossSigningKey),
		UserSigningKeys: make(map[string]fclient.CrossSigningKey),
	}
	a.crossSigningKeysFromDatabase(t.Context(), req, res, nil)
	dbLoaded := crossSigningEntries(res)
	a.crossSigningKeysFromDatabase(t.Context(), req, res, func(userID string, purpose fclient.CrossSigningKeyPurpose) bool {
		_, loaded := dbLoaded[userID][purpose]
		return loaded
	})

	_, masterB := res.MasterKeys["@a:test"].Keys["ed25519:master-b"]
	_, selfB := res.SelfSigningKeys["@a:test"].Keys["ed25519:self-b"]
	require.True(t, masterB, "the refreshed master key replaces the stale database fallback")
	require.True(t, selfB, "the refreshed self-signing key is returned from the same database generation")
}

type refreshingCrossSigningDB struct {
	storage.KeyDatabase
	calls int
}

func (d *refreshingCrossSigningDB) CrossSigningKeysForUser(_ context.Context, userID string) (map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey, error) {
	if userID != "@a:test" {
		return nil, nil
	}
	d.calls++
	generation := "A"
	if d.calls > 1 {
		generation = "B"
	}
	key := func(purpose fclient.CrossSigningKeyPurpose) fclient.CrossSigningKey {
		return fclient.CrossSigningKey{
			Usage: []fclient.CrossSigningKeyPurpose{purpose},
			Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{
				gomatrixserverlib.KeyID("ed25519:" + generation): {generation[0]},
			},
		}
	}
	return map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{
		fclient.CrossSigningKeyPurposeMaster:      key(fclient.CrossSigningKeyPurposeMaster),
		fclient.CrossSigningKeyPurposeSelfSigning: key(fclient.CrossSigningKeyPurposeSelfSigning),
	}, nil
}

func (d *refreshingCrossSigningDB) CrossSigningSigsForTarget(_ context.Context, _, _ string, _ gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	return nil, nil
}

// A device-list refresh can replace the user's master and self-signing keys
// while a /keys/query is in flight. The second database pass must replace the
// earlier database generation so the response does not pair an old master with
// a new self-signing key, while a key federation supplied stays in place.
func TestCrossSigningKeysRefreshReplacesEarlierDatabaseGeneration(t *testing.T) {
	db := &refreshingCrossSigningDB{}
	a := &UserInternalAPI{KeyDatabase: db}
	req := &api.QueryKeysRequest{UserToDevices: map[string][]string{
		"@a:test": nil, "@fed:test": nil,
	}}
	res := &api.QueryKeysResponse{
		MasterKeys:      map[string]fclient.CrossSigningKey{},
		SelfSigningKeys: map[string]fclient.CrossSigningKey{},
		UserSigningKeys: map[string]fclient.CrossSigningKey{},
	}

	a.crossSigningKeysFromDatabase(t.Context(), req, res, nil)
	require.Equal(t, gomatrixserverlib.KeyID("ed25519:A"), onlyKeyID(t, res.MasterKeys["@a:test"]))
	require.Equal(t, gomatrixserverlib.KeyID("ed25519:A"), onlyKeyID(t, res.SelfSigningKeys["@a:test"]))
	dbLoaded := crossSigningEntries(res)

	federation := fclient.CrossSigningKey{
		Usage: []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:  map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:federation": {1}},
	}
	res.MasterKeys["@fed:test"] = federation

	a.crossSigningKeysFromDatabase(t.Context(), req, res, func(targetUserID string, keyType fclient.CrossSigningKeyPurpose) bool {
		_, ok := dbLoaded[targetUserID][keyType]
		return ok
	})

	require.Equal(t, gomatrixserverlib.KeyID("ed25519:B"), onlyKeyID(t, res.MasterKeys["@a:test"]))
	require.Equal(t, gomatrixserverlib.KeyID("ed25519:B"), onlyKeyID(t, res.SelfSigningKeys["@a:test"]))
	require.Equal(t, federation, res.MasterKeys["@fed:test"], "a federation key must not be replaced")
}

func onlyKeyID(t *testing.T, key fclient.CrossSigningKey) gomatrixserverlib.KeyID {
	t.Helper()
	require.Len(t, key.Keys, 1)
	for id := range key.Keys {
		return id
	}
	return ""
}

func TestQueryKeysAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)
	// Nil dependencies ensure no work is attempted for the abandoned request.
	a := &UserInternalAPI{}
	res := &api.QueryKeysResponse{}
	a.QueryKeys(ctx, &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil}}, res)
	require.NotNil(t, res.MasterKeys)
	require.Empty(t, res.MasterKeys)
}

type stagedRefreshCrossSigningDB struct {
	storage.KeyDatabase
	reads  []map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey
	sigErr gomatrixserverlib.KeyID
}

func (d *stagedRefreshCrossSigningDB) CrossSigningKeysForUser(context.Context, string) (map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey, error) {
	keys := d.reads[0]
	d.reads = d.reads[1:]
	return keys, nil
}

func (d *stagedRefreshCrossSigningDB) CrossSigningSigsForTarget(_ context.Context, _, _ string, keyID gomatrixserverlib.KeyID) (types.CrossSigningSigMap, error) {
	if keyID == d.sigErr {
		return nil, errors.New("database failed")
	}
	return nil, nil
}

func crossSigningGenerationKey(purpose fclient.CrossSigningKeyPurpose, generation string) fclient.CrossSigningKey {
	return fclient.CrossSigningKey{
		Usage: []fclient.CrossSigningKeyPurpose{purpose},
		Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{
			gomatrixserverlib.KeyID("ed25519:" + generation + "-" + string(purpose)): {generation[0]},
		},
	}
}

// A failed signature read during the reload must leave the whole previous
// generation in place rather than only the purposes whose reads succeeded.
func TestCrossSigningRefreshFailureKeepsPreviousGeneration(t *testing.T) {
	db := &stagedRefreshCrossSigningDB{
		reads: []map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{
			{
				fclient.CrossSigningKeyPurposeMaster:      crossSigningGenerationKey(fclient.CrossSigningKeyPurposeMaster, "a"),
				fclient.CrossSigningKeyPurposeSelfSigning: crossSigningGenerationKey(fclient.CrossSigningKeyPurposeSelfSigning, "a"),
			},
			{
				fclient.CrossSigningKeyPurposeMaster:      crossSigningGenerationKey(fclient.CrossSigningKeyPurposeMaster, "b"),
				fclient.CrossSigningKeyPurposeSelfSigning: crossSigningGenerationKey(fclient.CrossSigningKeyPurposeSelfSigning, "b"),
			},
		},
		sigErr: "ed25519:b-self_signing",
	}
	a := &UserInternalAPI{KeyDatabase: db}
	req := &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil}}
	res := &api.QueryKeysResponse{
		MasterKeys:      make(map[string]fclient.CrossSigningKey),
		SelfSigningKeys: make(map[string]fclient.CrossSigningKey),
		UserSigningKeys: make(map[string]fclient.CrossSigningKey),
	}
	a.crossSigningKeysFromDatabase(t.Context(), req, res, nil)
	dbLoaded := crossSigningEntries(res)
	a.crossSigningKeysFromDatabase(t.Context(), req, res,
		func(userID string, purpose fclient.CrossSigningKeyPurpose) bool {
			_, loaded := dbLoaded[userID][purpose]
			return loaded
		},
		func(userID string, purpose fclient.CrossSigningKeyPurpose) bool {
			_, loaded := dbLoaded[userID][purpose]
			return loaded
		},
	)

	require.Equal(t, gomatrixserverlib.KeyID("ed25519:a-master"), onlyKeyID(t, res.MasterKeys["@a:test"]),
		"the master key must not advance without the self-signing key")
	require.Equal(t, gomatrixserverlib.KeyID("ed25519:a-self_signing"), onlyKeyID(t, res.SelfSigningKeys["@a:test"]),
		"the self-signing key must stay on the previous generation")
}

// A full refresh that supplies one purpose but omits another must drop the
// omitted purpose instead of leaving the previous generation paired with the
// refreshed key.
func TestCrossSigningRefreshDropsOmittedPurpose(t *testing.T) {
	db := &stagedRefreshCrossSigningDB{
		reads: []map[fclient.CrossSigningKeyPurpose]fclient.CrossSigningKey{
			{
				fclient.CrossSigningKeyPurposeMaster:      crossSigningGenerationKey(fclient.CrossSigningKeyPurposeMaster, "a"),
				fclient.CrossSigningKeyPurposeSelfSigning: crossSigningGenerationKey(fclient.CrossSigningKeyPurposeSelfSigning, "a"),
			},
			{
				fclient.CrossSigningKeyPurposeMaster: crossSigningGenerationKey(fclient.CrossSigningKeyPurposeMaster, "b"),
			},
		},
	}
	a := &UserInternalAPI{KeyDatabase: db}
	req := &api.QueryKeysRequest{UserToDevices: map[string][]string{"@a:test": nil}}
	res := &api.QueryKeysResponse{
		MasterKeys:      make(map[string]fclient.CrossSigningKey),
		SelfSigningKeys: make(map[string]fclient.CrossSigningKey),
		UserSigningKeys: make(map[string]fclient.CrossSigningKey),
	}
	a.crossSigningKeysFromDatabase(t.Context(), req, res, nil)
	dbLoaded := crossSigningEntries(res)
	authoritative := func(userID string, purpose fclient.CrossSigningKeyPurpose) bool {
		_, loaded := dbLoaded[userID][purpose]
		return loaded
	}
	a.crossSigningKeysFromDatabase(t.Context(), req, res, authoritative, authoritative)

	require.Equal(t, gomatrixserverlib.KeyID("ed25519:b-master"), onlyKeyID(t, res.MasterKeys["@a:test"]))
	require.NotContains(t, res.SelfSigningKeys, "@a:test",
		"an omitted self-signing key must not survive next to the refreshed master key")
}
