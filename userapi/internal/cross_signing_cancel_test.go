package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
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
			a.crossSigningKeysFromDatabase(ctx, req, res)
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
