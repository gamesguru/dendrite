// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fedsenderapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/setup/config"
)

func TestRemoteKeyBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	b := &remoteKeyBackoff{now: func() time.Time { return now }}

	assert.Zero(t, b.remaining("dead"), "an unknown server is not backing off")

	b.failure("dead")
	assert.Equal(t, keyQueryBackoffBase, b.remaining("dead"))
	assert.Zero(t, b.remaining("other"), "backoff is per server")

	now = now.Add(keyQueryBackoffBase)
	assert.Zero(t, b.remaining("dead"), "backoff expires")

	b.failure("dead")
	assert.Equal(t, 2*keyQueryBackoffBase, b.remaining("dead"), "each failure doubles the wait")

	for range 20 {
		now = now.Add(keyQueryBackoffMax)
		b.failure("dead")
	}
	assert.Equal(t, keyQueryBackoffMax, b.remaining("dead"), "the wait is capped")

	b.success("dead")
	assert.Zero(t, b.remaining("dead"), "a success clears the backoff")
}

func TestRemoteKeyBackoffCountsAConcurrentBurstOnce(t *testing.T) {
	now := time.Unix(1000, 0)
	b := &remoteKeyBackoff{now: func() time.Time { return now }}

	// Several queries were in flight when the server stopped answering.
	for range 5 {
		b.failure("dead")
	}
	assert.Equal(t, keyQueryBackoffBase, b.remaining("dead"), "a burst is one failure")

	now = now.Add(keyQueryBackoffBase)
	b.failure("dead")
	assert.Equal(t, 2*keyQueryBackoffBase, b.remaining("dead"), "a failure after the wait doubles it")
}

func TestRemoteKeyBackoffForgetsQuietServers(t *testing.T) {
	now := time.Unix(1000, 0)
	b := &remoteKeyBackoff{now: func() time.Time { return now }}
	b.failure("gone")

	now = now.Add(keyQueryBackoffBase + keyQueryBackoffMax + time.Second)
	b.failure("other")
	assert.NotContains(t, b.servers, "gone")
	assert.Contains(t, b.servers, "other")
}

func TestIsTransientKeyQueryError(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"timeout":             {context.DeadlineExceeded, true},
		"connection error":    {errors.New("dial tcp: i/o timeout"), true},
		"unreachable":         {&fedsenderapi.FederationClientError{Err: "unreachable"}, true},
		"blacklisted":         {&fedsenderapi.FederationClientError{Blacklisted: true, Code: http.StatusForbidden}, true},
		"bad gateway":         {&fedsenderapi.FederationClientError{Code: http.StatusBadGateway}, true},
		"rate limited":        {&fedsenderapi.FederationClientError{Code: http.StatusTooManyRequests}, true},
		"refused request":     {&fedsenderapi.FederationClientError{Code: http.StatusForbidden}, false},
		"not found":           {&fedsenderapi.FederationClientError{Code: http.StatusNotFound}, false},
		"http 502":            {spec.HTTPError{Code: http.StatusBadGateway}, true},
		"http 400":            {spec.HTTPError{Code: http.StatusBadRequest}, false},
		"wrapped bad request": {errors.Join(errors.New("ctx"), spec.HTTPError{Code: http.StatusBadRequest}), false},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, isTransientKeyQueryError(tc.err))
		})
	}
}

type countingKeyClient struct {
	fedsenderapi.KeyserverFederationAPI
	calls atomic.Int32
	err   error
}

func (c *countingKeyClient) QueryKeys(context.Context, spec.ServerName, spec.ServerName, map[string][]string) (fclient.RespQueryKeys, error) {
	c.calls.Add(1)
	return fclient.RespQueryKeys{}, c.err
}

func newKeyQueryAPI(client *countingKeyClient) *UserInternalAPI {
	return &UserInternalAPI{
		FedClient: client,
		Config:    &config.UserAPI{Matrix: &config.Global{}},
	}
}

func TestQueryKeysFromServerBacksOffAfterTimeout(t *testing.T) {
	client := &countingKeyClient{err: context.DeadlineExceeded}
	api := newKeyQueryAPI(client)
	ctx := context.Background()

	_, err := api.queryKeysFromServer(ctx, ctx, "dead", nil)
	require.Error(t, err)
	assert.EqualValues(t, 1, client.calls.Load())

	_, err = api.queryKeysFromServer(ctx, ctx, "dead", nil)
	require.ErrorContains(t, err, "backing off")
	assert.EqualValues(t, 1, client.calls.Load(), "the second query must not reach the server")

	_, err = api.queryKeysFromServer(ctx, ctx, "alive", nil)
	require.Error(t, err)
	assert.EqualValues(t, 2, client.calls.Load(), "backoff is per server")
}

func TestQueryKeysFromServerIgnoresCallerCancellationAndRefusals(t *testing.T) {
	client := &countingKeyClient{err: context.Canceled}
	api := newKeyQueryAPI(client)

	gaveUp, giveUp := context.WithCancelCause(context.Background())
	giveUp(errors.New("client gave up"))
	_, err := api.queryKeysFromServer(gaveUp, gaveUp, "server", nil)
	require.Error(t, err)
	assert.Zero(t, api.keyQueryBackoff.remaining("server"), "a client that gave up says nothing about the server")

	client.err = &fedsenderapi.FederationClientError{Code: http.StatusForbidden}
	_, err = api.queryKeysFromServer(context.Background(), context.Background(), "server", nil)
	require.Error(t, err)
	assert.Zero(t, api.keyQueryBackoff.remaining("server"), "a refused request must not silence the server")

	client.err = nil
	_, err = api.queryKeysFromServer(context.Background(), context.Background(), "server", nil)
	require.NoError(t, err)
}

func TestQueryKeysFromServerConcurrentFailuresBackOffOnce(t *testing.T) {
	client := &countingKeyClient{err: context.DeadlineExceeded}
	api := newKeyQueryAPI(client)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = api.queryKeysFromServer(context.Background(), context.Background(), "dead", nil)
		}()
	}
	wg.Wait()
	assert.LessOrEqual(t, api.keyQueryBackoff.remaining("dead"), keyQueryBackoffBase, "one burst of failures is one backoff step")
}
