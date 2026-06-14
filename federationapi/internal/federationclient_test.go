// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// TestFailBlacklistableError verifies that only transport-level failures
// (connection errors, timeouts, malformed responses, 401 signature failures
// and 5xx) contribute to a destination's backoff, while well-formed 4xx
// application responses (e.g. 403 M_FORBIDDEN from /backfill) do not.
func TestFailBlacklistableError(t *testing.T) {
	newStats := func() *statistics.ServerStatistics {
		testDB := test.NewInMemoryFederationDatabase()
		stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
		return stats.ForServer("matrix.org")
	}

	cases := []struct {
		name        string
		err         error
		wantBackoff bool
	}{
		{"nil error", nil, false},
		{"403 spec M_FORBIDDEN", spec.HTTPError{Code: 403, WrappedError: spec.RespError{ErrCode: "M_FORBIDDEN", Err: "Host not in room."}}, false},
		{"404 spec not found", spec.HTTPError{Code: 404}, false},
		{"403 gomatrix", gomatrix.HTTPError{Code: 403}, false},
		{"401 spec signature failure", spec.HTTPError{Code: 401}, true},
		{"500 spec server error", spec.HTTPError{Code: 500}, true},
		{"502 spec bad gateway", spec.HTTPError{Code: 502}, true},
		{"transport error", fmt.Errorf("connection refused"), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats := newStats()
			until, _ := failBlacklistableError(tc.err, stats)
			assert.Equal(t, tc.wantBackoff, !until.IsZero(),
				"backoff state mismatch for %s", tc.name)
			assert.Equal(t, tc.wantBackoff, stats.BackoffInfo() != nil,
				"persisted backoff mismatch for %s", tc.name)
		})
	}
}

const (
	FailuresUntilAssumedOffline = 3
	FailuresUntilBlacklist      = 8
)

func (t *testFedClient) QueryKeys(ctx context.Context, origin, s spec.ServerName, keys map[string][]string) (fclient.RespQueryKeys, error) {
	t.queryKeysCalled = true
	if t.shouldFail {
		return fclient.RespQueryKeys{}, fmt.Errorf("Failure")
	}
	return fclient.RespQueryKeys{}, nil
}

func (t *testFedClient) ClaimKeys(ctx context.Context, origin, s spec.ServerName, oneTimeKeys map[string]map[string]string) (fclient.RespClaimKeys, error) {
	t.claimKeysCalled = true
	if t.shouldFail {
		return fclient.RespClaimKeys{}, fmt.Errorf("Failure")
	}
	return fclient.RespClaimKeys{}, nil
}

func TestFederationClientQueryKeys(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		testDB, process.NewProcessContext(),
		false,
		cfg.Matrix.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.QueryKeys(context.Background(), "origin", "server", nil)
	assert.Nil(t, err)
	assert.True(t, fedClient.queryKeysCalled)
}

func TestFederationClientQueryKeysBlacklisted(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()
	err := testDB.AddServerToBlacklist("server")
	assert.NoError(t, err)

	cfg := config.FederationAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		testDB, process.NewProcessContext(),
		false,
		cfg.Matrix.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err = fedapi.QueryKeys(context.Background(), "origin", "server", nil)
	assert.NotNil(t, err)
	assert.False(t, fedClient.queryKeysCalled)
}

func TestFederationClientQueryKeysFailure(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{shouldFail: true}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		testDB, process.NewProcessContext(),
		false,
		cfg.Matrix.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.QueryKeys(context.Background(), "origin", "server", nil)
	assert.NotNil(t, err)
	assert.True(t, fedClient.queryKeysCalled)
}

func TestFederationClientClaimKeys(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()

	cfg := config.FederationAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		testDB, process.NewProcessContext(),
		false,
		cfg.Matrix.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.ClaimKeys(context.Background(), "origin", "server", nil)
	assert.Nil(t, err)
	assert.True(t, fedClient.claimKeysCalled)
}

func TestFederationClientClaimKeysBlacklisted(t *testing.T) {
	testDB := test.NewInMemoryFederationDatabase()
	_ = testDB.AddServerToBlacklist("server")

	cfg := config.FederationAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "server",
			},
		},
	}
	fedClient := &testFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, false)
	queues := queue.NewOutgoingQueues(
		testDB, process.NewProcessContext(),
		false,
		cfg.Matrix.ServerName, fedClient, &stats,
		nil,
	)
	fedapi := FederationInternalAPI{
		db:         testDB,
		cfg:        &cfg,
		statistics: &stats,
		federation: fedClient,
		queues:     queues,
	}
	_, err := fedapi.ClaimKeys(context.Background(), "origin", "server", nil)
	assert.NotNil(t, err)
	assert.False(t, fedClient.claimKeysCalled)
}
