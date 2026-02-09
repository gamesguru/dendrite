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
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/dendrite/federationapi/queue"
	"codefloe.com/pat-s/dendrite/federationapi/statistics"
	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/setup/process"
	"codefloe.com/pat-s/dendrite/test"
)

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
