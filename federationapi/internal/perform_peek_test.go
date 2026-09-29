package internal

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/federationapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

// outboundPeekDB holds a single outbound peek.
type outboundPeekDB struct {
	*test.InMemoryFederationDatabase
	peek *types.OutboundPeek
}

func (d *outboundPeekDB) GetOutboundPeek(_ context.Context, serverName spec.ServerName, roomID, peekID string) (*types.OutboundPeek, error) {
	if d.peek == nil || d.peek.ServerName != serverName || d.peek.RoomID != roomID || d.peek.PeekID != peekID {
		return nil, nil
	}
	return d.peek, nil
}

func (d *outboundPeekDB) GetOutboundPeeks(_ context.Context, roomID string) ([]types.OutboundPeek, error) {
	if d.peek == nil || d.peek.RoomID != roomID {
		return nil, nil
	}
	return []types.OutboundPeek{*d.peek}, nil
}

// peekFedClient fails every peek like an unreachable server, recording who
// was asked.
type peekFedClient struct {
	fclient.FederationClient
	peeked []spec.ServerName
}

func (c *peekFedClient) Peek(_ context.Context, _, s spec.ServerName, _, _ string, _ []gomatrixserverlib.RoomVersion) (fclient.RespPeek, error) {
	c.peeked = append(c.peeked, s)
	return fclient.RespPeek{}, errors.New("remote unreachable")
}

func TestPerformOutboundPeekRenewal(t *testing.T) {
	remote := spec.ServerName("remote.test")
	roomID := "!room:remote.test"
	testDB := &outboundPeekDB{InMemoryFederationDatabase: test.NewInMemoryFederationDatabase()}
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cfg := config.FederationAPI{Matrix: &config.Global{SigningIdentity: fclient.SigningIdentity{ServerName: "local.test", KeyID: "ed25519:1", PrivateKey: key}}}
	fedClient := &peekFedClient{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, true)
	fedAPI := NewFederationInternalAPI(testDB, &cfg, nil, fedClient, &stats, nil, nil, nil)
	peek := func() (*api.PerformOutboundPeekResponse, error) {
		fedClient.peeked = nil
		res := &api.PerformOutboundPeekResponse{}
		err := fedAPI.PerformOutboundPeek(context.Background(), &api.PerformOutboundPeekRequest{
			RoomID: roomID, ServerNames: []spec.ServerName{remote},
		}, res)
		return res, err
	}
	now := time.Now().UnixMilli()

	t.Run("a live peek is not renewed", func(t *testing.T) {
		testDB.peek = &types.OutboundPeek{PeekID: roomID, RoomID: roomID, ServerName: remote, RenewedTimestamp: now, RenewalInterval: time.Hour.Milliseconds()}
		res, err := peek()
		require.NoError(t, err)
		assert.Nil(t, res.LastError)
		assert.Empty(t, fedClient.peeked)
	})

	t.Run("an expired peek is renewed through its server", func(t *testing.T) {
		testDB.peek = &types.OutboundPeek{PeekID: roomID, RoomID: roomID, ServerName: remote, RenewedTimestamp: now - 2*time.Hour.Milliseconds(), RenewalInterval: time.Hour.Milliseconds()}
		res, err := peek()
		require.Error(t, err)
		assert.Equal(t, []spec.ServerName{remote}, fedClient.peeked)
		require.NotNil(t, res.LastError)
		assert.Contains(t, res.LastError.Message, "remote unreachable")
	})

	t.Run("no servers to peek via is an error", func(t *testing.T) {
		testDB.peek = nil
		res := &api.PerformOutboundPeekResponse{}
		err := fedAPI.PerformOutboundPeek(context.Background(), &api.PerformOutboundPeekRequest{RoomID: roomID}, res)
		require.Error(t, err)
		require.NotNil(t, res.LastError)
	})
}
