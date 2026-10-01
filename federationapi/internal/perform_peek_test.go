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
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
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

// remotePeekKeys serves the remote server's public key, so its events pass
// signature checks.
type remotePeekKeys struct{ public ed25519.PublicKey }

func (k remotePeekKeys) FetchKeys(_ context.Context, requests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp) (map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult, error) {
	res := map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult{}
	for req := range requests {
		res[req] = gomatrixserverlib.PublicKeyLookupResult{
			VerifyKey:    gomatrixserverlib.VerifyKey{Key: spec.Base64Bytes(k.public)},
			ExpiredTS:    gomatrixserverlib.PublicKeyNotExpired,
			ValidUntilTS: spec.AsTimestamp(time.Now().Add(time.Hour)),
		}
	}
	return res, nil
}

func (remotePeekKeys) FetcherName() string { return "remotePeekKeys" }

func (remotePeekKeys) StoreKeys(context.Context, map[gomatrixserverlib.PublicKeyLookupRequest]gomatrixserverlib.PublicKeyLookupResult) error {
	return nil
}

// recordingPeekDB records the outbound peek it is asked to store.
type recordingPeekDB struct{ outboundPeekDB }

func (d *recordingPeekDB) AddOutboundPeek(_ context.Context, serverName spec.ServerName, roomID, peekID string, renewalInterval int64) error {
	d.peek = &types.OutboundPeek{PeekID: peekID, RoomID: roomID, ServerName: serverName, RenewedTimestamp: time.Now().UnixMilli(), RenewalInterval: renewalInterval}
	return nil
}

// statePeekFedClient answers peeks with a room's current state.
type statePeekFedClient struct {
	fclient.FederationClient
	resp fclient.RespPeek
}

func (c *statePeekFedClient) Peek(context.Context, spec.ServerName, spec.ServerName, string, string, []gomatrixserverlib.RoomVersion) (fclient.RespPeek, error) {
	return c.resp, nil
}

// lostReplyRoomserver never answers the first state input, like a reply lost
// on its way back, and stores later ones.
type lostReplyRoomserver struct {
	roomserverAPI.FederationRoomserverAPI
	inputs int
}

func (r *lostReplyRoomserver) InputRoomEvents(ctx context.Context, _ *roomserverAPI.InputRoomEventsRequest, res *roomserverAPI.InputRoomEventsResponse) {
	r.inputs++
	if r.inputs == 1 {
		// Give up eventually, so that a peek without a deadline fails the
		// test instead of hanging it.
		select {
		case <-ctx.Done():
			res.ErrMsg = ctx.Err().Error()
		case <-time.After(2 * time.Second):
			res.ErrMsg = "no reply"
		}
	}
}

func (r *lostReplyRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

// TestOutboundPeekRecordedOnlyWithItsState checks that a peek whose state was
// not stored fails, so that the next peek tries again. A peek recorded before
// its state made every later one skip the remote, leaving the room unreadable
// until the peek expired.
func TestOutboundPeekRecordedOnlyWithItsState(t *testing.T) {
	restore := outboundPeekInputTimeout
	outboundPeekInputTimeout = 100 * time.Millisecond
	t.Cleanup(func() { outboundPeekInputTimeout = restore })

	remote := spec.ServerName("remote.test")
	bob := test.NewUser(t, test.WithSigningServer(remote, "ed25519:remote", test.PrivateKeyA))
	room := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, bob, spec.MRoomHistoryVisibility, map[string]any{"history_visibility": "world_readable"}, test.WithStateKey(""))
	var state gomatrixserverlib.EventJSONs
	for _, event := range room.CurrentState() {
		state = append(state, event.JSON())
	}
	events := room.Events()

	testDB := &recordingPeekDB{outboundPeekDB{InMemoryFederationDatabase: test.NewInMemoryFederationDatabase()}}
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	cfg := config.FederationAPI{Matrix: &config.Global{SigningIdentity: fclient.SigningIdentity{ServerName: "local.test", KeyID: "ed25519:1", PrivateKey: key}}}
	fedClient := &statePeekFedClient{resp: fclient.RespPeek{
		RenewalInterval: time.Hour.Milliseconds(), StateEvents: state, AuthEvents: state,
		RoomVersion: room.Version, LatestEvent: events[len(events)-1].PDU,
	}}
	public, ok := test.PrivateKeyA.Public().(ed25519.PublicKey)
	require.True(t, ok)
	keyRing := &gomatrixserverlib.KeyRing{KeyDatabase: remotePeekKeys{public: public}}
	rs := &lostReplyRoomserver{}
	stats := statistics.NewStatistics(testDB, FailuresUntilBlacklist, FailuresUntilAssumedOffline, true)
	fedAPI := NewFederationInternalAPI(testDB, &cfg, rs, fedClient, &stats, nil, nil, keyRing)
	peek := func() *api.PerformOutboundPeekResponse {
		res := &api.PerformOutboundPeekResponse{}
		_ = fedAPI.PerformOutboundPeek(context.Background(), &api.PerformOutboundPeekRequest{RoomID: room.ID, ServerNames: []spec.ServerName{remote}}, res)
		return res
	}

	res := peek()
	require.NotNil(t, res.LastError, "a peek whose state was not stored must fail")
	assert.Nil(t, testDB.peek, "a peek whose state was not stored must not be recorded")

	res = peek()
	require.Nil(t, res.LastError)
	assert.Equal(t, 2, rs.inputs, "the next peek must fetch the state again")
	require.NotNil(t, testDB.peek)
	assert.Equal(t, room.ID, testDB.peek.RoomID)
}
