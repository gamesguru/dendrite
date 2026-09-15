package base

import (
	"context"
	"errors"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
)

type testOwner struct{ err error }

func (o *testOwner) Generation() int64 { return 1 }
func (o *testOwner) Identity() string  { return "candidate-a" }
func (o *testOwner) Valid() error      { return o.err }

type recordingFederationClient struct {
	fclient.FederationClient
	sent int
}

func (c *recordingFederationClient) SendTransaction(context.Context, gomatrixserverlib.Transaction) (fclient.RespSend, error) {
	c.sent++
	return fclient.RespSend{}, nil
}

func (c *recordingFederationClient) P2PSendTransactionToRelay(context.Context, spec.UserID, gomatrixserverlib.Transaction, spec.ServerName) (fclient.EmptyResp, error) {
	c.sent++
	return fclient.EmptyResp{}, nil
}

func TestGuardedFederationClientRefusesDispatchWithoutOwnership(t *testing.T) {
	retired := errors.New("ownership generation is retired")
	inner := &recordingFederationClient{}
	guarded := GuardFederationClient(inner, &testOwner{err: retired})

	_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
	require.ErrorIs(t, err, retired)
	user, err := spec.NewUserID("@alice:example.com", true)
	require.NoError(t, err)
	_, err = guarded.P2PSendTransactionToRelay(t.Context(), *user, gomatrixserverlib.Transaction{}, "relay.example.com")
	require.ErrorIs(t, err, retired)
	require.Zero(t, inner.sent, "a retired activation must not start a new outbound transaction")
}

func TestGuardedFederationClientDispatchesWhileOwned(t *testing.T) {
	inner := &recordingFederationClient{}
	guarded := GuardFederationClient(inner, &testOwner{})

	_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
	require.NoError(t, err)
	require.Equal(t, 1, inner.sent)
}

// federationWrites are the client methods that make a remote server accept an
// effect on this homeserver's behalf. Each must be refused once ownership is
// lost, not just the transaction senders.
func TestGuardedFederationClientRefusesEveryWrite(t *testing.T) {
	retired := errors.New("ownership generation is retired")
	inner := &recordingFederationClient{}
	guarded := GuardFederationClient(inner, &testOwner{err: retired})
	user, err := spec.NewUserID("@alice:example.com", true)
	require.NoError(t, err)

	for name, call := range map[string]func() error{
		"SendTransaction": func() error {
			_, callErr := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
			return callErr
		},
		"P2PSendTransactionToRelay": func() error {
			_, callErr := guarded.P2PSendTransactionToRelay(t.Context(), *user, gomatrixserverlib.Transaction{}, "relay.example.com")
			return callErr
		},
		"SendJoin": func() error {
			_, callErr := guarded.SendJoin(t.Context(), "example.com", "remote.example.com", nil)
			return callErr
		},
		"SendJoinPartialState": func() error {
			_, callErr := guarded.SendJoinPartialState(t.Context(), "example.com", "remote.example.com", nil)
			return callErr
		},
		"SendLeave": func() error {
			return guarded.SendLeave(t.Context(), "example.com", "remote.example.com", nil)
		},
		"SendInviteV2": func() error {
			_, callErr := guarded.SendInviteV2(t.Context(), "example.com", "remote.example.com", fclient.InviteV2Request{})
			return callErr
		},
		"SendInviteV3": func() error {
			_, callErr := guarded.SendInviteV3(t.Context(), "example.com", "remote.example.com", fclient.InviteV3Request{}, *user)
			return callErr
		},
		"SendKnock": func() error {
			_, callErr := guarded.SendKnock(t.Context(), "example.com", "remote.example.com", nil)
			return callErr
		},
		"ExchangeThirdPartyInvite": func() error {
			return guarded.ExchangeThirdPartyInvite(t.Context(), "example.com", "remote.example.com", gomatrixserverlib.ProtoEvent{})
		},
		// Burning a remote one-time key is irrecoverable, so it matters most.
		"ClaimKeys": func() error {
			_, callErr := guarded.ClaimKeys(t.Context(), "example.com", "remote.example.com", nil)
			return callErr
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, call(), retired)
		})
	}
	require.Zero(t, inner.sent, "no federation write may reach a remote server")
}

// TestOwnershipRefusalIsDistinguishable keeps the destination queue from
// charging an ownership refusal to the remote server. Those failures persist
// retry state, divert to relays and eventually blacklist servers that were
// never unreachable, in a database the successor inherits.
func TestOwnershipRefusalIsDistinguishable(t *testing.T) {
	guarded := GuardFederationClient(&recordingFederationClient{}, &testOwner{err: errors.New("ownership generation is retired")})
	_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
	require.ErrorIs(t, err, ErrDispatchRefused)
}

// TestMonitoringSurvivesClosedAdmission keeps probes and metrics answerable
// while application routes are closed. Blinding monitoring at the moment
// ownership becomes uncertain makes a still-legitimate owner look dead to
// every probe, exactly when an operator needs the signal.
func TestMonitoringSurvivesClosedAdmission(t *testing.T) {
	for _, path := range []string{
		"/_zendrite/monitor/up",
		"/_zendrite/monitor/health",
		"/metrics",
	} {
		require.True(t, alwaysAdmitted(path), "%s must stay reachable while admission is closed", path)
	}
	for _, path := range []string{
		"/_matrix/client/v3/sync",
		"/_matrix/federation/v1/send/1",
		"/_synapse/admin/v1/whatever",
	} {
		require.False(t, alwaysAdmitted(path), "%s is an application route and must be refused", path)
	}
}
