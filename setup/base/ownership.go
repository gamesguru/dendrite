package base

import (
	"context"
	"fmt"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/ownership"
)

// ErrDispatchRefused is re-exported so federation callers can tell an ownership
// refusal apart from a remote failure.
var ErrDispatchRefused = ownership.ErrDispatchRefused

// GuardFederationClient binds outbound federation dispatch to one authorized
// activation. Remote servers cannot reject a retired owner, so the dispatch
// acceptance point is here: a request that has not yet started its HTTP
// round-trip is refused.
//
// Every method that makes a remote server accept an effect on this
// homeserver's behalf is covered, not only the transaction senders. A signed
// membership event accepted after takeover is as damaging as a stale
// transaction, and ClaimKeys burns remote one-time keys irrecoverably.
//
// Requests already dispatched may still complete, which is the documented
// at-least-once behavior. Recovering them is the successor's job, using the
// durable queue and stable transaction identifiers.
func GuardFederationClient(client fclient.FederationClient, owner ownership.Owner) fclient.FederationClient {
	return &guardedFederationClient{FederationClient: client, owner: owner}
}

type guardedFederationClient struct {
	fclient.FederationClient
	owner ownership.Owner
}

func (g *guardedFederationClient) owned() error {
	if g.owner == nil {
		return fmt.Errorf("%w: ownership has not been established", ErrDispatchRefused)
	}
	if err := g.owner.Valid(); err != nil {
		return fmt.Errorf("%w: %w", ErrDispatchRefused, err)
	}
	return nil
}

func (g *guardedFederationClient) SendTransaction(ctx context.Context, t gomatrixserverlib.Transaction) (fclient.RespSend, error) {
	if err := g.owned(); err != nil {
		return fclient.RespSend{}, err
	}
	return g.FederationClient.SendTransaction(ctx, t)
}

func (g *guardedFederationClient) P2PSendTransactionToRelay(ctx context.Context, u spec.UserID, t gomatrixserverlib.Transaction, forwardingServer spec.ServerName) (fclient.EmptyResp, error) {
	if err := g.owned(); err != nil {
		return fclient.EmptyResp{}, err
	}
	return g.FederationClient.P2PSendTransactionToRelay(ctx, u, t, forwardingServer)
}

func (g *guardedFederationClient) SendJoin(ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU) (fclient.RespSendJoin, error) {
	if err := g.owned(); err != nil {
		return fclient.RespSendJoin{}, err
	}
	return g.FederationClient.SendJoin(ctx, origin, s, event)
}

func (g *guardedFederationClient) SendJoinPartialState(ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU) (fclient.RespSendJoin, error) {
	if err := g.owned(); err != nil {
		return fclient.RespSendJoin{}, err
	}
	return g.FederationClient.SendJoinPartialState(ctx, origin, s, event)
}

func (g *guardedFederationClient) SendLeave(ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU) error {
	if err := g.owned(); err != nil {
		return err
	}
	return g.FederationClient.SendLeave(ctx, origin, s, event)
}

func (g *guardedFederationClient) SendInviteV2(ctx context.Context, origin, s spec.ServerName, request fclient.InviteV2Request) (fclient.RespInviteV2, error) {
	if err := g.owned(); err != nil {
		return fclient.RespInviteV2{}, err
	}
	return g.FederationClient.SendInviteV2(ctx, origin, s, request)
}

func (g *guardedFederationClient) SendInviteV3(ctx context.Context, origin, s spec.ServerName, request fclient.InviteV3Request, userID spec.UserID) (fclient.RespInviteV2, error) {
	if err := g.owned(); err != nil {
		return fclient.RespInviteV2{}, err
	}
	return g.FederationClient.SendInviteV3(ctx, origin, s, request, userID)
}

func (g *guardedFederationClient) SendKnock(ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU) (fclient.RespSendKnock, error) {
	if err := g.owned(); err != nil {
		return fclient.RespSendKnock{}, err
	}
	return g.FederationClient.SendKnock(ctx, origin, s, event)
}

func (g *guardedFederationClient) ExchangeThirdPartyInvite(ctx context.Context, origin, s spec.ServerName, builder gomatrixserverlib.ProtoEvent) error {
	if err := g.owned(); err != nil {
		return err
	}
	return g.FederationClient.ExchangeThirdPartyInvite(ctx, origin, s, builder)
}

// ClaimKeys consumes remote one-time keys, which cannot be recovered once
// spent, so a retired owner must never reach it.
func (g *guardedFederationClient) ClaimKeys(ctx context.Context, origin, s spec.ServerName, oneTimeKeys map[string]map[string]string) (fclient.RespClaimKeys, error) {
	if err := g.owned(); err != nil {
		return fclient.RespClaimKeys{}, err
	}
	return g.FederationClient.ClaimKeys(ctx, origin, s, oneTimeKeys)
}
