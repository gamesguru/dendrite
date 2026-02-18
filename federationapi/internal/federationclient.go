package internal

import (
	"context"
	"fmt"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/federationapi/statistics"
)

const defaultTimeout = time.Second * 30

// Functions here are "proxying" calls to the gomatrixserverlib federation
// client.

func (a *FederationInternalAPI) MakeJoin(
	ctx context.Context, origin, s spec.ServerName, roomID, userID string,
) (res gomatrixserverlib.MakeJoinResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	stats := a.statistics.ForServer(s) //nolint:contextcheck
	ires, err := a.federation.MakeJoin(ctx, origin, s, roomID, userID)
	if err != nil {
		// Record failure for backoff tracking (joins are user-initiated so we don't pre-filter)
		failBlacklistableError(err, stats) //nolint:contextcheck
		return &fclient.RespMakeJoin{}, err
	}
	stats.Success(statistics.SendDirect) //nolint:contextcheck
	return &ires, nil
}

func (a *FederationInternalAPI) SendJoin(
	ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU,
) (res gomatrixserverlib.SendJoinResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()
	stats := a.statistics.ForServer(s) //nolint:contextcheck
	ires, err := a.federation.SendJoin(ctx, origin, s, event)
	if err != nil {
		// Record failure for backoff tracking (joins are user-initiated so we don't pre-filter)
		failBlacklistableError(err, stats) //nolint:contextcheck
		return &fclient.RespSendJoin{}, err
	}
	stats.Success(statistics.SendDirect) //nolint:contextcheck
	return &ires, nil
}

// SendJoinPartialState sends a join event using MSC3706 partial state join (omit_members=true).
func (a *FederationInternalAPI) SendJoinPartialState(
	ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU,
) (res gomatrixserverlib.SendJoinResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5) //nolint:mnd
	defer cancel()
	stats := a.statistics.ForServer(s) //nolint:contextcheck
	ires, err := a.federation.SendJoinPartialState(ctx, origin, s, event)
	if err != nil {
		// Record failure for backoff tracking (joins are user-initiated so we don't pre-filter)
		failBlacklistableError(err, stats) //nolint:contextcheck
		return &fclient.RespSendJoin{}, err
	}
	stats.Success(statistics.SendDirect) //nolint:contextcheck
	return &ires, nil
}

// PartialStateJoinClient wraps the FederationInternalAPI to use SendJoinPartialState
// instead of SendJoin when calling gomatrixserverlib.PerformJoin.
// It also tracks whether the join response had members omitted for partial state joins.
type PartialStateJoinClient struct {
	*FederationInternalAPI
	// LastJoinMembersOmitted tracks if the last join response omitted members
	LastJoinMembersOmitted bool
	// LastJoinServersInRoom tracks the servers returned in the last partial state join
	LastJoinServersInRoom []string
}

// SendJoin calls SendJoinPartialState for MSC3706 faster joins.
func (p *PartialStateJoinClient) SendJoin(
	ctx context.Context, origin, s spec.ServerName, event gomatrixserverlib.PDU,
) (res gomatrixserverlib.SendJoinResponse, err error) {
	res, err = p.SendJoinPartialState(ctx, origin, s, event)
	if err == nil && res != nil {
		p.LastJoinMembersOmitted = res.GetMembersOmitted()
		p.LastJoinServersInRoom = res.GetServersInRoom()
	}
	return res, err
}

func (a *FederationInternalAPI) GetEventAuth(
	ctx context.Context, origin, s spec.ServerName,
	roomVersion gomatrixserverlib.RoomVersion, roomID, eventID string,
) (res fclient.RespEventAuth, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.GetEventAuth(ctx, origin, s, roomVersion, roomID, eventID)
	})
	if err != nil {
		return fclient.RespEventAuth{}, err
	}
	resp, ok := ires.(fclient.RespEventAuth)
	if !ok {
		return fclient.RespEventAuth{}, fmt.Errorf("unexpected response type from GetEventAuth")
	}
	return resp, nil
}

func (a *FederationInternalAPI) GetUserDevices(
	ctx context.Context, origin, s spec.ServerName, userID string,
) (fclient.RespUserDevices, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.GetUserDevices(ctx, origin, s, userID)
	})
	if err != nil {
		return fclient.RespUserDevices{}, err
	}
	resp, ok := ires.(fclient.RespUserDevices)
	if !ok {
		return fclient.RespUserDevices{}, fmt.Errorf("unexpected response type for GetUserDevices")
	}
	return resp, nil
}

func (a *FederationInternalAPI) ClaimKeys(
	ctx context.Context, origin, s spec.ServerName, oneTimeKeys map[string]map[string]string,
) (fclient.RespClaimKeys, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.ClaimKeys(ctx, origin, s, oneTimeKeys)
	})
	if err != nil {
		return fclient.RespClaimKeys{}, err
	}
	resp, ok := ires.(fclient.RespClaimKeys)
	if !ok {
		return fclient.RespClaimKeys{}, fmt.Errorf("unexpected response type for ClaimKeys")
	}
	return resp, nil
}

func (a *FederationInternalAPI) QueryKeys(
	ctx context.Context, origin, s spec.ServerName, keys map[string][]string,
) (fclient.RespQueryKeys, error) {
	ires, err := a.doRequestIfNotBackingOffOrBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.QueryKeys(ctx, origin, s, keys)
	})
	if err != nil {
		return fclient.RespQueryKeys{}, err
	}
	resp, ok := ires.(fclient.RespQueryKeys)
	if !ok {
		return fclient.RespQueryKeys{}, fmt.Errorf("unexpected response type for QueryKeys")
	}
	return resp, nil
}

func (a *FederationInternalAPI) Backfill(
	ctx context.Context, origin, s spec.ServerName, roomID string, limit int, eventIDs []string,
) (res gomatrixserverlib.Transaction, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.Backfill(ctx, origin, s, roomID, limit, eventIDs)
	})
	if err != nil {
		return gomatrixserverlib.Transaction{}, err
	}
	resp, ok := ires.(gomatrixserverlib.Transaction)
	if !ok {
		return gomatrixserverlib.Transaction{}, fmt.Errorf("unexpected response type for Backfill")
	}
	return resp, nil
}

func (a *FederationInternalAPI) LookupState(
	ctx context.Context, origin, s spec.ServerName, roomID, eventID string, roomVersion gomatrixserverlib.RoomVersion,
) (res gomatrixserverlib.StateResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.LookupState(ctx, origin, s, roomID, eventID, roomVersion)
	})
	if err != nil {
		return &fclient.RespState{}, err
	}
	r, ok := ires.(fclient.RespState)
	if !ok {
		return &fclient.RespState{}, fmt.Errorf("unexpected response type for LookupState")
	}
	return &r, nil
}

func (a *FederationInternalAPI) LookupStateIDs(
	ctx context.Context, origin, s spec.ServerName, roomID, eventID string,
) (res gomatrixserverlib.StateIDResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.LookupStateIDs(ctx, origin, s, roomID, eventID)
	})
	if err != nil {
		return fclient.RespStateIDs{}, err
	}
	resp, ok := ires.(fclient.RespStateIDs)
	if !ok {
		return fclient.RespStateIDs{}, fmt.Errorf("unexpected response type for LookupStateIDs")
	}
	return resp, nil
}

func (a *FederationInternalAPI) LookupMissingEvents(
	ctx context.Context, origin, s spec.ServerName, roomID string,
	missing fclient.MissingEvents, roomVersion gomatrixserverlib.RoomVersion,
) (res fclient.RespMissingEvents, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.LookupMissingEvents(ctx, origin, s, roomID, missing, roomVersion)
	})
	if err != nil {
		return fclient.RespMissingEvents{}, err
	}
	resp, ok := ires.(fclient.RespMissingEvents)
	if !ok {
		return fclient.RespMissingEvents{}, fmt.Errorf("unexpected response type for LookupMissingEvents")
	}
	return resp, nil
}

func (a *FederationInternalAPI) GetEvent(
	ctx context.Context, origin, s spec.ServerName, eventID string,
) (res gomatrixserverlib.Transaction, err error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.GetEvent(ctx, origin, s, eventID)
	})
	if err != nil {
		return gomatrixserverlib.Transaction{}, err
	}
	resp, ok := ires.(gomatrixserverlib.Transaction)
	if !ok {
		return gomatrixserverlib.Transaction{}, fmt.Errorf("unexpected response type for GetEvent")
	}
	return resp, nil
}

func (a *FederationInternalAPI) LookupServerKeys(
	ctx context.Context, s spec.ServerName, keyRequests map[gomatrixserverlib.PublicKeyLookupRequest]spec.Timestamp,
) ([]gomatrixserverlib.ServerKeys, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.LookupServerKeys(ctx, s, keyRequests)
	})
	if err != nil {
		return []gomatrixserverlib.ServerKeys{}, err
	}
	resp, ok := ires.([]gomatrixserverlib.ServerKeys)
	if !ok {
		return []gomatrixserverlib.ServerKeys{}, fmt.Errorf("unexpected response type for LookupServerKeys")
	}
	return resp, nil
}

func (a *FederationInternalAPI) MSC2836EventRelationships(
	ctx context.Context, origin, s spec.ServerName, r fclient.MSC2836EventRelationshipsRequest,
	roomVersion gomatrixserverlib.RoomVersion,
) (res fclient.MSC2836EventRelationshipsResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.MSC2836EventRelationships(ctx, origin, s, r, roomVersion)
	})
	if err != nil {
		return res, err
	}
	resp, ok := ires.(fclient.MSC2836EventRelationshipsResponse)
	if !ok {
		return res, fmt.Errorf("unexpected response type for MSC2836EventRelationships")
	}
	return resp, nil
}

func (a *FederationInternalAPI) RoomHierarchies(
	ctx context.Context, origin, s spec.ServerName, roomID string, suggestedOnly bool,
) (res fclient.RoomHierarchyResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ires, err := a.doRequestIfNotBlacklisted(s, func() (any, error) { //nolint:contextcheck
		return a.federation.RoomHierarchy(ctx, origin, s, roomID, suggestedOnly)
	})
	if err != nil {
		return res, err
	}
	resp, ok := ires.(fclient.RoomHierarchyResponse)
	if !ok {
		return res, fmt.Errorf("unexpected response type for RoomHierarchies")
	}
	return resp, nil
}
